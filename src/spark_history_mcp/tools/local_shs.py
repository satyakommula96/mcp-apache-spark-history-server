"""Start/stop local Spark History Servers on demand for an event-log path.

Give a ``gs://.../spark-logs`` (or hdfs://, s3a://, file:/) directory and
``start_history_server`` launches a local History Server reading it, waits
until its REST API answers, and registers it under a name -- every other
tool can then use it via ``server=<name>``. Servers keep running after the
MCP restarts and are re-registered on startup from a small registry file.

Environment (all optional):
  SPARK_MCP_SPARK_HOME      Spark distribution to run (default: $SPARK_HOME,
                            else the newest ~/spark-*-bin-* directory)
  SPARK_MCP_SHS_HOME        State dir (default ~/.cache/spark-mcp/shs)
  SPARK_MCP_SHS_PLUGIN_JARS Extra daemon classpath, ":"-separated (default:
                            every *.jar in $SPARK_MCP_SHS_HOME/jars -- drop
                            the DPAS/Gluten jar there for /gluten + /dpas tabs)
  GOOGLE_CLOUD_PROJECT      GCP project for the GCS connector (optional)
"""

import glob
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

import requests
from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig
from spark_history_mcp.core.app import tool

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")
# Written verbatim into spark-defaults.conf: forbid whitespace/newlines so a
# path can't inject extra config lines.
_LOG_DIR_RE = re.compile(r"^(?:gs|hdfs|s3a|abfss|file)://\S+$|^file:/\S+$")
_FIRST_PORT = 18082
_LOCK = threading.Lock()


def _home() -> str:
    return os.path.expanduser(
        os.environ.get("SPARK_MCP_SHS_HOME", "~/.cache/spark-mcp/shs")
    )


def _registry_path() -> str:
    return os.path.join(_home(), "registry.json")


def _load_registry() -> Dict[str, Dict[str, Any]]:
    try:
        with open(_registry_path()) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_registry(reg: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(_home(), exist_ok=True)
    tmp = _registry_path() + ".tmp"
    with open(tmp, "w") as f:
        json.dump(reg, f, indent=1)
    os.replace(tmp, _registry_path())


def _spark_home() -> str:
    for cand in (os.environ.get("SPARK_MCP_SPARK_HOME"), os.environ.get("SPARK_HOME")):
        if cand and os.path.isfile(
            os.path.join(cand, "sbin", "start-history-server.sh")
        ):
            return cand
    found = sorted(glob.glob(os.path.expanduser("~/spark-*-bin-*")), reverse=True)
    for cand in found:
        if os.path.isfile(os.path.join(cand, "sbin", "start-history-server.sh")):
            return cand
    raise RuntimeError(
        "No Spark distribution found: set SPARK_MCP_SPARK_HOME or SPARK_HOME"
    )


def _plugin_classpath(plugin_jars: Optional[List[str]]) -> str:
    if plugin_jars:
        jars = [os.path.expanduser(j) for j in plugin_jars]
    elif os.environ.get("SPARK_MCP_SHS_PLUGIN_JARS"):
        jars = os.environ["SPARK_MCP_SHS_PLUGIN_JARS"].split(":")
    else:
        jars = sorted(glob.glob(os.path.join(_home(), "jars", "*.jar")))
    missing = [j for j in jars if not os.path.isfile(j)]
    if missing:
        raise ValueError(f"plugin jar(s) not found: {missing}")
    return ":".join(jars)


def _alive(url: str, timeout: float = 3.0) -> bool:
    try:
        r = requests.get(f"{url}/api/v1/version", timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _free_port(taken: set) -> int:
    port = _FIRST_PORT
    while port < _FIRST_PORT + 200:
        if port not in taken:
            with socket.socket() as s:
                try:
                    s.bind(("127.0.0.1", port))
                    return port
                except OSError:
                    pass
        port += 1
    raise RuntimeError("no free port in 18082-18281")


def _default_name(log_dir: str) -> str:
    # "gs://bucket/.../05-11-2026/ab03e426.../dataproc_cluster/spark-logs"
    # -> a short, stable, readable name: shs-<last dated/id segment>-<hash>
    parts = [p for p in re.split(r"[/:]+", log_dir) if p]
    tag = next(
        (
            p
            for p in reversed(parts)
            if p not in ("spark-logs", "dataproc_cluster", "eventlog", "event-logs")
        ),
        "logs",
    )
    tag = re.sub(r"[^a-z0-9]+", "-", tag.lower()).strip("-")[:16] or "logs"
    return f"shs-{tag}-{hashlib.sha1(log_dir.encode(), usedforsecurity=False).hexdigest()[:6]}"


def _env(entry: Dict[str, Any], classpath: str = "") -> Dict[str, str]:
    d = entry["dir"]
    env = dict(os.environ)
    env.update(
        SPARK_CONF_DIR=os.path.join(d, "conf"),
        SPARK_LOG_DIR=os.path.join(d, "logs"),
        SPARK_PID_DIR=os.path.join(d, "pid"),
        SPARK_DAEMON_MEMORY=entry.get("memory", "2g"),
    )
    if classpath:
        env["SPARK_DAEMON_CLASSPATH"] = classpath
    return env


def _register(ctx: Context, name: str, url: str) -> None:
    clients = ctx.request_context.lifespan_context.clients
    clients[name] = SparkRestClient(ServerConfig(url=url, timeout=300))


def restore_registered(clients: Dict[str, SparkRestClient]) -> List[str]:
    """Called at MCP startup: re-register local servers still running."""
    restored = []
    for name, entry in _load_registry().items():
        if entry.get("owner"):
            continue  # managed by a configured event-log server
        if name not in clients and _alive(entry["url"], timeout=1.0):
            clients[name] = SparkRestClient(ServerConfig(url=entry["url"], timeout=300))
            restored.append(name)
    return restored


def normalize_log_dir(log_dir: str) -> str:
    """Strip whitespace/trailing slash and validate an event-log dir URI."""
    log_dir = log_dir.strip().rstrip("/")
    if not _LOG_DIR_RE.match(log_dir):
        raise ValueError("log_dir must be a URI like gs://bucket/path (no spaces)")
    return log_dir


def ensure_local_history_server(
    log_dir: str,
    name: Optional[str] = None,
    port: Optional[int] = None,
    plugin_jars: Optional[List[str]] = None,
    gcp_project: Optional[str] = None,
    memory: str = "2g",
    wait_seconds: int = 120,
    taken_names: Optional[set] = None,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    """Start (or reuse) a local History Server for ``log_dir``.

    Usable without an MCP request context (the event-log backend calls it
    lazily). Does not register the server with the MCP; the caller does.
    ``owner`` tags the registry entry so MCP startup does not re-register
    servers that a configured event-log server manages itself.

    Returns:
        {name, url, log_dir, status: "running"|"reused"|"starting"[, hint, log_tail]}
    """
    log_dir = normalize_log_dir(log_dir)
    taken_names = taken_names or set()
    with _LOCK:
        reg = _load_registry()
        for n, e in reg.items():
            if (e["log_dir"] == log_dir or n == name) and _alive(e["url"]):
                if name and n != name:
                    continue
                return {
                    "name": n,
                    "url": e["url"],
                    "log_dir": e["log_dir"],
                    "status": "reused",
                }

        name = name or _default_name(log_dir)
        if not _NAME_RE.match(name):
            raise ValueError("name must match [a-z0-9][a-z0-9_-]{0,39}")
        if name in taken_names and name not in reg:
            raise ValueError(
                f"server name {name!r} is already configured; pick another"
            )
        taken = {e["port"] for e in reg.values()}
        if port is None:
            port = _free_port(taken)
        elif not (1024 < port < 65536):
            raise ValueError("port must be 1025-65535")

        spark_home = _spark_home()
        classpath = _plugin_classpath(plugin_jars)
        d = os.path.join(_home(), name)
        for sub in ("conf", "logs", "pid", "store"):
            os.makedirs(os.path.join(d, sub), exist_ok=True)
        log4j = os.path.join(spark_home, "conf", "log4j2.properties")
        if not os.path.exists(log4j):
            log4j += ".template"
        if os.path.exists(log4j):
            shutil.copyfile(log4j, os.path.join(d, "conf", "log4j2.properties"))
        project = gcp_project or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if project and not re.match(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$", project):
            raise ValueError("invalid gcp_project")
        conf = [
            f"spark.history.fs.logDirectory {log_dir}",
            f"spark.history.ui.port {port}",
            "spark.history.fs.update.interval 30s",
            "spark.history.fs.cleaner.enabled false",
            f"spark.history.store.path {os.path.join(d, 'store')}",
            "spark.history.retainedApplications 50",
            "spark.hadoop.fs.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.auth.type APPLICATION_DEFAULT",
        ]
        if project:
            conf.append(f"spark.hadoop.fs.gs.project.id {project}")
        with open(os.path.join(d, "conf", "spark-defaults.conf"), "w") as f:
            f.write("\n".join(conf) + "\n")

        entry = {
            "log_dir": log_dir,
            "port": port,
            "url": f"http://localhost:{port}",
            "dir": d,
            "spark_home": spark_home,
            "memory": memory,
        }
        if owner:
            entry["owner"] = owner
        proc = subprocess.run(  # noqa: S603 -- fixed script path, arg list, no shell
            [os.path.join(spark_home, "sbin", "start-history-server.sh")],
            env=_env(entry, classpath),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"start-history-server.sh failed: {(proc.stderr or proc.stdout)[-800:]}"
            )
        reg[name] = entry
        _save_registry(reg)

    url = entry["url"]
    deadline = time.time() + max(0, wait_seconds)
    while time.time() < deadline and not _alive(url):
        time.sleep(2)
    if not _alive(url):
        logs = sorted(glob.glob(os.path.join(d, "logs", "*.out")))
        tail = ""
        if logs:
            with open(logs[-1], errors="replace") as f:
                tail = "".join(f.readlines()[-15:])
        return {
            "name": name,
            "url": url,
            "log_dir": log_dir,
            "status": "starting",
            "hint": "not answering yet; call again or check the log",
            "log_tail": tail[-1500:],
        }
    return {"name": name, "url": url, "log_dir": log_dir, "status": "running"}


@tool(
    annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=False, idempotentHint=True
    )
)
def start_history_server(
    log_dir: str,
    name: Optional[str] = None,
    port: Optional[int] = None,
    plugin_jars: Optional[List[str]] = None,
    gcp_project: Optional[str] = None,
    memory: str = "2g",
    wait_seconds: int = 120,
    wait_for_apps_seconds: int = 90,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Start a local Spark History Server reading the given event-log directory
    (e.g. a Dataproc cluster's "gs://<bucket>/.../spark-logs") and register
    it so every other tool can use it with server=<name>. Idempotent: if a
    local server for the same log_dir (or name) is already up, it is reused.

    GCS uses Application Default Credentials (gcloud auth application-default
    login). The DPAS/Gluten UI plugin (/gluten, /dpas tabs and /gluten/json)
    is loaded from plugin_jars, or from the default jars dir (see module doc).

    Args:
        log_dir: Event-log directory URI (gs://, hdfs://, s3a://, abfss://, file:/)
        name: Server name to register (default derived from the path, e.g.
            "shs-ab03e42684f061d1-1a2b3c"); lowercase letters, digits, - and _
        port: Port to listen on (default: first free port from 18082)
        plugin_jars: Jars to add to the daemon classpath (overrides the default)
        gcp_project: GCP project id for the GCS connector (default $GOOGLE_CLOUD_PROJECT)
        memory: Daemon heap size (default "2g")
        wait_seconds: How long to wait for the REST API to come up (default 120)
        wait_for_apps_seconds: After it is up, how long to wait for the first
            scan of log_dir to list applications (default 90; 0 = don't wait)

    Returns:
        {name, url, log_dir, status: "running"|"reused"|"starting", apps_listed}
    """
    clients = ctx.request_context.lifespan_context.clients
    started = ensure_local_history_server(
        log_dir,
        name=name,
        port=port,
        plugin_jars=plugin_jars,
        gcp_project=gcp_project,
        memory=memory,
        wait_seconds=wait_seconds,
        taken_names=set(clients),
    )
    name, url, log_dir = started["name"], started["url"], started["log_dir"]
    if started["status"] == "reused":
        _register(ctx, name, url)
        return started
    if started["status"] == "starting":
        return started
    _register(ctx, name, url)
    client = ctx.request_context.lifespan_context.clients[name]
    apps = None
    deadline = time.time() + max(0, wait_for_apps_seconds)
    while True:
        try:
            apps = len(client.list_applications(limit=500))
        except Exception:
            apps = None
        if apps or time.time() >= deadline:
            break
        time.sleep(3)
    out = {
        "name": name,
        "url": url,
        "log_dir": log_dir,
        "status": "running",
        "apps_listed": apps,
    }
    if not apps:
        out["note"] = (
            "no applications listed yet -- the first scan of a large log "
            "dir can take a few minutes; check list_local_history_servers "
            "or retry find_applications later"
        )
    return out


@tool(
    annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=True, idempotentHint=True
    )
)
def stop_history_server(name: str, *, ctx: Context) -> Dict[str, Any]:
    """
    Stop a local History Server started by start_history_server and
    unregister it. Only servers in the local registry can be stopped.

    Args:
        name: The registered server name

    Returns:
        {name, stopped}
    """
    with _LOCK:
        reg = _load_registry()
        entry = reg.get(name)
        if entry is None:
            raise ValueError(f"{name!r} was not started by start_history_server")
        subprocess.run(  # noqa: S603 -- fixed script path, arg list, no shell
            [os.path.join(entry["spark_home"], "sbin", "stop-history-server.sh")],
            env=_env(entry),
            capture_output=True,
            text=True,
            timeout=60,
        )
        del reg[name]
        _save_registry(reg)
        ctx.request_context.lifespan_context.clients.pop(name, None)
    deadline = time.time() + 15  # the stop script returns before the JVM exits
    while _alive(entry["url"], timeout=1.0) and time.time() < deadline:
        time.sleep(1)
    return {"name": name, "stopped": not _alive(entry["url"], timeout=1.0)}


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_local_history_servers(*, ctx: Context) -> List[Dict[str, Any]]:
    """
    List History Servers started by start_history_server, with whether each
    is up and registered for use with server=<name>.

    Returns:
        [{name, url, log_dir, alive, registered}]
    """
    clients = ctx.request_context.lifespan_context.clients
    out = []
    for name, e in _load_registry().items():
        alive = _alive(e["url"], timeout=1.0)
        if alive and name not in clients:
            _register(ctx, name, e["url"])
        out.append(
            {
                "name": name,
                "url": e["url"],
                "log_dir": e["log_dir"],
                "alive": alive,
                "registered": name in clients,
            }
        )
    return out


@tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
def open_event_logs(
    log_dir: str,
    name: Optional[str] = None,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Register an event-log directory (e.g. a benchmark run's
    "gs://<bucket>/.../spark-logs") so every other tool can use it with
    server=<name> -- WITHOUT starting a History Server. Application listing,
    environment/config comparison and the raw-event-log Gluten/DPAS tools read
    the logs directly; stage/task/executor/SQL tools start a local History
    Server over the same directory on first use. Prefer this over
    start_history_server. Idempotent per log_dir.

    Args:
        log_dir: Event-log directory (gs://, s3a://, s3://, file:/ or an
            absolute local path)
        name: Server name to register (default derived from the path)

    Returns:
        {name, log_dir, apps: number of applications, latest: [up to 5 app ids]}
    """
    from spark_history_mcp.api.event_log_client import EventLogClient

    raw = log_dir.strip().rstrip("/")
    if raw.startswith("/"):
        raw = "file://" + raw
    if raw.startswith("s3://"):
        raw = "s3a://" + raw[len("s3://") :]
    log_dir = normalize_log_dir(raw)
    clients = ctx.request_context.lifespan_context.clients
    for n, c in clients.items():
        if isinstance(c, EventLogClient) and c.log_dir == log_dir:
            if name in (None, n):
                name = n
                break
    else:
        name = name or "el-" + _default_name(log_dir)[len("shs-") :]
        if not _NAME_RE.match(name):
            raise ValueError("name must match [a-z0-9][a-z0-9_-]{0,39}")
        if name in clients:
            raise ValueError(
                f"server name {name!r} is already configured; pick another"
            )
        clients[name] = EventLogClient(ServerConfig(event_log_dir=log_dir), name=name)
    apps = clients[name].list_applications()
    return {
        "name": name,
        "log_dir": log_dir,
        "apps": len(apps),
        "latest": [a.id for a in apps[:5]],
    }
