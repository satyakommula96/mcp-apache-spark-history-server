"""EventLogClient: reading Spark event logs directly from a directory."""

import gzip
import json
import os

import pytest
import zstandard

from spark_history_mcp.api.event_log_client import EventLogClient, _decompress_prefix
from spark_history_mcp.api_client.exceptions import NotFoundException
from spark_history_mcp.config.config import ServerConfig
from spark_history_mcp.utils.event_log_codec import iter_event_log_records

START_MS = 1_794_989_947_000
END_MS = START_MS + 8_227


def _events(app_id, attempt_id=None, name="my-app", end=True, extra=()):
    evs = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
        {
            "Event": "SparkListenerResourceProfileAdded",
            "Resource Profile Id": 0,
            "Executor Resource Requests": {
                "cores": {
                    "Resource Name": "cores",
                    "Amount": 4,
                    "Discovery Script": "",
                    "Vendor": "",
                },
                "memory": {
                    "Resource Name": "memory",
                    "Amount": 2048,
                    "Discovery Script": "",
                    "Vendor": "",
                },
            },
            "Task Resource Requests": {
                "cpus": {"Resource Name": "cpus", "Amount": 1.0}
            },
        },
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "JVM Information": {
                "Java Home": "/jdk",
                "Java Version": "17.0.9",
                "Scala Version": "2.12.18",
            },
            "Spark Properties": {
                "spark.plugins": "org.apache.gluten.GlutenPlugin",
                "spark.app.name": name,
                "spark.my.password": "hunter2",
            },
            "Hadoop Properties": {"fs.gs.impl": "x"},
            "System Properties": {"java.version": "17"},
            "Metrics Properties": {},
            "Classpath Entries": {"/jars/gluten.jar": "System Classpath"},
        },
        {
            "Event": "SparkListenerApplicationStart",
            "App Name": name,
            "App ID": app_id,
            "Timestamp": START_MS,
            "User": "satya",
            **({"App Attempt ID": attempt_id} if attempt_id else {}),
        },
        {"Event": "org.apache.gluten.events.GlutenPlanFallbackEvent", "executionId": 0},
        *extra,
    ]
    if end:
        evs.append({"Event": "SparkListenerApplicationEnd", "Timestamp": END_MS})
    return evs


def _jsonl(events) -> bytes:
    return b"".join(json.dumps(e).encode() + b"\n" for e in events)


def _write(path, data: bytes, mtime_ms=END_MS + 3):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    os.utime(path, (mtime_ms / 1000, mtime_ms / 1000))


def _client(log_dir, **kw):
    cfg = ServerConfig(event_log_dir=str(log_dir), **kw)
    return EventLogClient(cfg, name="test")


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_MCP_EVENTLOG_CACHE", str(tmp_path / "cache"))


@pytest.fixture
def log_dir(tmp_path):
    d = tmp_path / "spark-logs"
    # plain, completed
    _write(str(d / "local-1"), _jsonl(_events("local-1", name="plain")))
    # zstd as several concatenated frames, like Spark's streaming writer
    z = zstandard.ZstdCompressor()
    evs = _events("app-2", name="zstd")
    frames = z.compress(_jsonl(evs[:2])) + z.compress(_jsonl(evs[2:]))
    _write(str(d / "app-2.zstd"), frames)
    # in-progress gzip
    _write(
        str(d / "app-3.gz.inprogress"),
        gzip.compress(_jsonl(_events("app-3", end=False))),
    )
    # rolling v2 with two parts, YARN attempt
    app = "application_1700_0004"
    r = d / f"eventlog_v2_{app}_1"
    evs = _events(app, attempt_id="1", name="rolling")
    _write(str(r / f"events_1_{app}_1"), _jsonl(evs[:4]))
    _write(str(r / f"events_2_{app}_1"), _jsonl(evs[4:]))
    _write(str(r / f"appstatus_{app}_1"), b"")
    # noise that is not an event log
    _write(str(d / "README.txt"), b"hello\n")
    _write(str(d / ".local-1.crc"), b"\x00\x01")
    return d


def test_list_applications_reads_headers(log_dir):
    apps = {a.id: a for a in _client(log_dir).list_applications()}
    assert set(apps) == {"local-1", "app-2", "app-3", "application_1700_0004"}
    plain = apps["local-1"].attempts[0]
    assert apps["local-1"].name == "plain"
    assert plain.completed is True
    assert plain.start_time_epoch == START_MS
    # uncompressed: exact end from SparkListenerApplicationEnd
    assert plain.end_time_epoch == END_MS
    assert plain.duration == END_MS - START_MS
    assert plain.spark_user == "satya"
    assert plain.app_spark_version == "3.5.1"
    assert plain.start_time.endswith("GMT")
    # compressed: end approximated by the last write
    zstd = apps["app-2"].attempts[0]
    assert zstd.completed and zstd.end_time_epoch == END_MS + 3
    running = apps["app-3"].attempts[0]
    assert running.completed is False and running.duration is None
    assert apps["application_1700_0004"].attempts[0].attempt_id == "1"


def test_list_applications_filters_and_limit(log_dir):
    c = _client(log_dir)
    assert [a.id for a in c.list_applications(status=["running"])] == ["app-3"]
    assert len(c.list_applications(status=["completed"])) == 3
    assert len(c.list_applications(limit=2)) == 2
    assert c.list_applications(min_date="2099-01-01") == []


def test_get_application_and_not_found(log_dir):
    c = _client(log_dir)
    app = c.get_application("application_1700_0004")
    assert app.name == "rolling" and app.attempts[0].completed is True
    with pytest.raises(NotFoundException):
        c.get_application("does-not-exist")
    with pytest.raises(NotFoundException):
        c.get_environment("application_1700_0004", app_attempt_id="9")


def test_rolling_without_completed_marker_is_in_progress(tmp_path):
    d = tmp_path / "logs"
    r = d / "eventlog_v2_app-9"
    _write(str(r / "events_1_app-9"), _jsonl(_events("app-9", end=False)))
    _write(str(r / "appstatus_app-9.inprogress"), b"")
    att = _client(d).get_application("app-9").attempts[0]
    assert att.completed is False


def test_get_environment(log_dir):
    env = _client(log_dir).get_environment("app-2")
    assert env.runtime.java_version == "17.0.9"
    props = dict(map(tuple, env.spark_properties))
    assert props["spark.plugins"] == "org.apache.gluten.GlutenPlugin"
    assert props["spark.my.password"] == "*********(redacted)"
    assert [k for k, _ in env.spark_properties] == sorted(props)
    assert env.classpath_entries == [["/jars/gluten.jar", "System Classpath"]]
    rp = env.resource_profiles[0]
    assert rp.id == 0 and rp.executor_resources["cores"].amount == 4
    assert rp.task_resources["cpus"].amount == 1.0


def test_download_event_log_matches_history_server_zip(log_dir):
    c = _client(log_dir)
    rolling = list(
        iter_event_log_records(c.download_event_log("application_1700_0004"))
    )
    assert rolling[0]["Event"] == "SparkListenerLogStart"
    assert rolling[-1]["Event"] == "SparkListenerApplicationEnd"
    assert len(rolling) == len(_events("x"))
    zstd = list(iter_event_log_records(c.download_event_log("app-2")))
    assert any(r["Event"].startswith("org.apache.gluten.") for r in zstd)


def test_summary_disk_cache_survives_new_client(log_dir, monkeypatch):
    _client(log_dir).list_applications()
    c2 = _client(log_dir)
    monkeypatch.setattr(c2, "_header_events", lambda e: pytest.fail("re-read header"))
    # completed logs come from the cache; only the in-progress one is re-read
    monkeypatch.setattr(c2, "_scan_header", lambda e: pytest.fail(f"re-scan {e.name}"))
    assert c2.get_application("local-1").attempts[0].end_time_epoch == END_MS


def test_decompress_prefix_tolerates_truncation():
    data = _jsonl(_events("a")) * 50
    z = zstandard.ZstdCompressor().compress(data)
    assert data.startswith(_decompress_prefix(z[: len(z) // 2])[:100])
    g = gzip.compress(data)
    assert data.startswith(_decompress_prefix(g[: len(g) // 2]))


class _FakeShs:
    def __init__(self):
        self.calls = []
        self.ready_after = 0

    def list_stages(self, app_id, **kw):
        self.calls.append(("list_stages", app_id))
        if self.ready_after > 0:
            self.ready_after -= 1
            raise NotFoundException(status=404, reason="Not Found")
        return ["stage"]

    def get_application(self, app_id):
        if self.ready_after > 0:
            self.ready_after -= 1
            raise NotFoundException(status=404, reason="Not Found")
        return object()

    def get_gluten_ui_summary(self, app_id, app_attempt_id=None):
        return {"queries": {}}


def test_rest_only_calls_forward_to_lazy_history_server(log_dir, monkeypatch):
    shs = _FakeShs()
    started = []
    c = EventLogClient(
        ServerConfig(event_log_dir=str(log_dir)),
        history_server_factory=lambda: started.append(1) or shs,
    )
    c.list_applications()
    c.get_environment("local-1")
    assert started == [] and c.get_gluten_ui_summary("local-1") is None

    monkeypatch.setattr(
        "spark_history_mcp.api.event_log_client.time.sleep", lambda s: None
    )
    shs.ready_after = 2  # server still parsing the directory
    assert c.list_stages(app_id="local-1") == ["stage"]
    assert started == [1]
    assert c.list_stages("local-1") == ["stage"]
    assert started == [1]  # started once
    assert c.get_gluten_ui_summary("local-1") == {"queries": {}}


def test_unknown_attribute_is_not_forwarded(log_dir):
    c = _client(log_dir)
    with pytest.raises(AttributeError):
        c.not_a_client_method  # noqa: B018
    with pytest.raises(AttributeError):
        c._private  # noqa: B018


def test_auto_history_server_disabled(log_dir):
    c = _client(log_dir, auto_history_server=False)
    with pytest.raises(RuntimeError, match="auto_history_server"):
        c.list_stages(app_id="local-1")


def test_lifespan_builds_event_log_client(tmp_path, monkeypatch, log_dir):
    import asyncio

    from spark_history_mcp.core import app as app_module

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        f"servers:\n  logs:\n    default: true\n    event_log_dir: {log_dir}\n"
    )
    monkeypatch.setenv("SHS_MCP_CONFIG", str(cfg_path))
    monkeypatch.setattr(
        "spark_history_mcp.tools.local_shs.restore_registered", lambda clients: []
    )

    async def run():
        async with app_module.app_lifespan(None) as ctx:
            client = ctx.clients["logs"]
            assert isinstance(client, EventLogClient)
            assert ctx.default_client is client
            servers = ctx.app_discovery.find_application_servers("local-1")
            assert servers == ["logs"]

    asyncio.run(run())


def test_open_event_logs_tool_registers_client(log_dir):
    from types import SimpleNamespace

    from spark_history_mcp.tools.local_shs import open_event_logs

    clients = {}
    ctx = SimpleNamespace(
        request_context=SimpleNamespace(
            lifespan_context=SimpleNamespace(clients=clients)
        )
    )
    out = open_event_logs(str(log_dir), ctx=ctx)
    assert out["apps"] == 4 and out["name"].startswith("el-")
    assert isinstance(clients[out["name"]], EventLogClient)
    again = open_event_logs(str(log_dir) + "/", ctx=ctx)
    assert again["name"] == out["name"] and len(clients) == 1
    with pytest.raises(ValueError):
        open_event_logs("relative/path", ctx=ctx)
