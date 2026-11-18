"""Read Spark event logs directly from storage, no History Server needed.

A server configured with ``event_log_dir`` (``gs://``, ``s3a://``/``s3://``,
``file:/`` or a local path) is served by :class:`EventLogClient` instead of
:class:`SparkRestClient`. It implements the same interface:

* Answered straight from the log files (no JVM, no replay wait):
  ``list_applications``, ``get_application``, ``get_environment`` and
  ``download_event_log`` -- which is what every raw-event-log tool
  (Gluten/DPAS fallback, cost decisions, native metrics) and the config /
  environment comparison tools need.
* Everything else (stages, tasks, executors, jobs, SQL executions) needs the
  aggregates Spark's own status listeners compute while replaying the log, so
  those calls are forwarded to a local History Server over the same
  directory. It is started on first use (see ``tools/local_shs.py``), reused
  across MCP restarts, and only started if such a call actually happens.

Event-log layouts handled (see Spark's ``EventLogFileWriters``):

* single file ``<app-id>[_<attempt>][.<codec>][.inprogress]``
* rolling dir ``eventlog_v2_<app-id>[_<attempt>]/`` holding
  ``events_<n>_<app-id>[_<attempt>][.<codec>]`` parts and an
  ``appstatus_<app-id>[_<attempt>][.inprogress]`` marker
"""

import calendar
import io
import json
import logging
import os
import re
import threading
import time
import zipfile
import zlib
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from spark_history_mcp.api_client.exceptions import NotFoundException
from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.application_attempt import (
    ApplicationAttempt,
)
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.config.config import ServerConfig

logger = logging.getLogger(__name__)

_CODEC_SUFFIX_RE = re.compile(r"\.(lz4|lzf|snappy|zstd|zst|gz)$")
_INPROGRESS = ".inprogress"
_ROLLING_PREFIX = "eventlog_v2_"
_PART_RE = re.compile(r"^events_(\d+)_")
# Bytes of a log read to find the header events (LogStart, EnvironmentUpdate,
# ApplicationStart). Grows geometrically up to the cap when the environment
# update (classpath, Hadoop properties) is unusually large.
_HEADER_FIRST_READ = 1 << 20
_HEADER_MAX_READ = 64 << 20
_TAIL_READ = 1 << 20
_LISTING_TTL = 30.0
# Spark redacts these when writing the log; re-apply as a safety net.
_REDACT_RE = re.compile(r"(?i)secret|password|token|access[.]?key")
_REDACTED = "*********(redacted)"
# How long a forwarded call waits for a freshly started History Server to
# finish parsing the requested application.
_SHS_APP_WAIT_SECONDS = 300


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def _to_fsspec_url(log_dir: str) -> str:
    """Hadoop-style URIs to the form fsspec understands."""
    if log_dir.startswith(("s3a://", "s3n://")):
        return "s3://" + log_dir.split("://", 1)[1]
    if log_dir.startswith("file://"):
        return log_dir[len("file://") :]
    if log_dir.startswith("file:"):
        return log_dir[len("file:") :]
    return log_dir


def _to_hadoop_uri(log_dir: str) -> str:
    """The form a History Server's ``spark.history.fs.logDirectory`` takes."""
    if log_dir.startswith("s3://"):
        return "s3a://" + log_dir[len("s3://") :]
    if "://" not in log_dir and not log_dir.startswith("file:"):
        return "file://" + os.path.abspath(os.path.expanduser(log_dir))
    return log_dir


@dataclass
class _LogEntry:
    """One application attempt's event log in the directory."""

    name: str  # basename (file or rolling dir)
    path: str  # full fsspec path
    rolling: bool
    size: int
    mtime: float  # epoch seconds
    in_progress: bool
    # Rolling logs only: ordered part paths.
    parts: Optional[List[str]] = None

    @property
    def stem(self) -> str:
        """Sanitized ``<app-id>[_<attempt>]`` from the name."""
        n = self.name
        if self.rolling:
            return n[len(_ROLLING_PREFIX) :]
        if n.endswith(_INPROGRESS):
            n = n[: -len(_INPROGRESS)]
        return _CODEC_SUFFIX_RE.sub("", n)

    @property
    def cache_key(self) -> str:
        return f"{self.path}|{self.size}|{int(self.mtime)}|{self.in_progress}"


def _mtime(info: Dict[str, Any]) -> float:
    """fsspec implementations disagree on the modification-time field."""
    for key in ("mtime", "updated", "LastModified", "last_modified", "modified"):
        value = info.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        if isinstance(value, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    return float(calendar.timegm(time.strptime(value, fmt)))
                except ValueError:
                    continue
    return 0.0


# ---------------------------------------------------------------------------
# Decoding a prefix of a (possibly compressed) log
# ---------------------------------------------------------------------------


def _decompress_prefix(data: bytes) -> bytes:
    """Decompress as much of ``data`` (a prefix of a log file) as possible.

    Unlike :func:`event_log_codec._decompress`, never fails on a truncated
    stream: the header scan reads only the first few MB of a log.
    """
    from spark_history_mcp.utils.event_log_codec import (
        _XERIAL_SNAPPY_MAGIC,
        _lz4block_decompress,
    )

    if data.startswith(b"\x1f\x8b"):
        return zlib.decompressobj(wbits=31).decompress(data)
    if data.startswith(b"\x28\xb5\x2f\xfd"):
        import zstandard

        # Spark's writer may emit several concatenated frames; a
        # decompressobj stops at the end of one, so continue on unused_data.
        out = io.BytesIO()
        dctx = zstandard.ZstdDecompressor()
        rest = data
        while rest:
            dobj = dctx.decompressobj()
            try:
                out.write(dobj.decompress(rest))
            except zstandard.ZstdError:
                break
            if not dobj.eof:
                break  # truncated final frame: keep its decoded prefix
            rest = dobj.unused_data
        return out.getvalue()
    if data.startswith(b"LZ4Block"):
        import lz4.block

        return _lz4block_decompress(lz4.block, data)
    if data.startswith(b"\x04\x22\x4d\x18"):
        import lz4.frame

        try:
            return lz4.frame.LZ4FrameDecompressor().decompress(data)
        except RuntimeError:
            return b""
    if data.startswith(_XERIAL_SNAPPY_MAGIC):
        import snappy  # type: ignore[import-untyped]

        pos, chunks = len(_XERIAL_SNAPPY_MAGIC) + 8, []
        while pos + 4 <= len(data):
            n = int.from_bytes(data[pos : pos + 4], "big")
            pos += 4
            if pos + n > len(data):
                break
            chunks.append(snappy.decompress(data[pos : pos + n]))
            pos += n
        return b"".join(chunks)
    if data.startswith(b"ZV"):
        raise RuntimeError(
            "lzf-compressed event logs are not supported for direct reading"
        )
    return data


def _iter_json_lines(raw: bytes, complete: bool) -> Iterator[Dict[str, Any]]:
    """Parse newline-delimited JSON; when not ``complete``, drop the last
    (possibly cut) line."""
    lines = raw.split(b"\n")
    if not complete and lines:
        lines = lines[:-1]
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if isinstance(ev, dict):
            yield ev


# ---------------------------------------------------------------------------
# Per-attempt summary
# ---------------------------------------------------------------------------


@dataclass
class _AttemptSummary:
    app_id: Optional[str] = None
    attempt_id: Optional[str] = None
    name: Optional[str] = None
    spark_user: Optional[str] = None
    spark_version: Optional[str] = None
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    # True when end_ms came from SparkListenerApplicationEnd rather than the
    # file's modification time.
    end_exact: bool = False
    completed: bool = False
    last_updated_ms: Optional[int] = None
    # False for files in the directory that aren't Spark event logs.
    is_event_log: bool = True


def _fmt_time(ms: Optional[int]) -> Optional[str]:
    """History Server's date format, e.g. ``2026-11-18T10:00:00.123GMT``."""
    if ms is None:
        return None
    secs, millis = divmod(int(ms), 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs)) + f".{millis:03d}GMT"


def _parse_date(value: Optional[str]) -> Optional[int]:
    """Parse a History Server date filter (``yyyy-MM-dd`` or
    ``yyyy-MM-dd'T'HH:mm:ss.SSSz``) to epoch ms."""
    if not value:
        return None
    v = value.strip()
    for fmt, cut in (
        ("%Y-%m-%dT%H:%M:%S.%f", "GMT"),
        ("%Y-%m-%dT%H:%M:%S", "GMT"),
        ("%Y-%m-%d", None),
    ):
        candidate = v[: -len(cut)] if cut and v.endswith(cut) else v.rstrip("Z")
        try:
            return int(calendar.timegm(time.strptime(candidate, fmt)) * 1000)
        except ValueError:
            continue
    raise ValueError(f"unrecognized date {value!r}; use yyyy-MM-dd")


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class EventLogClient:
    """``SparkRestClient``-compatible client over an event-log directory."""

    def __init__(
        self,
        server_config: ServerConfig,
        name: str = "eventlog",
        fs: Any = None,
        history_server_factory: Optional[Callable[[], Any]] = None,
    ):
        if not server_config.event_log_dir:
            raise ValueError("EventLogClient requires event_log_dir")
        self.name = name
        self.log_dir = server_config.event_log_dir.strip().rstrip("/")
        self._root = _to_fsspec_url(self.log_dir)
        # Unique per directory: used as cache key and for alias de-duplication.
        self.config = server_config.model_copy(
            update={"url": server_config.url or f"eventlog+{self.log_dir}"}
        )
        self.timeout = self.config.timeout
        self._fs = fs
        self._fs_lock = threading.Lock()
        self._listing: Optional[Tuple[float, List[_LogEntry]]] = None
        self._listing_lock = threading.Lock()
        self._summaries: Dict[str, _AttemptSummary] = {}
        self._environments: Dict[str, Environment] = {}
        self._cache_lock = threading.Lock()
        self._shs = None
        self._shs_started_at = 0.0
        self._shs_lock = threading.Lock()
        self._history_server_factory = history_server_factory
        self._disk_cache_dir = os.path.expanduser(
            os.environ.get("SPARK_MCP_EVENTLOG_CACHE", "~/.cache/spark-mcp/eventlog")
        )

    # -- storage ----------------------------------------------------------
    @property
    def fs(self):
        with self._fs_lock:
            if self._fs is None:
                import fsspec

                self._fs, _ = fsspec.core.url_to_fs(self._root)
            return self._fs

    def _strip(self, path: str) -> str:
        return self.fs._strip_protocol(path)

    def _entries(self, refresh: bool = False) -> List[_LogEntry]:
        with self._listing_lock:
            now = time.time()
            if (
                not refresh
                and self._listing is not None
                and now - self._listing[0] < _LISTING_TTL
            ):
                return self._listing[1]
            entries = self._scan_dir()
            self._listing = (now, entries)
            return entries

    def _scan_dir(self) -> List[_LogEntry]:
        root = self._strip(self._root)
        entries: List[_LogEntry] = []
        try:
            infos = self.fs.ls(root, detail=True)
        except FileNotFoundError:
            raise ValueError(f"event-log directory not found: {self.log_dir}") from None
        for info in infos:
            path = info["name"]
            base = path.rstrip("/").rsplit("/", 1)[-1]
            is_dir = info.get("type") == "directory"
            if is_dir and base.startswith(_ROLLING_PREFIX):
                entries.append(self._rolling_entry(path, base))
            elif not is_dir and not base.startswith("."):
                if base.startswith(("appstatus_", "events_")):
                    continue
                entries.append(
                    _LogEntry(
                        name=base,
                        path=path,
                        rolling=False,
                        size=int(info.get("size") or 0),
                        mtime=_mtime(info),
                        in_progress=base.endswith(_INPROGRESS),
                    )
                )
        return entries

    def _rolling_entry(self, path: str, base: str) -> _LogEntry:
        parts, status_name, size, mtime = [], None, 0, 0.0
        try:
            infos = self.fs.ls(path, detail=True)
        except FileNotFoundError:
            infos = []
        for info in infos:
            child = info["name"].rstrip("/").rsplit("/", 1)[-1]
            if child.startswith("appstatus_"):
                status_name = child
            elif (m := _PART_RE.match(child)) is not None:
                parts.append((int(m.group(1)), info["name"]))
                size += int(info.get("size") or 0)
                mtime = max(mtime, _mtime(info))
        parts.sort()
        return _LogEntry(
            name=base,
            path=path,
            rolling=True,
            size=size,
            mtime=mtime,
            # No status marker at all: treat as still being written.
            in_progress=status_name is None or status_name.endswith(_INPROGRESS),
            parts=[p for _, p in parts],
        )

    def _read_range(self, path: str, start: int, end: int) -> bytes:
        return self.fs.cat_file(path, start=start, end=end)

    def _read_all(self, path: str) -> bytes:
        return self.fs.cat_file(path)

    # -- header / tail scanning ------------------------------------------
    def _header_events(self, entry: _LogEntry) -> List[Dict[str, Any]]:
        """Events at the start of the log, up to and including the first
        ``SparkListenerApplicationStart`` (or what a capped read yields)."""
        path = entry.parts[0] if entry.rolling and entry.parts else entry.path
        if entry.rolling and not entry.parts:
            return []
        want = _HEADER_FIRST_READ
        while True:
            data = self._read_range(path, 0, want)
            complete = len(data) < want
            raw = _decompress_prefix(data)
            events = []
            for ev in _iter_json_lines(raw, complete):
                events.append(ev)
                if ev.get("Event") == "SparkListenerApplicationStart":
                    return events
            if complete or want >= _HEADER_MAX_READ:
                return events
            want *= 4

    def _tail_end_event(self, entry: _LogEntry) -> Optional[Dict[str, Any]]:
        """``SparkListenerApplicationEnd`` when it can be read cheaply: the
        tail of an uncompressed log. ``None`` otherwise."""
        path = entry.parts[-1] if entry.rolling and entry.parts else entry.path
        if entry.rolling and not entry.parts:
            return None
        head = self._read_range(path, 0, 8)
        if head[:1] not in (b"{", b" ", b"\n"):
            return None  # compressed: the tail can't be decoded on its own
        size = self.fs.size(path)
        data = self._read_range(path, max(0, size - _TAIL_READ), size)
        for line in reversed(data.split(b"\n")):
            line = line.strip()
            if b"SparkListenerApplicationEnd" not in line:
                continue
            try:
                return json.loads(line)
            except ValueError:
                return None
        return None

    def _summary(self, entry: _LogEntry) -> _AttemptSummary:
        key = entry.cache_key
        with self._cache_lock:
            cached = self._summaries.get(key)
        if cached is None:
            cached = self._load_disk_summary(key)
        if cached is not None:
            return cached

        s = self._scan_header(entry)
        if s.completed:
            end_ev = self._tail_end_event(entry)
            if end_ev is not None and end_ev.get("Timestamp") is not None:
                s.end_ms = int(end_ev["Timestamp"])
                s.end_exact = True
        with self._cache_lock:
            self._summaries[key] = s
        if s.completed:
            self._save_disk_summary(key, s)
        return s

    def _scan_header(self, entry: _LogEntry) -> _AttemptSummary:
        s = _AttemptSummary(
            completed=not entry.in_progress,
            last_updated_ms=int(entry.mtime * 1000) if entry.mtime else None,
        )
        events = self._header_events(entry)
        s.is_event_log = any(
            isinstance(ev, dict)
            and str(ev.get("Event", "")).startswith(
                ("SparkListener", "org.apache.spark.")
            )
            for ev in events
        )
        for ev in events:
            kind = ev.get("Event")
            if kind == "SparkListenerLogStart":
                s.spark_version = ev.get("Spark Version")
            elif kind == "SparkListenerApplicationStart":
                s.app_id = ev.get("App ID")
                s.attempt_id = ev.get("App Attempt ID")
                s.name = ev.get("App Name")
                s.spark_user = ev.get("User")
                if ev.get("Timestamp") is not None:
                    s.start_ms = int(ev["Timestamp"])
        if s.app_id is None:
            # Header not found (empty / truncated / unusual log): fall back to
            # the file name, which is the sanitized app id.
            s.app_id = entry.stem
        # Until ApplicationEnd is read, approximate with the last write.
        s.end_ms = s.last_updated_ms if s.completed else None
        return s

    # -- disk cache of summaries (completed logs never change) -----------
    def _disk_path(self, key: str) -> str:
        import hashlib

        digest = hashlib.sha1(key.encode(), usedforsecurity=False).hexdigest()
        return os.path.join(self._disk_cache_dir, digest[:2], digest + ".json")

    def _load_disk_summary(self, key: str) -> Optional[_AttemptSummary]:
        try:
            with open(self._disk_path(key)) as f:
                s = _AttemptSummary(**json.load(f))
        except (OSError, ValueError, TypeError):
            return None
        with self._cache_lock:
            self._summaries[key] = s
        return s

    def _save_disk_summary(self, key: str, s: _AttemptSummary) -> None:
        path = self._disk_path(key)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + f".{os.getpid()}.tmp"
            with open(tmp, "w") as f:
                json.dump(asdict(s), f)
            os.replace(tmp, path)
        except OSError:
            logger.debug("could not write event-log summary cache %s", path)

    # -- application lookup -----------------------------------------------
    @staticmethod
    def _sanitize(app_id: str) -> str:
        """Spark's ``EventLogFileWriter.nameForAppAndAttempt`` sanitizing."""
        return re.sub(r"[.${}'\"]", "_", re.sub(r"[ :/]", "-", app_id)).lower()

    def _entries_for_app(self, app_id: str) -> List[Tuple[_LogEntry, _AttemptSummary]]:
        stem = self._sanitize(app_id)
        out = []
        for refresh in (False, True):
            candidates = [
                e
                for e in self._entries(refresh=refresh)
                if e.stem == stem or e.stem.startswith(stem + "_")
            ]
            for e in candidates:
                s = self._summary(e)
                if not s.is_event_log:
                    continue
                if s.app_id == app_id or (s.app_id or "").lower() == stem:
                    out.append((e, s))
            if out:
                break
        return out

    def _require_app(self, app_id: str) -> List[Tuple[_LogEntry, _AttemptSummary]]:
        found = self._entries_for_app(app_id)
        if not found:
            raise NotFoundException(
                status=404,
                reason="Not Found",
                body=f"no such app: {app_id} (no event log in {self.log_dir})",
            )
        return found

    def _attempt_entry(
        self, app_id: str, app_attempt_id: Optional[str]
    ) -> Tuple[_LogEntry, _AttemptSummary]:
        found = self._require_app(app_id)
        if app_attempt_id:
            for e, s in found:
                if (s.attempt_id or "") == app_attempt_id:
                    return e, s
            raise NotFoundException(
                status=404,
                reason="Not Found",
                body=f"no attempt {app_attempt_id} for {app_id}",
            )

        # Latest attempt: highest numeric attempt id, then most recent start.
        def key(item):
            _, s = item
            try:
                num = int(s.attempt_id) if s.attempt_id else -1
            except ValueError:
                num = -1
            return (num, s.start_ms or 0)

        return max(found, key=key)

    @staticmethod
    def _to_attempt(s: _AttemptSummary) -> ApplicationAttempt:
        duration = None
        if s.start_ms is not None and s.end_ms is not None:
            duration = max(0, s.end_ms - s.start_ms)
        return ApplicationAttempt(
            attemptId=s.attempt_id,
            startTime=_fmt_time(s.start_ms),
            endTime=_fmt_time(s.end_ms) if s.completed else _fmt_time(-1),
            lastUpdated=_fmt_time(s.last_updated_ms),
            duration=duration if s.completed else None,
            sparkUser=s.spark_user,
            completed=s.completed,
            appSparkVersion=s.spark_version,
            startTimeEpoch=s.start_ms,
            endTimeEpoch=s.end_ms if s.completed else -1,
            lastUpdatedEpoch=s.last_updated_ms,
        )

    def _to_application(
        self, app_id: str, attempts: List[_AttemptSummary]
    ) -> Application:
        attempts = sorted(attempts, key=lambda s: s.start_ms or 0, reverse=True)
        return Application(
            id=app_id,
            name=next((s.name for s in attempts if s.name), None),
            attempts=[self._to_attempt(s) for s in attempts],
        )

    # ------------------------------------------------------------------
    # SparkRestClient interface: answered from the log files
    # ------------------------------------------------------------------
    def list_applications(
        self,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Application]:
        """List applications in the directory, newest first.

        End time/duration come from ``SparkListenerApplicationEnd`` when it
        can be read cheaply (the tail of an uncompressed log), otherwise from
        the log's last write -- a completed log is closed right after that
        event, so the two differ by about a second.
        """
        from concurrent.futures import ThreadPoolExecutor

        entries = self._entries()
        # Newest first so a small ``limit`` only reads the headers it needs.
        entries = sorted(entries, key=lambda e: e.mtime, reverse=True)
        statuses = {s.lower() for s in status or []}
        lo, hi = _parse_date(min_date), _parse_date(max_date)
        end_lo, end_hi = _parse_date(min_end_date), _parse_date(max_end_date)

        def keep(s: _AttemptSummary) -> bool:
            if statuses and ("completed" if s.completed else "running") not in statuses:
                return False
            if lo is not None and (s.start_ms or 0) < lo:
                return False
            if hi is not None and (s.start_ms or 0) > hi:
                return False
            if end_lo is not None and (not s.completed or (s.end_ms or 0) < end_lo):
                return False
            if end_hi is not None and (not s.completed or (s.end_ms or 0) > end_hi):
                return False
            return True

        apps: Dict[str, List[_AttemptSummary]] = {}
        batch = 16
        with ThreadPoolExecutor(max_workers=8) as pool:
            for i in range(0, len(entries), batch):
                for s in pool.map(self._safe_summary, entries[i : i + batch]):
                    if s is not None and s.is_event_log and keep(s):
                        apps.setdefault(s.app_id, []).append(s)
                if limit is not None and len(apps) >= limit:
                    break

        result = [self._to_application(app_id, atts) for app_id, atts in apps.items()]
        result.sort(
            key=lambda a: (
                max((t.start_time_epoch or 0) for t in a.attempts or [])
                if a.attempts
                else 0
            ),
            reverse=True,
        )
        return result[:limit] if limit is not None else result

    def _safe_summary(self, entry: _LogEntry) -> Optional[_AttemptSummary]:
        try:
            return self._summary(entry)
        except Exception as exc:  # one unreadable log must not hide the rest
            logger.warning("skipping event log %s: %s", entry.path, exc)
            return None

    def get_application(self, app_id: str) -> Application:
        found = self._require_app(app_id)
        attempts = [s for _, s in found]
        return self._to_application(app_id, attempts)

    def get_environment(
        self, app_id: str, app_attempt_id: Optional[str] = None
    ) -> Environment:
        entry, _ = self._attempt_entry(app_id, app_attempt_id)
        key = entry.cache_key
        with self._cache_lock:
            cached = self._environments.get(key)
        if cached is not None:
            return cached
        env_event, profiles = None, []
        for ev in self._header_events(entry):
            kind = ev.get("Event")
            if kind == "SparkListenerEnvironmentUpdate":
                env_event = ev
            elif kind == "SparkListenerResourceProfileAdded":
                profiles.append(ev)
        if env_event is None:
            raise NotFoundException(
                status=404,
                reason="Not Found",
                body=f"no SparkListenerEnvironmentUpdate in the event log of {app_id}",
            )
        env = _build_environment(env_event, profiles)
        with self._cache_lock:
            self._environments[key] = env
        return env

    def download_event_log(
        self, app_id: str, app_attempt_id: Optional[str] = None
    ) -> bytes:
        """The raw log(s) zipped exactly like the History Server's /logs."""
        if app_attempt_id:
            entries = [self._attempt_entry(app_id, app_attempt_id)[0]]
        else:
            entries = [e for e, _ in self._require_app(app_id)]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
            for e in entries:
                if e.rolling:
                    for part in e.parts or []:
                        child = part.rstrip("/").rsplit("/", 1)[-1]
                        zf.writestr(f"{e.name}/{child}", self._read_all(part))
                else:
                    zf.writestr(e.name, self._read_all(e.path))
        return buf.getvalue()

    def get_gluten_ui_summary(self, app_id: str, app_attempt_id: Optional[str] = None):
        """Use a History Server's /gluten/json only if one is already up for
        this directory; otherwise ``None`` so callers read the raw log."""
        if self._shs is None:
            return None
        return self._shs.get_gluten_ui_summary(app_id, app_attempt_id=app_attempt_id)

    @staticmethod
    def _app_path(app_id: str, app_attempt_id: Optional[str] = None) -> str:
        return f"{app_id}/{app_attempt_id}" if app_attempt_id else app_id

    # ------------------------------------------------------------------
    # Everything else: a local History Server over the same directory
    # ------------------------------------------------------------------
    def history_server_client(self):
        """The ``SparkRestClient`` of a local History Server for this
        directory, starting one on first use."""
        with self._shs_lock:
            if self._shs is not None:
                return self._shs
            if self._history_server_factory is not None:
                self._shs = self._history_server_factory()
            else:
                self._shs = self._start_history_server()
            self._shs_started_at = time.time()
            return self._shs

    def _start_history_server(self):
        from spark_history_mcp.api.spark_client import SparkRestClient
        from spark_history_mcp.tools.local_shs import ensure_local_history_server

        if self.config.auto_history_server is False:
            raise RuntimeError(
                f"server '{self.name}' reads event logs directly and "
                "auto_history_server is disabled; this call needs a History "
                "Server (stage/task/executor/SQL data)"
            )
        logger.info("starting local History Server for %s", self.log_dir)
        started = ensure_local_history_server(
            _to_hadoop_uri(self.log_dir),
            # No fixed name: reuse any live local server for this directory
            # (e.g. one started earlier with start_history_server).
            memory=self.config.history_server_memory or "4g",
            wait_seconds=180,
            owner=f"eventlog:{self.name}",
        )
        if started["status"] == "starting":
            raise RuntimeError(
                "local History Server did not come up in time: "
                + started.get("log_tail", "")[-600:]
            )
        return SparkRestClient(ServerConfig(url=started["url"], timeout=300))

    def _forward(self, attr: str):
        def call(*args, **kwargs):
            shs = self.history_server_client()
            method = getattr(shs, attr)
            deadline = time.time() + _SHS_APP_WAIT_SECONDS
            while True:
                try:
                    return method(*args, **kwargs)
                except NotFoundException:
                    # A just-started server parses the directory in the
                    # background; wait for the app if the log exists.
                    app_id = kwargs.get("app_id") or (args[0] if args else None)
                    if (
                        not app_id
                        or time.time() >= deadline
                        or not self._entries_for_app(app_id)
                    ):
                        raise
                    try:
                        shs.get_application(app_id)
                    except NotFoundException:
                        time.sleep(3)
                        continue
                    return method(*args, **kwargs)

        call.__name__ = attr
        return call

    def __getattr__(self, attr: str):
        # Only called for attributes not defined above: forward public
        # SparkRestClient methods to the History Server.
        if attr.startswith("_"):
            raise AttributeError(attr)
        from spark_history_mcp.api.spark_client import SparkRestClient

        if not callable(getattr(SparkRestClient, attr, None)):
            raise AttributeError(attr)
        return self._forward(attr)


def _pairs(mapping: Any) -> List[List[str]]:
    """{k: v} -> sorted [[k, v]] with redaction, like the History Server."""
    if not isinstance(mapping, dict):
        return []
    out = []
    for k, v in mapping.items():
        v = "" if v is None else str(v)
        if _REDACT_RE.search(str(k)):
            v = _REDACTED
        out.append([str(k), v])
    out.sort(key=lambda kv: kv[0])
    return out


def _build_environment(
    env_event: Dict[str, Any], profiles: List[Dict[str, Any]]
) -> Environment:
    jvm = env_event.get("JVM Information") or {}
    resource_profiles = []
    for p in profiles:
        resource_profiles.append(
            {
                "id": p.get("Resource Profile Id"),
                "executorResources": {
                    name: {
                        "resourceName": r.get("Resource Name", name),
                        "amount": r.get("Amount"),
                        "discoveryScript": r.get("Discovery Script", ""),
                        "vendor": r.get("Vendor", ""),
                    }
                    for name, r in (p.get("Executor Resource Requests") or {}).items()
                },
                "taskResources": {
                    name: {
                        "resourceName": r.get("Resource Name", name),
                        "amount": r.get("Amount"),
                    }
                    for name, r in (p.get("Task Resource Requests") or {}).items()
                },
            }
        )
    return Environment.from_dict(
        {
            "runtime": {
                "javaVersion": jvm.get("Java Version"),
                "javaHome": jvm.get("Java Home"),
                "scalaVersion": jvm.get("Scala Version"),
            },
            "sparkProperties": _pairs(env_event.get("Spark Properties")),
            "hadoopProperties": _pairs(env_event.get("Hadoop Properties")),
            "systemProperties": _pairs(env_event.get("System Properties")),
            "metricsProperties": _pairs(env_event.get("Metrics Properties")),
            "classpathEntries": _pairs(env_event.get("Classpath Entries")),
            "resourceProfiles": resource_profiles,
        }
    )
