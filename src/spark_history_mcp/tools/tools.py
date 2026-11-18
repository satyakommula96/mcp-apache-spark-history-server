import heapq
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations

from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.sql_execution import SQLExecution
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.core.app import tool
from spark_history_mcp.models.mcp_types import (
    FailedTask,
    NativeEngineInfo,
    NativeEngineMetrics,
    NativeEngineSpillHit,
    NativeOperatorMetrics,
    SqlCompareSide,
    SqlExecutionComparison,
    SqlExecutionDetail,
    SqlExecutionSummary,
    SqlJobSummary,
    SqlNodeMetrics,
    SqlNodeTypeDiff,
    SqlPlanComparison,
    SqlStageSummary,
    StageCompareSide,
    StageComparison,
    StageMetricsAggregation,
    StageTaskQuantiles,
)

from ..utils.utils import error_text, parallel_execute

logger = logging.getLogger(__name__)


def _parse_spark_datetime(
    value: Union[str, int, float, datetime, None],
) -> Optional[datetime]:
    """Parse a Spark REST API timestamp into a ``datetime``.

    The OpenAPI-generated models expose timestamps as ISO strings (e.g.
    ``"2025-08-05T00:52:08.178GMT"``). This helper normalises the various
    representations the API and tests may produce:

    * ``None`` -> ``None``
    * ``datetime`` -> returned unchanged (used by unit tests with mocks)
    * epoch milliseconds (``int``/``float``) -> converted via ``fromtimestamp``
    * ``"...GMT"`` strings -> parsed as UTC
    * other ISO-8601 strings -> parsed via ``datetime.fromisoformat``

    Returns ``None`` when the value cannot be parsed, so callers can treat an
    unparseable timestamp the same as a missing one.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000)
    if isinstance(value, str):
        if value.endswith("GMT"):
            try:
                return datetime.strptime(
                    value.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z"
                )
            except ValueError:
                return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _duration_seconds(start: Any, end: Any) -> float:
    """Return the duration in seconds between two Spark timestamps.

    Returns ``0`` when either endpoint is missing or unparseable.
    """
    start_dt = _parse_spark_datetime(start)
    end_dt = _parse_spark_datetime(end)
    if start_dt and end_dt:
        return (end_dt - start_dt).total_seconds()
    return 0


def _duration_ms(start: Any, end: Any) -> int:
    """Return the duration in whole milliseconds between two Spark timestamps."""
    return int(_duration_seconds(start, end) * 1000)


# Lower number = shown first. Anything not listed sorts last, then by duration descending.
_SQL_STATUS_PRIORITY = {"FAILED": 0, "RUNNING": 1, "COMPLETED": 2}
_JOB_STATUS_PRIORITY = {"FAILED": 0, "RUNNING": 1, "UNKNOWN": 2, "SUCCEEDED": 3}
_STAGE_STATUS_PRIORITY = {
    "FAILED": 0,
    "COMPLETE": 1,
    "ACTIVE": 2,
    "PENDING": 3,
    "SKIPPED": 4,
}


def _strip_initial_plans(plan: str) -> str:
    """Remove ``== Initial Plan ==`` sections from an AQE plan description.

    Keeps only the final/current plan.
    Blocks are detected by the ``+- == Initial Plan ==`` marker and
    removed along with all lines indented deeper than the marker.
    """
    lines = plan.split("\n")
    out: List[str] = []
    i = 0
    while i < len(lines):
        if lines[i].strip().startswith("+- == Initial Plan =="):
            marker_indent = len(lines[i]) - len(lines[i].lstrip(" "))
            i += 1
            while i < len(lines):
                line_indent = len(lines[i]) - len(lines[i].lstrip(" "))
                if lines[i] == "" or line_indent > marker_indent:
                    i += 1
                else:
                    break
            continue
        out.append(lines[i])
        i += 1
    return "\n".join(out).rstrip("\n")


def _collect_sql_job_ids(execution: SQLExecution) -> List[int]:
    """Return all job IDs (success + failed + running) for a SQL execution."""
    ids: List[int] = []
    for group in (
        execution.success_job_ids,
        execution.failed_job_ids,
        execution.running_job_ids,
    ):
        if group:
            ids.extend(group)
    return ids


def _stage_ids_from_jobs(jobs: List[Job]) -> set:
    """Return the set of stage IDs referenced by the given jobs."""
    stage_ids: set = set()
    for job in jobs:
        if job.stage_ids:
            stage_ids.update(job.stage_ids)
    return stage_ids


def _aggregate_stages(
    stages: List[StageData], stage_ids: Optional[set]
) -> StageMetricsAggregation:
    """Aggregate stage metrics, optionally scoped to a set of stage IDs.

    De-duplicates by stage ID (keeping the first attempt seen)
    """
    agg = StageMetricsAggregation()
    seen: set = set()
    for stage in stages:
        sid = stage.stage_id
        if (stage_ids is not None and sid not in stage_ids) or sid in seen:
            continue
        seen.add(sid)
        if stage.status == "SKIPPED":  # never ran; its num_tasks are phantom
            continue
        agg.stage_count += 1
        agg.tasks += stage.num_tasks or 0
        agg.duration += _duration_ms(stage.submission_time, stage.completion_time)
        agg.input_bytes += stage.input_bytes or 0
        agg.shuffle_read_bytes += stage.shuffle_read_bytes or 0
        agg.shuffle_write_bytes += stage.shuffle_write_bytes or 0
        agg.disk_bytes_spilled += stage.disk_bytes_spilled or 0
        agg.jvm_gc_time += stage.jvm_gc_time or 0
    return agg


def _sort_sql_executions(
    executions: List[SQLExecution], sort_by: Optional[str]
) -> List[SQLExecution]:
    def duration(e: SQLExecution) -> int:
        return e.duration or 0

    if sort_by == "duration":
        return sorted(executions, key=duration, reverse=True)
    if sort_by == "id":
        return sorted(executions, key=lambda e: e.id or 0, reverse=True)
    if sort_by is not None:
        raise ValueError(f"invalid sort_by {sort_by!r}; expected 'duration' or 'id'")
    # default: failed first, then by duration descending
    return sorted(
        executions,
        key=lambda e: (_SQL_STATUS_PRIORITY.get(e.status, 99), -duration(e)),
    )


def _default_sort_jobs(jobs: List[Job]) -> List[Job]:
    """Sort jobs failed-first, then by duration descending."""
    return sorted(
        jobs,
        key=lambda j: (
            _JOB_STATUS_PRIORITY.get(j.status, 99),
            -_duration_ms(j.submission_time, j.completion_time),
        ),
    )


def _sort_jobs(jobs: List[Job], sort_by: Optional[str]) -> List[Job]:
    if sort_by == "duration":
        return sorted(
            jobs,
            key=lambda j: _duration_ms(j.submission_time, j.completion_time),
            reverse=True,
        )
    if sort_by == "failed-tasks":
        return sorted(jobs, key=lambda j: j.num_failed_tasks or 0, reverse=True)
    if sort_by == "id":
        return sorted(jobs, key=lambda j: j.job_id or 0, reverse=True)
    if sort_by is not None:
        raise ValueError(
            f"invalid sort_by {sort_by!r}; expected 'duration', 'failed-tasks', or 'id'"
        )
    return _default_sort_jobs(jobs)


def _default_sort_stages(stages: List[StageData]) -> List[StageData]:
    """Sort stages by status priority, then by duration descending."""
    return sorted(
        stages,
        key=lambda s: (
            _STAGE_STATUS_PRIORITY.get(s.status, 99),
            -_duration_ms(s.submission_time, s.completion_time),
        ),
    )


def _sort_stages(stages: List[StageData], sort_by: Optional[str]) -> List[StageData]:
    if sort_by == "duration":
        return sorted(
            stages,
            key=lambda s: _duration_ms(s.submission_time, s.completion_time),
            reverse=True,
        )
    if sort_by == "failed-tasks":
        return sorted(stages, key=lambda s: s.num_failed_tasks or 0, reverse=True)
    if sort_by == "id":
        return sorted(stages, key=lambda s: s.stage_id or 0, reverse=True)
    if sort_by is not None:
        raise ValueError(
            f"invalid sort_by {sort_by!r}; expected 'duration', 'failed-tasks', or 'id'"
        )
    return _default_sort_stages(stages)


def _sort_executors(
    executors: List[Executor], sort_by: Optional[str]
) -> List[Executor]:
    if sort_by == "failed-tasks":
        return sorted(executors, key=lambda e: e.failed_tasks or 0, reverse=True)
    if sort_by == "duration":
        return sorted(executors, key=lambda e: e.total_duration or 0, reverse=True)
    if sort_by == "gc":
        return sorted(executors, key=lambda e: e.total_gc_time or 0, reverse=True)
    if sort_by == "id":
        # "driver" first, then numeric ids ascending ("2" before "10").
        return sorted(
            executors,
            key=lambda e: (0, 0, "") if e.id == "driver"
            else (1, int(e.id), "") if (e.id or "").isdigit() else (2, 0, e.id or ""),
        )
    if sort_by is not None:
        raise ValueError(
            f"invalid sort_by {sort_by!r}; expected 'failed-tasks', 'duration', 'gc', or 'id'"
        )
    # default: active executors first, then by duration descending
    return sorted(
        executors,
        key=lambda e: (0 if e.is_active else 1, -(e.total_duration or 0)),
    )


def _count_node_types(nodes) -> Dict[str, int]:
    """Count plan nodes by node name."""
    counts: Dict[str, int] = {}
    for node in nodes or []:
        # Scan names embed the table ("Scan parquet spark_catalog.db.t") and
        # Spark 3.5 adds the catalog prefix -- compare operator types only.
        name = re.sub(r"^((?:\w*Scan\w*) \w+) .*", r"\1", node.node_name or "")
        counts[name] = counts.get(name, 0) + 1
    return counts


# Gluten replaces its plugin class name when the project moved under the
# Apache umbrella; accept both so older event logs are still detected.
_GLUTEN_PLUGIN_CLASSES = (
    "io.glutenproject.glutenplugin",
    "org.apache.gluten.glutenplugin",
)
_GLUTEN_BACKEND_CONF_KEY = "spark.gluten.sql.columnar.backend.lib"

# Gluten's "*Transformer" naming convention (ProjectExecTransformer,
# HashAggregateExecTransformer, ...) is stable across its Velox and
# ClickHouse backends, as are its columnar<->row conversion node names.
# Bare "ColumnarToRow"/"RowToColumnar" are NOT native: vanilla Spark emits
# ColumnarToRow for its own vectorized Parquet/ORC reader. Gluten's
# conversions are prefixed (VeloxColumnarToRow, RowToVeloxColumnar,
# CHColumnarToRow ...), which "velox"/the CH prefix below still catch.
_NATIVE_NODE_PATTERN = re.compile(
    r"transformer|velox|nativefilescan|\bch(columnartorow|rowtocolumnar)", re.IGNORECASE
)
_SPILL_METRIC_PATTERN = re.compile(r"spill", re.IGNORECASE)
_ZERO_METRIC_VALUE_PATTERN = re.compile(r"^0(\.0+)?(\s*[a-zA-Z]+)?$")


def _detect_native_engine(env: Optional[Environment]) -> NativeEngineInfo:
    """Detect whether an application ran on the Gluten/Velox native engine.

    Gluten offloads SQL execution to a native backend (typically Velox) via a
    Spark plugin registered in ``spark.plugins``. When active, most physical
    plan operators are replaced with native "*Transformer" nodes whose
    metrics (spill, native memory, native wall time) only show up in the SQL
    plan node metrics -- not in the standard JVM stage/task fields the rest
    of this module otherwise relies on.
    """
    props = dict(env.spark_properties or []) if env else {}
    plugins = {p.strip().lower() for p in (props.get("spark.plugins") or "").split(",") if p.strip()}
    is_native = any(p in plugins for p in _GLUTEN_PLUGIN_CLASSES) or any(
        "gluten" in p for p in plugins
    )
    backend = (props.get(_GLUTEN_BACKEND_CONF_KEY) or "").strip().lower() or None
    return NativeEngineInfo(
        engine="gluten" if is_native else "spark",
        backend=backend,
        is_native=is_native,
    )


def _is_native_node(node_name: Optional[str]) -> bool:
    return bool(node_name and _NATIVE_NODE_PATTERN.search(node_name))


# Multi-task SQL metrics render as
#   "total (min, med, max (stageId: taskId))\n0.0 B (0.0 B, 0.0 B, 0.0 B (stage 6.0: task 3771))"
# -- only the leading total decides whether anything happened.
_METRIC_TOTAL_HEADER_RE = re.compile(r"^total \(min, med, max[^\n]*?\)\)\s*", re.IGNORECASE)


def _metric_total(value: Optional[str]) -> str:
    v = _METRIC_TOTAL_HEADER_RE.sub("", (value or "").strip())
    return v.split(" (", 1)[0].strip()


def _is_nonzero_metric_value(value: Optional[str]) -> bool:
    v = _metric_total(value)
    return bool(v) and not _ZERO_METRIC_VALUE_PATTERN.match(v)


def _build_native_operator_metrics(
    execution_id: Optional[int], nodes
) -> List[NativeOperatorMetrics]:
    """Curated per-operator metrics for the native (Gluten/Velox) nodes in a
    SQL plan, in the same shape as ``_build_node_metrics`` but filtered down
    to native operators only."""
    result: List[NativeOperatorMetrics] = []
    for n in nodes or []:
        if not _is_native_node(n.node_name) or not n.metrics:
            continue
        metrics: Dict[str, str] = {}
        for m in n.metrics:
            metrics[m.name] = " ".join((m.value or "").split())
        if metrics:
            result.append(
                NativeOperatorMetrics(
                    sql_execution_id=execution_id,
                    node_id=n.node_id,
                    node_name=n.node_name,
                    metrics=metrics,
                )
            )
    return result


def _find_native_spill_hits(
    execution_id: Optional[int], nodes
) -> List[NativeEngineSpillHit]:
    """Find non-zero spill-related metrics on native operators.

    Native (e.g. Velox) spill is invisible to the standard stage-level
    ``memoryBytesSpilled``/``diskBytesSpilled`` fields, so this is the only
    way to detect it for Gluten/Velox jobs.
    """
    hits: List[NativeEngineSpillHit] = []
    for n in nodes or []:
        if not _is_native_node(n.node_name):
            continue
        for m in n.metrics or []:
            if (
                m.name
                and _SPILL_METRIC_PATTERN.search(m.name)
                and _is_nonzero_metric_value(m.value)
            ):
                hits.append(
                    NativeEngineSpillHit(
                        sql_execution_id=execution_id,
                        node_id=n.node_id,
                        node_name=n.node_name,
                        metric=m.name,
                        value=" ".join((m.value or "").split()),
                    )
                )
    return hits


def get_client_or_default(
    ctx, server_name: Optional[str] = None, app_id: Optional[str] = None
):
    """
    Get a client by server name, app discovery, or default client.

    Args:
        ctx: The MCP context
        server_name: Optional server name
        app_id: Optional app ID for discovery

    Returns:
        SparkRestClient: The requested client

    Raises:
        ValueError: If no client is found
    """
    app_discovery = ctx.request_context.lifespan_context.app_discovery
    default_client = ctx.request_context.lifespan_context.default_client

    # If app_id provided, use discovery
    if app_id and not server_name:
        client, _ = app_discovery.get_client_for_app(app_id, server_name)
        return client

    clients = ctx.request_context.lifespan_context.clients

    # An explicitly requested server must exist; do not silently fall back to
    # the default, or callers can't tell which server actually answered.
    if server_name:
        client = clients.get(server_name)
        if client is None:
            available = ", ".join(sorted(clients)) or "none"
            raise ValueError(
                f"No Spark server named '{server_name}' is configured "
                f"(available servers: {available})."
            )
        return client

    if default_client:
        return default_client

    raise ValueError(
        "No Spark client found. Please specify a valid server name or set a default server."
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_applications(
    server: Optional[str] = None,
    app_id: Optional[str] = None,
    status: Optional[list[str]] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    min_end_date: Optional[str] = None,
    max_end_date: Optional[str] = None,
    limit: Optional[int] = None,
    *,
    ctx: Context,
) -> list[Application]:
    """
    Get applications from the Spark History Server.

    Pass ``app_id`` to retrieve a single application by its ID, returned as a
    one-element list (the other filters are ignored in that case).

    Args:
        server: Optional server name to use (uses default if not specified)
        app_id: Optional application ID to fetch a single application
        status: Optional list of application states to filter by, e.g. ["completed"] or ["running"]
        min_date: Optional earliest start date/time to list
        max_date: Optional latest start date/time to list
        min_end_date: Optional earliest end date/time to list
        max_end_date: Optional latest end date/time to list
        limit: Optional maximum number of applications to return (limits the number of applications listed)
    Date format:
        - Accepted: YYYY-MM-DD["T"HH:mm:ss.SSS"GMT"]
        - Time is optional. If present, it must include milliseconds and the literal GMT.
        - Examples: 2015-02-10, 2015-02-03T16:42:40.000GMT
        - Timezone: All values are interpreted as GMT.
    Returns:
        List of Application objects for all applications
    """

    if app_id:
        client = get_client_or_default(ctx, server, app_id)
        return [client.get_application(app_id)]

    if server:
        # Return from specific server
        client = get_client_or_default(ctx, server)
        return client.list_applications(
            status=status,
            min_date=min_date,
            max_date=max_date,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            limit=limit,
        )
    else:
        # Return from all servers
        all_apps = []
        clients = ctx.request_context.lifespan_context.clients

        seen_urls = set()
        for server_name, client in clients.items():
            url = getattr(getattr(client, "config", None), "url", None)
            if isinstance(url, str):
                if url.rstrip("/") in seen_urls:
                    continue  # alias of a server already listed
                seen_urls.add(url.rstrip("/"))
            try:
                apps = client.list_applications(
                    status=status,
                    min_date=min_date,
                    max_date=max_date,
                    min_end_date=min_end_date,
                    max_end_date=max_end_date,
                    limit=limit,
                )
                all_apps.extend(apps)
            except Exception as e:
                logger.warning(
                    f"Failed to get applications from server '{server_name}': {e}"
                )
                continue  # Skip unreachable servers

        return all_apps


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_jobs(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    job_id: Optional[int] = None,
    sort_by: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    offset: int = 0,
    length: Optional[int] = None,
    *,
    ctx: Context,
) -> list:
    """
    Get a list of jobs for a Spark application.

    Returns job metadata including ID, name, status, submission/completion times,
    and task counts. Supports client-side pagination to limit response size.

    By default jobs are ordered failed-status-first, then by duration descending.

    Pass ``job_id`` to retrieve a single job by its ID (the returned ``Job``
    already carries its full detail, e.g. failed/killed/skipped task and stage
    counts).

    Use ``sort_by="duration"`` with ``length=N`` to get the N slowest jobs.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of job status values to filter by
        job_id: Optional job ID to return only that job
        sort_by: Optional ordering, all descending: "duration", "failed-tasks",
            or "id". When unset, failed jobs come first, then by duration descending.
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        offset: Number of jobs to skip from the start (default: 0)
        length: Maximum number of jobs to return (default: None, returns all)

    Returns:
        List of Job objects for the application
    """
    client = get_client_or_default(ctx, server, app_id)

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if length is not None and length < 0:
        raise ValueError("length must be non-negative")

    jobs = client.list_jobs(app_id=app_id, status=status, app_attempt_id=app_attempt_id)

    if job_id is not None:
        jobs = [j for j in jobs if j.job_id == job_id]

    jobs = _sort_jobs(jobs, sort_by)

    if offset:
        jobs = jobs[offset:]
    if length is not None:
        jobs = jobs[:length]

    return jobs


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_stages(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    sort_by: Optional[str] = None,
    with_summaries: bool = False,
    app_attempt_id: Optional[str] = None,
    offset: int = 0,
    length: Optional[int] = None,
    *,
    ctx: Context,
) -> list:
    """
    Get a list of stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics. Supports client-side
    pagination to limit response size.

    By default stages are ordered by status priority (failed first), then by
    duration descending. Use ``sort_by="duration"`` with ``length=N`` for the N
    slowest stages.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of stage status values to filter by
        sort_by: Optional ordering, all descending: "duration", "failed-tasks",
            or "id". When unset, failed stages come first, then by duration descending.
        with_summaries: Whether to include summary metrics in the response
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        offset: Number of stages to skip from the start (default: 0)
        length: Maximum number of stages to return (default: None, returns all)

    Returns:
        List of StageData objects for the application
    """
    client = get_client_or_default(ctx, server, app_id)

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if length is not None and length < 0:
        raise ValueError("length must be non-negative")

    stages = client.list_stages(
        app_id=app_id,
        status=status,
        with_summaries=with_summaries,
        app_attempt_id=app_attempt_id,
    )

    stages = _sort_stages(stages, sort_by)

    if offset:
        stages = stages[offset:]
    if length is not None:
        stages = stages[:length]

    return stages


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_stage(
    app_id: str,
    stage_id: int,
    attempt_id: Optional[int] = None,
    server: Optional[str] = None,
    with_summaries: bool = False,
    quantiles: Optional[str] = None,
    *,
    ctx: Context,
) -> StageData:
    """
    Get information about a specific stage.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: Optional stage attempt ID (if not provided, returns the latest attempt)
        server: Optional server name to use (uses default if not specified)
        with_summaries: Whether to include task metric distributions for the stage
        quantiles: Optional comma-separated quantiles for the task metric
            distributions (only used when with_summaries is set; server default if omitted)

    Returns:
        StageData object containing stage information
    """
    client = get_client_or_default(ctx, server, app_id)

    if attempt_id is not None:
        # Get specific attempt
        stage_data = client.get_stage_attempt(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            details=False,
            with_summaries=with_summaries,
        )
    else:
        # Get all attempts and use the latest one
        stages = client.list_stage_attempts(
            app_id=app_id,
            stage_id=stage_id,
            details=False,
            with_summaries=with_summaries,
        )

        if not stages:
            raise ValueError(f"No stage found with ID {stage_id}")

        # If multiple attempts exist, get the one with the highest attempt_id
        if isinstance(stages, list):
            stage_data = max(stages, key=lambda s: s.attempt_id)
        else:
            stage_data = stages

    # If summaries were requested but metrics distributions are missing, fetch them separately
    if with_summaries and (
        not hasattr(stage_data, "task_metrics_distributions")
        or stage_data.task_metrics_distributions is None
    ):
        summary_kwargs = {"quantiles": quantiles} if quantiles else {}
        task_summary = client.get_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_data.attempt_id,
            **summary_kwargs,
        )
        stage_data.task_metrics_distributions = task_summary

    return stage_data


def _resolve_latest_stage_attempt_id(
    client, app_id: str, stage_id: int, app_attempt_id: Optional[str] = None
) -> int:
    """Return the highest attempt id for a stage, or raise if the stage is absent."""
    kwargs = {"app_attempt_id": app_attempt_id} if app_attempt_id else {}
    attempts = client.list_stage_attempts(
        app_id=app_id,
        stage_id=stage_id,
        details=False,
        with_summaries=False,
        **kwargs,
    )
    if not attempts:
        raise ValueError(
            f"No stage found with ID {stage_id} in application {app_id} "
            "(it may have been skipped by AQE)"
        )
    return max(attempts, key=lambda s: s.attempt_id or 0).attempt_id or 0


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_stage_task_failures(
    app_id: str,
    stage_id: int,
    server: Optional[str] = None,
    stage_attempt_id: Optional[int] = None,
    app_attempt_id: Optional[str] = None,
    full_messages: bool = False,
    *,
    ctx: Context,
) -> list:
    """
    Get the per-task error messages for a stage's failed tasks.

    Use this to diagnose *why* a stage failed: it returns the actual exception
    and stack trace recorded for each failed task, which the stage- and
    job-level tools do not expose. A typical workflow is to find a failed or
    slow stage with ``list_stages`` (or a failed job with ``list_jobs``), then
    call this tool with that stage ID to read the underlying errors.

    Only tasks with status ``FAILED`` are returned. By default the latest stage
    attempt is inspected; pass ``stage_attempt_id`` to target a specific one.
    Every failed task is returned (paginated, not the server's default 20).
    Native (Velox) stack traces can be ~65KB each and are usually identical
    across tasks, so by default each message is trimmed to its head plus all
    "Caused by" lines, and a task whose message repeats an earlier one gets
    ``"[same error as task N]"`` instead. Pass ``full_messages=True`` for the
    complete text of every task.

    Args:
        app_id: The Spark application ID.
        stage_id: The stage ID to inspect.
        server: Server name to query; uses the default server if omitted.
        stage_attempt_id: Specific stage attempt to inspect; defaults to the
            latest attempt.
        app_attempt_id: YARN application attempt ID; uses the latest if omitted.
        full_messages: Return every task's untrimmed error message (can be
            hundreds of thousands of tokens on native failures)

    Returns:
        A list of ``FailedTask`` objects, one per failed task, each with
        ``task_id``, ``attempt``, ``executor_id``, ``host``, ``status``, and the
        full ``error_message``. Empty if the stage attempt has no failed tasks.

    Raises:
        ValueError: If the stage does not exist (e.g. it was skipped by AQE).
    """
    client = get_client_or_default(ctx, server, app_id)

    if stage_attempt_id is None:
        stage_attempt_id = _resolve_latest_stage_attempt_id(
            client, app_id, stage_id, app_attempt_id
        )

    page = 500
    tasks: list = []
    while True:
        batch = client.list_stage_tasks(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_attempt_id,
            status="failed",
            app_attempt_id=app_attempt_id,
            offset=len(tasks),
            length=page,
        )
        tasks.extend(batch or [])
        if not batch or len(batch) < page:
            break

    first_task_for: Dict[str, Any] = {}
    out = []
    for t in tasks:
        msg = t.error_message
        if msg and not full_messages:
            key = _error_signature(msg)
            if key in first_task_for:
                msg = f"[same error as task {first_task_for[key]}]"
            else:
                first_task_for[key] = t.task_id
                msg = _trim_error_message(msg)
        out.append(
            FailedTask(
                task_id=t.task_id,
                attempt=t.attempt,
                executor_id=t.executor_id,
                host=t.host,
                status=t.status,
                error_message=msg,
            )
        )
    return out


# Numbers (ids, addresses, and the per-task memory figures in Velox's
# memory-pool dump, with their units) vary between otherwise identical errors.
_ERR_VOLATILE_RE = re.compile(r"0x[0-9a-f]+|\d[\d.,]*(?:\s*[KMGTPE]i?B\b)?", re.I)


def _error_signature(msg: str) -> str:
    """Error identity ignoring numbers and whitespace runs."""
    return " ".join(_ERR_VOLATILE_RE.sub("#", msg[:2000]).split())


def _trim_error_message(msg: str, head_chars: int = 3000, max_causes: int = 10) -> str:
    if len(msg) <= head_chars:
        return msg
    head = msg[:head_chars]
    causes = [ln.strip() for ln in msg[head_chars:].splitlines() if ln.lstrip().startswith("Caused by")]
    tail = "\n".join(causes[:max_causes])
    return f"{head}\n... [{len(msg) - head_chars} chars trimmed; full_messages=True for all]" + (
        f"\n{tail}" if tail else ""
    )


# Environment sections that can be requested individually via ``section``.
_ENVIRONMENT_SECTION_FIELDS = (
    "runtime",
    "spark_properties",
    "system_properties",
    "hadoop_properties",
    "metrics_properties",
    "classpath_entries",
)


def _resolve_environment_section(section: str) -> str:
    """Resolve a requested ``section`` to a canonical environment field name.

    Accepts the field names in ``_ENVIRONMENT_SECTION_FIELDS``, compared
    case-insensitively. Raises ``ValueError`` for anything else.
    """
    keep_field = section.strip().lower()
    if keep_field not in _ENVIRONMENT_SECTION_FIELDS:
        valid = ", ".join(repr(s) for s in _ENVIRONMENT_SECTION_FIELDS)
        raise ValueError(f"invalid section {section!r}; expected one of {valid}")
    return keep_field


def _filter_environment_section(env: Environment, section: str) -> Environment:
    """Return a copy of ``env`` with only the requested section populated.

    Fields for the other sections — and ``resource_profiles`` — are cleared so
    the response carries only the requested data.
    """
    keep_field = _resolve_environment_section(section)
    filtered = env.model_copy(deep=True)
    for env_field in _ENVIRONMENT_SECTION_FIELDS:
        if env_field != keep_field:
            setattr(filtered, env_field, None)
    filtered.resource_profiles = None
    return filtered


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_environment(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    section: Optional[str] = None,
    key_filter: Optional[str] = None,
    *,
    ctx: Context,
):
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        section: Optional section filter to return only one part of the
            environment. One of "runtime", "spark_properties",
            "system_properties", "hadoop_properties", "metrics_properties", or
            "classpath_entries", or "all". When omitted, everything EXCEPT
            hadoop_properties and classpath_entries is returned -- those two
            are ~85% of the payload (~22K tokens on a Dataproc app) and are
            rarely what you're looking for; request them explicitly.
        key_filter: Optional case-insensitive substring; keeps only property
            / classpath entries whose key or value contains it (e.g.
            "spark.gluten.dp" or "memory"). Applies to every returned section.

    Returns:
        ApplicationEnvironmentInfo object containing environment details. When
        ``section`` is given, only that section is populated.
    """
    client = get_client_or_default(ctx, server, app_id)

    env = client.get_environment(app_id=app_id, app_attempt_id=app_attempt_id)

    if section is not None and section.strip().lower() != "all":
        env = _filter_environment_section(env, section)
    elif section is None:
        env = env.model_copy(deep=True)
        env.hadoop_properties = None
        env.classpath_entries = None

    if key_filter:
        env = _filter_environment_keys(env, key_filter)

    return env


def _filter_environment_keys(env: Environment, key_filter: str) -> Environment:
    """Keep only [key, value] entries where key or value contains key_filter."""
    needle = key_filter.lower()
    filtered = env.model_copy(deep=True)
    for env_field in _ENVIRONMENT_SECTION_FIELDS:
        if env_field == "runtime":
            continue
        pairs = getattr(filtered, env_field, None)
        if pairs:
            setattr(
                filtered,
                env_field,
                [p for p in pairs if any(needle in str(x).lower() for x in p)],
            )
    return filtered


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_executors(
    app_id: str,
    server: Optional[str] = None,
    executor_id: Optional[str] = None,
    sort_by: Optional[str] = None,
    include_inactive: bool = False,
    app_attempt_id: Optional[str] = None,
    offset: int = 0,
    length: Optional[int] = None,
    *,
    ctx: Context,
):
    """
    Get executor information for a Spark application.

    Retrieves a list of executors (active by default) for the specified Spark application
    with their resource allocation, task statistics, and performance metrics. Supports
    client-side pagination to limit response size.

    By default executors are ordered active-first, then by duration descending.

    Pass ``executor_id`` to retrieve a single executor by its ID (searches all
    executors, including inactive; returns a list with the match or empty if none).

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        executor_id: Optional executor ID to return only that executor
        sort_by: Optional ordering: "failed-tasks", "duration", or "gc" (descending),
            or "id" (ascending). When unset, active executors come first, then by
            duration descending.
        include_inactive: Whether to include inactive executors (default: False)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        offset: Number of executors to skip from the start (default: 0)
        length: Maximum number of executors to return (default: None, returns all)

    Returns:
        List of Executor objects containing executor information
    """
    client = get_client_or_default(ctx, server, app_id)

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if length is not None and length < 0:
        raise ValueError("length must be non-negative")

    if executor_id is not None:
        # Single-executor lookup: search all executors, including inactive.
        executors = client.list_all_executors(
            app_id=app_id, app_attempt_id=app_attempt_id
        )
        return [e for e in executors if e.id == executor_id]

    if include_inactive:
        executors = client.list_all_executors(
            app_id=app_id, app_attempt_id=app_attempt_id
        )
    else:
        executors = client.list_executors(app_id=app_id, app_attempt_id=app_attempt_id)

    executors = _sort_executors(executors, sort_by)

    if offset:
        executors = executors[offset:]
    if length is not None:
        executors = executors[:length]

    return executors


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_executor_summary(app_id: str, server: Optional[str] = None, *, ctx: Context):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing aggregated executor metrics
    """
    client = get_client_or_default(ctx, server, app_id)

    executors = client.list_all_executors(app_id=app_id)
    return _calculate_executor_metrics(executors)


def _filter_threads(threads, state, name, blocked_only):
    """Filter thread-dump entries by state, name substring, and blocked status."""
    if not state and not name and not blocked_only:
        return list(threads)

    name_low = name.lower() if name else None
    result = []
    for t in threads:
        if state and (t.thread_state or "").upper() != state.upper():
            continue
        if name_low and name_low not in (t.thread_name or "").lower():
            continue
        # lock_name is set for any parked/waiting thread on Spark 3.5+, so it
        # can't signal contention.
        if blocked_only and t.thread_state != "BLOCKED" and t.blocked_by_thread_id is None:
            continue
        result.append(t)
    return result


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_executor_thread_dump(
    app_id: str,
    executor_id: str,
    server: Optional[str] = None,
    state: Optional[str] = None,
    name: Optional[str] = None,
    blocked_only: bool = False,
    app_attempt_id: Optional[str] = None,
    *,
    ctx: Context,
) -> list:
    """
    Get the JVM thread dump for a driver or executor.

    Returns one entry per thread, each with its ID, name, state, daemon flag,
    lock/blocking information, and full stack trace. Use ``executor_id="driver"``
    for the driver.

    The application must be **running**: completed applications return an error
    because the Spark History Server does not persist thread dumps.

    Args:
        app_id: The Spark application ID
        executor_id: Executor ID, or "driver" for the driver
        server: Optional server name to use (uses default if not specified)
        state: Optional thread-state filter (e.g. "RUNNABLE", "BLOCKED",
            "WAITING", "TIMED_WAITING"); case-insensitive exact match
        name: Optional case-insensitive substring match on the thread name
        blocked_only: When True, return only threads blocked on a lock or
            another thread
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)

    Returns:
        List of ThreadStackTrace objects, sorted by thread ID
    """
    client = get_client_or_default(ctx, server, app_id)

    threads = client.get_executor_thread_dump(
        app_id=app_id, executor_id=executor_id, app_attempt_id=app_attempt_id
    )

    threads = _filter_threads(threads, state, name, blocked_only)
    threads.sort(key=lambda t: t.thread_id if t.thread_id is not None else 0)
    return threads


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def compare_job_environments(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    server2: Optional[str] = None,
    include_common: bool = False,
    key_filter: Optional[str] = None,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Compare Spark environment configurations between two jobs.

    Identifies differences in Spark properties, JVM settings, system properties,
    and other configuration parameters between two Spark applications.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name for app_id1 (uses default if not specified)
        server2: Optional server name for app_id2 (defaults to ``server``) --
            set it to compare runs on different History Servers, e.g. a
            deb11 app against a deb12 app
        include_common: Also return every identical spark property (off by
            default: on a Dataproc app that's ~3.5K tokens of no signal; a
            ``common_count`` is always returned)
        key_filter: Optional case-insensitive substring to restrict the
            spark-property comparison to matching keys (e.g. "spark.gluten")

    Returns:
        Dictionary containing configuration differences and similarities
    """
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server2 or server, app_id2)

    env1 = client1.get_environment(app_id=app_id1)
    env2 = client2.get_environment(app_id=app_id2)

    def props_to_dict(props):
        return {k: v for k, v in props} if props else {}

    spark_props1 = props_to_dict(env1.spark_properties)
    spark_props2 = props_to_dict(env2.spark_properties)
    if key_filter:
        needle = key_filter.lower()
        spark_props1 = {k: v for k, v in spark_props1.items() if needle in k.lower()}
        spark_props2 = {k: v for k, v in spark_props2.items() if needle in k.lower()}

    system_props1 = props_to_dict(env1.system_properties)
    system_props2 = props_to_dict(env2.system_properties)

    comparison = {
        "applications": {
            "app1": app_id1,
            "app2": app_id2,
            "server1": server,
            "server2": server2 or server,
        },
        "runtime_comparison": {
            "app1": {
                "java_version": env1.runtime.java_version,
                "java_home": env1.runtime.java_home,
                "scala_version": env1.runtime.scala_version,
            },
            "app2": {
                "java_version": env2.runtime.java_version,
                "java_home": env2.runtime.java_home,
                "scala_version": env2.runtime.scala_version,
            },
        },
        "spark_properties": {
            "common": {
                k: {"app1": v, "app2": spark_props2.get(k)}
                for k, v in spark_props1.items()
                if k in spark_props2 and v == spark_props2[k]
            },
            "different": {
                k: {"app1": v, "app2": spark_props2.get(k, "NOT_SET")}
                for k, v in spark_props1.items()
                if k in spark_props2 and v != spark_props2[k]
            },
            "only_in_app1": {
                k: v for k, v in spark_props1.items() if k not in spark_props2
            },
            "only_in_app2": {
                k: v for k, v in spark_props2.items() if k not in spark_props1
            },
        },
        "system_properties": {
            "key_differences": {
                k: {
                    "app1": system_props1.get(k, "NOT_SET"),
                    "app2": system_props2.get(k, "NOT_SET"),
                }
                for k in [
                    "java.version",
                    "java.runtime.version",
                    "os.name",
                    "os.version",
                    "user.timezone",
                    "file.encoding",
                ]
                if system_props1.get(k) != system_props2.get(k)
            }
        },
    }

    common = comparison["spark_properties"]["common"]
    comparison["spark_properties"]["common_count"] = len(common)
    if not include_common:
        del comparison["spark_properties"]["common"]
    return comparison


def _calculate_executor_metrics(executors):
    """Calculate executor summary metrics from executor list."""

    def memory_used(executor):
        metrics = executor.memory_metrics
        if metrics is None:
            return 0
        return (metrics.used_on_heap_storage_memory or 0) + (
            metrics.used_off_heap_storage_memory or 0
        )

    return {
        "total_executors": len(executors),
        "active_executors": sum(1 for e in executors if e.is_active),
        "memory_used": sum(memory_used(e) for e in executors),
        "disk_used": sum(e.disk_used for e in executors),
        "completed_tasks": sum(e.completed_tasks for e in executors),
        "failed_tasks": sum(e.failed_tasks for e in executors),
        "total_duration": sum(e.total_duration for e in executors),
        "total_gc_time": sum(e.total_gc_time for e in executors),
        "total_input_bytes": sum(e.total_input_bytes for e in executors),
        "total_shuffle_read": sum(e.total_shuffle_read for e in executors),
        "total_shuffle_write": sum(e.total_shuffle_write for e in executors),
    }


def _calc_executor_summary_from_client(client, app_id: str):
    """Helper function to calculate executor summary without MCP context."""
    executors = client.list_all_executors(app_id=app_id)
    return _calculate_executor_metrics(executors)


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def compare_job_performance(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    server2: Optional[str] = None,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Compare performance metrics between two Spark jobs.

    Analyzes execution times, resource usage, task distribution, and other
    performance indicators to identify differences between jobs.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)
        server2: Optional server for app_id2 when it lives on a different
            History Server (e.g. deb11 vs deb12); defaults to server

    Returns:
        Dictionary containing detailed performance comparison
    """
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server2 or server, app_id2)

    # Define API calls for parallel execution
    api_calls = [
        ("app1", lambda: client1.get_application(app_id1)),
        ("app2", lambda: client2.get_application(app_id2)),
        ("exec_summary1", lambda: _calc_executor_summary_from_client(client1, app_id1)),
        ("exec_summary2", lambda: _calc_executor_summary_from_client(client2, app_id2)),
        ("jobs1", lambda: client1.list_jobs(app_id=app_id1)),
        ("jobs2", lambda: client2.list_jobs(app_id=app_id2)),
    ]

    # Execute all API calls in parallel
    execution_result = parallel_execute(
        api_calls,
        max_workers=6,
        timeout=300,  # Apply generous timeout for large scale Spark applications
    )

    # If parallel execution fails, try sequential as fallback
    if execution_result["errors"] and len(execution_result["results"]) == 0:
        try:
            # Sequential fallback - get basic info first
            app1 = client1.get_application(app_id1)
            app2 = client2.get_application(app_id2)

            # Use the actual errors from parallel execution
            error_summary = "; ".join(execution_result["errors"])
            return {
                "error": f"Parallel execution failed: {error_summary}. Falling back to basic app info only.",
                "partial_data": {
                    "app1": {"id": app_id1, "name": app1.name},
                    "app2": {"id": app_id2, "name": app2.name},
                },
            }
        except Exception as e:
            # If even basic app info fails, provide the original errors plus this failure
            all_errors = execution_result["errors"] + [
                f"Sequential fallback failed: {error_text(e)}"
            ]
            return {"error": f"Complete failure: {'; '.join(all_errors)}"}

    if execution_result["errors"]:
        return {"error": f"API failures: {'; '.join(execution_result['errors'])}"}

    results = execution_result["results"]

    # Extract results
    app1 = results["app1"]
    app2 = results["app2"]
    exec_summary1 = results["exec_summary1"]
    exec_summary2 = results["exec_summary2"]
    jobs1 = results["jobs1"]
    jobs2 = results["jobs2"]

    # Calculate job duration statistics
    def calc_job_stats(jobs):
        if not jobs:
            return {"count": 0, "total_duration": 0, "avg_duration": 0}

        completed_jobs = [j for j in jobs if j.completion_time and j.submission_time]
        if not completed_jobs:
            return {"count": len(jobs), "total_duration": 0, "avg_duration": 0}

        durations = [
            _duration_seconds(j.submission_time, j.completion_time)
            for j in completed_jobs
        ]

        return {
            "count": len(jobs),
            "completed_count": len(completed_jobs),
            "total_duration": sum(durations),
            "avg_duration": sum(durations) / len(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
        }

    job_stats1 = calc_job_stats(jobs1)
    job_stats2 = calc_job_stats(jobs2)

    comparison = {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "resource_allocation": {
            "app1": {
                "cores_granted": app1.cores_granted,
                "max_cores": app1.max_cores,
                "cores_per_executor": app1.cores_per_executor,
                "memory_per_executor_mb": app1.memory_per_executor_mb,
            },
            "app2": {
                "cores_granted": app2.cores_granted,
                "max_cores": app2.max_cores,
                "cores_per_executor": app2.cores_per_executor,
                "memory_per_executor_mb": app2.memory_per_executor_mb,
            },
        },
        "executor_metrics": {
            "app1": exec_summary1,
            "app2": exec_summary2,
            "comparison": {
                "executor_count_ratio": exec_summary2["total_executors"]
                / max(exec_summary1["total_executors"], 1),
                "task_completion_ratio": exec_summary2["completed_tasks"]
                / max(exec_summary1["completed_tasks"], 1),
                "gc_time_ratio": exec_summary2["total_gc_time"]
                / max(exec_summary1["total_gc_time"], 1),
            },
        },
        "job_performance": {
            "app1": job_stats1,
            "app2": job_stats2,
            "comparison": {
                "job_count_ratio": job_stats2["count"] / max(job_stats1["count"], 1),
                "avg_duration_ratio": job_stats2["avg_duration"]
                / job_stats1["avg_duration"]
                if job_stats1["avg_duration"] > 0
                else None,
                "total_duration_ratio": job_stats2["total_duration"]
                / job_stats1["total_duration"]
                if job_stats1["total_duration"] > 0
                else None,
            },
        },
    }

    # Standalone-only fields; all null on YARN/Dataproc -- don't spend tokens.
    if not any(v for side in comparison["resource_allocation"].values() for v in side.values()):
        del comparison["resource_allocation"]
    return comparison


def _resolve_longest_sql_id(client, app_id: str, execution_id: Optional[int]) -> int:
    """Resolve a SQL execution id, defaulting to the longest-running execution.

    Must scan every execution, not just the server's default first page of
    20 (see ``_fetch_all_sql_executions``) -- otherwise "longest running"
    silently degrades to "longest running among the first 20."
    """
    if execution_id is not None:
        return execution_id
    sql_list = _fetch_all_sql_executions(client, app_id)
    # Orphaned RUNNING executions in completed apps report a duration that
    # keeps growing with the wall clock; prefer finished ones.
    finished = [e for e in sql_list if (e.status or "").upper() != "RUNNING"]
    if finished or sql_list:
        return max(finished or sql_list, key=lambda x: x.duration or 0).id
    raise ValueError(f"No SQL executions found in application {app_id}")


def _compare_sql_plans(
    client1,
    app_id1: str,
    exec_id1: int,
    client2,
    app_id2: str,
    exec_id2: int,
) -> SqlPlanComparison:
    """Build a plan-structure diff between two (already resolved) SQL executions."""
    exec1 = client1.get_sql_execution(
        app_id1, exec_id1, details=True, plan_description=False
    )
    exec2 = client2.get_sql_execution(
        app_id2, exec_id2, details=True, plan_description=False
    )

    nodes1 = _count_node_types(exec1.nodes)
    nodes2 = _count_node_types(exec2.nodes)

    diffs = [
        SqlNodeTypeDiff(
            node_type=node_type,
            a=nodes1.get(node_type, 0),
            b=nodes2.get(node_type, 0),
        )
        for node_type in sorted(set(nodes1) | set(nodes2))
        if nodes1.get(node_type, 0) != nodes2.get(node_type, 0)
    ]

    return SqlPlanComparison(
        app_a=app_id1,
        app_b=app_id2,
        exec_id_a=exec_id1,
        exec_id_b=exec_id2,
        node_count_a=len(exec1.nodes or []),
        node_count_b=len(exec2.nodes or []),
        edge_count_a=len(exec1.edges or []),
        edge_count_b=len(exec2.edges or []),
        node_type_diffs=diffs,
    )


def truncate_plan_description(plan_desc: str, max_length: int) -> str:
    """
    Truncate plan description while preserving structure.

    Args:
        plan_desc: The plan description to truncate
        max_length: Maximum length in characters

    Returns:
        Truncated plan description with indicator if truncated
    """
    if not plan_desc or len(plan_desc) <= max_length:
        return plan_desc

    # Try to truncate at a logical boundary (end of a line)
    truncated = plan_desc[:max_length]
    last_newline = truncated.rfind("\n")

    # If we can preserve most content by truncating at newline, do so
    if last_newline > max_length * 0.8:
        truncated = truncated[:last_newline]

    return truncated + "\n... [truncated]"


def _sql_execution_summary(e: SQLExecution) -> SqlExecutionSummary:
    """Build the curated header summary for a SQL execution."""
    return SqlExecutionSummary(
        id=e.id,
        status=e.status,
        description=e.description,
        submission_time=e.submission_time,
        duration=e.duration,
        success_job_ids=e.success_job_ids or [],
        failed_job_ids=e.failed_job_ids or [],
        running_job_ids=e.running_job_ids or [],
    )


def _sql_job_summary(j: Job) -> SqlJobSummary:
    """Build a curated job row associated with a SQL execution."""
    return SqlJobSummary(
        job_id=j.job_id,
        status=j.status,
        description=j.description or j.name,
        duration=_duration_ms(j.submission_time, j.completion_time),
        num_tasks=j.num_tasks,
        num_failed_tasks=j.num_failed_tasks,
        stage_ids=j.stage_ids or [],
    )


def _sql_stage_summary(s: StageData) -> SqlStageSummary:
    """Build a curated stage row associated with a SQL execution."""
    return SqlStageSummary(
        stage_id=s.stage_id,
        attempt_id=s.attempt_id,
        status=s.status,
        description=s.description or s.name,
        num_tasks=s.num_tasks,
        num_failed_tasks=s.num_failed_tasks,
        duration=_duration_ms(s.submission_time, s.completion_time),
        input_bytes=s.input_bytes,
        shuffle_read_bytes=s.shuffle_read_bytes,
        shuffle_write_bytes=s.shuffle_write_bytes,
    )


def _build_node_metrics(nodes) -> List[SqlNodeMetrics]:
    """Build curated per-node plan metrics, skipping nodes without metrics."""
    result: List[SqlNodeMetrics] = []
    for n in nodes or []:
        if not n.metrics:
            continue
        metrics: Dict[str, str] = {}
        for m in n.metrics:
            # Collapse internal whitespace in values.
            metrics[m.name] = " ".join((m.value or "").split())
        if metrics:
            result.append(
                SqlNodeMetrics(
                    node_id=n.node_id, node_name=n.node_name, metrics=metrics
                )
            )
    return result


def _fetch_all_sql_executions(
    client,
    app_id: str,
    app_attempt_id: Optional[str] = None,
    page_size: int = 100,
) -> List[SQLExecution]:
    """Fetch every SQL execution for an app, paginating past the server's
    default page size.

    ``SparkRestClient.get_sql_list`` (and the raw History Server ``/sql``
    endpoint it wraps) defaults to returning only the first 20 executions
    when called without an explicit ``length``. Any caller that needs to
    reason about "all executions" -- not just "the first page" -- must call
    this instead of ``client.get_sql_list(...)`` directly, or it will
    silently miss executions past index 20 (e.g. picking the wrong "longest
    running" execution because the real longest one was execution #25).
    """
    all_executions: List[SQLExecution] = []
    offset = 0
    while True:
        page: List[SQLExecution] = client.get_sql_list(
            app_id=app_id,
            app_attempt_id=app_attempt_id,
            details=False,
            plan_description=False,
            offset=offset,
            length=page_size,
        )
        if not page:
            break
        all_executions.extend(page)
        offset += page_size
        if len(page) < page_size:
            break
    return all_executions


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def list_sql_executions(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    status: Optional[str] = None,
    description: Optional[str] = None,
    sort_by: Optional[str] = None,
    limit: int = 20,
    page_size: int = 100,
    *,
    ctx: Context,
) -> List[SqlExecutionSummary]:
    """
    List SQL executions for a Spark application as curated summaries.

    Returns a lightweight, summarized row per SQL execution (id, status,
    description, duration, and associated job IDs) without plan text or
    node-level details. Use ``get_sql_execution`` for a deep dive into a single
    execution.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID
        status: Optional status filter (COMPLETED|RUNNING|FAILED)
        description: Optional case-insensitive substring filter on the description
        sort_by: Optional sort field (duration|id). Defaults to failed-first then
            longest duration.
        limit: Maximum number of executions to return (default: 20; 0 returns all)
        page_size: Number of executions to fetch per page from the server (default: 100)

    Returns:
        List of SqlExecutionSummary objects
    """
    client = get_client_or_default(ctx, server, app_id)

    if limit < 0:
        raise ValueError("limit must be non-negative")

    all_executions = _fetch_all_sql_executions(
        client, app_id, app_attempt_id=app_attempt_id, page_size=page_size
    )

    if description:
        needle = description.lower()
        all_executions = [
            e for e in all_executions if needle in (e.description or "").lower()
        ]
    if status:
        wanted = status.upper()
        all_executions = [e for e in all_executions if (e.status or "") == wanted]

    all_executions = _sort_sql_executions(all_executions, sort_by)
    if limit:
        all_executions = all_executions[:limit]

    return [_sql_execution_summary(e) for e in all_executions]


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_sql_execution(
    app_id: str,
    execution_id: int,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    include_plan: Optional[bool] = None,
    include_initial_plan: bool = False,
    include_aggregated_metrics: bool = False,
    include_stages: bool = False,
    plan_max_length: Optional[int] = None,
    *,
    ctx: Context,
) -> SqlExecutionDetail:
    """
    Get details about a specific SQL execution.

    By default returns only the curated header (status, duration, associated
    job IDs) to keep the response small. Additional sections are opt-in:

    * ``include_plan``: physical plan text plus per-node metrics. AQE "Initial
      Plan" sections are stripped unless ``include_initial_plan`` is set.
    * ``include_initial_plan``: keep AQE initial plans (implies ``include_plan``).
    * ``include_aggregated_metrics``: associated jobs plus aggregated stage metrics
      (tasks, input, shuffle, spill, GC) scoped to this execution.
    * ``include_stages``: the individual stages for this execution.

    Args:
        app_id: The Spark application ID
        execution_id: The SQL execution ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID
        include_plan: Include the plan text and node metrics. If unset, falls back
            to the server's ``include_plan_description`` config (default False).
        include_initial_plan: Include AQE initial plans (implies include_plan)
        include_aggregated_metrics: Include associated jobs and aggregated stage metrics
        include_stages: Include the individual stages for this execution
        plan_max_length: Optional max character length for the plan text

    Returns:
        SqlExecutionDetail with the header and any requested sections
    """
    client = get_client_or_default(ctx, server, app_id)

    # Resolve whether to include the plan. Explicit arg wins; otherwise fall
    # back to the server's include_plan_description config (default False).
    if include_initial_plan:
        include_plan = True
    elif include_plan is None:
        config_value = getattr(client.config, "include_plan_description", None)
        include_plan = bool(config_value) if config_value is not None else False

    # Node-level metrics require details=True; only fetch them when needed.
    execution = client.get_sql_execution(
        app_id=app_id,
        execution_id=execution_id,
        app_attempt_id=app_attempt_id,
        details=include_plan,
        plan_description=include_plan,
    )

    detail = SqlExecutionDetail(execution=_sql_execution_summary(execution))

    if include_plan:
        plan = execution.plan_description or ""
        if not include_initial_plan:
            plan = _strip_initial_plans(plan)
        if plan_max_length is not None:
            plan = truncate_plan_description(plan, plan_max_length)
        detail.plan_description = plan
        detail.node_metrics = _build_node_metrics(execution.nodes)

    if include_aggregated_metrics or include_stages:
        job_ids = set(_collect_sql_job_ids(execution))
        jobs: List[Job] = []
        stage_list: List[StageData] = []
        if job_ids:
            all_jobs = client.list_jobs(app_id=app_id, app_attempt_id=app_attempt_id)
            jobs = _default_sort_jobs([j for j in all_jobs if j.job_id in job_ids])
            stage_ids = _stage_ids_from_jobs(jobs)
            if stage_ids:
                all_stages = client.list_stages(
                    app_id=app_id, app_attempt_id=app_attempt_id
                )
                seen: set = set()
                scoped: List[StageData] = []
                for s in all_stages:
                    if s.stage_id in stage_ids and s.stage_id not in seen:
                        seen.add(s.stage_id)
                        scoped.append(s)
                stage_list = _default_sort_stages(scoped)

        detail.jobs = [_sql_job_summary(j) for j in jobs]
        if include_aggregated_metrics:
            detail.stage_metrics = _aggregate_stages(stage_list, None)
        if include_stages:
            detail.stages = [_sql_stage_summary(s) for s in stage_list]

    return detail


def _jobs_and_stage_agg_for_executions(
    client, app_id: str, executions: List[SQLExecution]
) -> tuple:
    """Union the jobs (and their stages) across one or more SQL executions.

    A single logical operation can be split across sibling SQL execution IDs
    by the planner -- e.g. a DataSourceV2 ``SaveIntoDataSourceCommand``
    wrapper execution with zero jobs of its own, whose actual work runs
    under a second, immediately-following execution ID. Passing every
    execution in the group here aggregates their jobs/stages together,
    rather than silently reporting zero stage metrics for the wrapper.
    """
    job_ids: set = set()
    for execution in executions:
        job_ids |= set(_collect_sql_job_ids(execution))
    all_jobs = client.list_jobs(app_id=app_id)
    jobs = [j for j in all_jobs if j.job_id in job_ids]
    stage_ids = _stage_ids_from_jobs(jobs)
    all_stages = client.list_stages(app_id=app_id)
    agg = _aggregate_stages(all_stages, stage_ids)
    return jobs, stage_ids, agg, all_stages


def _collect_sql_compare_side(
    client, app_id: str, execution_id: int, extra_executions: Optional[List[SQLExecution]] = None
) -> SqlCompareSide:
    """Build one side of a SQL-execution comparison.

    ``extra_executions`` lets callers fold sibling executions' jobs/stages
    into the aggregate (see ``_jobs_and_stage_agg_for_executions``) while
    still reporting ``execution_id``'s own duration/description/status as
    the side's headline figure.
    """
    execution = client.get_sql_execution(
        app_id, execution_id, details=False, plan_description=False
    )
    jobs, _stage_ids, agg, _all_stages = _jobs_and_stage_agg_for_executions(
        client, app_id, [execution, *(extra_executions or [])]
    )

    return SqlCompareSide(
        app=app_id,
        sql_id=execution.id,
        description=execution.description,
        status=execution.status,
        duration=execution.duration,
        jobs=len(jobs),
        stages=agg.stage_count,
        tasks=agg.tasks,
        stage_time=agg.duration,
        input_bytes=agg.input_bytes,
        shuffle_read_bytes=agg.shuffle_read_bytes,
        shuffle_write_bytes=agg.shuffle_write_bytes,
        disk_bytes_spilled=agg.disk_bytes_spilled,
        jvm_gc_time=agg.jvm_gc_time,
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def compare_sql_executions(
    app_id1: str,
    app_id2: str,
    execution_id1: Optional[int] = None,
    execution_id2: Optional[int] = None,
    server: Optional[str] = None,
    server2: Optional[str] = None,
    include_plan_diff: bool = False,
    *,
    ctx: Context,
) -> SqlExecutionComparison:
    """
    Compare performance metrics between two SQL executions.

    For each execution, aggregates the metrics of the stages belonging to that
    query (jobs, stages, tasks, stage time, input, shuffle read/write, disk
    spill, GC time) so the two runs can be compared side by side.

    Set ``include_plan_diff`` to also attach a ``plan_comparison`` section with
    the plan-structure diff (per-side node/edge counts and the node types whose
    counts differ).

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        execution_id1: Execution ID for the first app (uses longest-running if omitted)
        execution_id2: Execution ID for the second app (uses longest-running if omitted)
        server: Optional server name to use (uses default if not specified)
        server2: Optional server for app_id2 when it lives on a different
            History Server (e.g. deb11 vs deb12); defaults to server
        include_plan_diff: Also compare the SQL plan structure (default False)

    Returns:
        SqlExecutionComparison with an ``a`` and ``b`` side, plus an optional
        ``plan_comparison`` when ``include_plan_diff`` is set
    """
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server2 or server, app_id2)

    execution_id1 = _resolve_longest_sql_id(client1, app_id1, execution_id1)
    execution_id2 = _resolve_longest_sql_id(client2, app_id2, execution_id2)

    comparison = SqlExecutionComparison(
        a=_collect_sql_compare_side(client1, app_id1, execution_id1),
        b=_collect_sql_compare_side(client2, app_id2, execution_id2),
    )

    if include_plan_diff:
        comparison.plan_comparison = _compare_sql_plans(
            client1, app_id1, execution_id1, client2, app_id2, execution_id2
        )

    return comparison


# Quantile points fetched for stage task-metric distributions: p25/p50/p75/max.
_STAGE_COMPARE_QUANTILES = "0.25,0.5,0.75,1.0"


def _build_stage_task_quantiles(summary) -> Optional[StageTaskQuantiles]:
    """Map a ``TaskMetricsSummary`` into the curated stage quantiles model."""
    if summary is None:
        return None

    def nested(obj, attr):
        return getattr(obj, attr) if obj is not None else None

    return StageTaskQuantiles(
        quantiles=summary.quantiles or [],
        duration=summary.duration,
        gc_time=summary.jvm_gc_time,
        scheduler_delay=summary.scheduler_delay,
        peak_execution_memory=summary.peak_execution_memory,
        input_bytes=nested(summary.input_metrics, "bytes_read"),
        output_bytes=nested(summary.output_metrics, "bytes_written"),
        shuffle_read_bytes=nested(summary.shuffle_read_metrics, "read_bytes"),
        shuffle_write_bytes=nested(summary.shuffle_write_metrics, "write_bytes"),
        disk_bytes_spilled=summary.disk_bytes_spilled,
        memory_bytes_spilled=summary.memory_bytes_spilled,
    )


def _collect_stage_side(client, app_id: str, stage_id: int) -> StageCompareSide:
    """Build one side of a stage comparison from the latest stage attempt."""
    attempts = client.list_stage_attempts(
        app_id=app_id,
        stage_id=stage_id,
        details=False,
        with_summaries=False,
    )
    if not attempts:
        raise ValueError(
            f"No stage found with ID {stage_id} in application {app_id} "
            "(it may have been skipped by AQE)"
        )

    stage = max(attempts, key=lambda s: s.attempt_id or 0)
    attempt_id = stage.attempt_id or 0

    # Task-metric quantiles are best-effort; a missing summary just omits them.
    try:
        summary = client.get_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            quantiles=_STAGE_COMPARE_QUANTILES,
        )
    except Exception:
        summary = None

    return StageCompareSide(
        app=app_id,
        stage_id=stage.stage_id,
        attempt_id=attempt_id,
        status=stage.status,
        description=stage.description or stage.name,
        duration=_duration_ms(stage.submission_time, stage.completion_time),
        tasks=stage.num_tasks or 0,
        failed_tasks=stage.num_failed_tasks or 0,
        input_bytes=stage.input_bytes or 0,
        output_bytes=stage.output_bytes or 0,
        shuffle_read_bytes=stage.shuffle_read_bytes or 0,
        shuffle_write_bytes=stage.shuffle_write_bytes or 0,
        disk_bytes_spilled=stage.disk_bytes_spilled or 0,
        memory_bytes_spilled=stage.memory_bytes_spilled or 0,
        jvm_gc_time=stage.jvm_gc_time or 0,
        task_quantiles=_build_stage_task_quantiles(summary),
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def compare_stages(
    app_id1: str,
    stage_id1: int,
    app_id2: str,
    stage_id2: int,
    server: Optional[str] = None,
    server2: Optional[str] = None,
    *,
    ctx: Context,
) -> StageComparison:
    """
    Compare two stages side by side, optionally across applications.

    For each stage the latest attempt is used. Returns stage-level metrics
    (duration, task counts, input/output, shuffle read/write, disk/memory spill,
    GC time) plus per-task metric distributions at the p25/p50/p75/max quantiles
    (duration, GC time, scheduler delay, peak execution memory, input, output,
    shuffle read/write, disk spill, memory spill).

    The two stages may belong to different applications. Deltas are not
    precomputed; compare the ``a`` and ``b`` sides directly.

    Args:
        app_id1: Application ID for the first stage
        stage_id1: Stage ID within the first application
        app_id2: Application ID for the second stage
        stage_id2: Stage ID within the second application
        server: Optional server name to use (uses default if not specified)
        server2: Optional server for app_id2 when it lives on a different
            History Server (e.g. deb11 vs deb12); defaults to server

    Returns:
        StageComparison with an ``a`` and ``b`` side
    """
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server2 or server, app_id2)

    return StageComparison(
        a=_collect_stage_side(client1, app_id1, stage_id1),
        b=_collect_stage_side(client2, app_id2, stage_id2),
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_native_engine_metrics(
    app_id: str,
    server: Optional[str] = None,
    execution_id: Optional[int] = None,
    top_n: int = 5,
    *,
    ctx: Context,
) -> NativeEngineMetrics:
    """
    Get Gluten/Velox native-engine metrics for a Spark application.

    Detects whether the application ran on a native SQL engine (Gluten,
    typically backed by Velox) via ``spark.plugins``/``spark.gluten.*``
    config, then surfaces the per-operator metrics reported by the native
    "*Transformer" plan nodes -- these carry native execution details (e.g.
    spill, peak memory, wall time) that never reach the standard JVM
    stage/task metrics used elsewhere in this server.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        execution_id: Optional SQL execution ID to scope to. When omitted,
            the ``top_n`` slowest SQL executions are scanned instead.
        top_n: Number of slowest SQL executions to scan when execution_id is
            not given (default: 5)

    Returns:
        NativeEngineMetrics with engine detection, per-operator native
        metrics, and any non-zero spill metrics found on native operators
    """
    client = get_client_or_default(ctx, server, app_id)

    engine_info = _detect_native_engine(client.get_environment(app_id=app_id))

    if execution_id is not None:
        sql_ids = [execution_id]
    else:
        sql_ids = [
            s.id
            for s in list_sql_executions(
                app_id, server=server, sort_by="duration", limit=top_n,
                ctx=ctx,
            )
            if s.id is not None
        ]

    operators: List[NativeOperatorMetrics] = []
    spill_hits: List[NativeEngineSpillHit] = []
    for sql_id in sql_ids:
        execution = client.get_sql_execution(
            app_id=app_id, execution_id=sql_id, details=True, plan_description=False
        )
        operators.extend(_build_native_operator_metrics(sql_id, execution.nodes))
        spill_hits.extend(_find_native_spill_hits(sql_id, execution.nodes))

    return NativeEngineMetrics(
        application_id=app_id,
        engine=engine_info,
        operators=operators,
        spill_hits=spill_hits,
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_job_bottlenecks(
    app_id: str, server: Optional[str] = None, top_n: int = 5, *, ctx: Context
) -> Dict[str, Any]:
    """
    Identify performance bottlenecks in a Spark job.

    Analyzes stages, tasks, and executors to find the most time-consuming
    operations and resource-intensive components. Also inspects scan
    operators in the slowest SQL executions (native or vanilla, BigQuery or
    file-based) for BigQuery scan counts and file-based scans that evaluated
    filters without pushing them down to the source.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top bottlenecks to return

    Returns:
        Dictionary containing identified bottlenecks and recommendations
    """
    client = get_client_or_default(ctx, server, app_id)

    all_stages = client.list_stages(app_id=app_id)
    slowest_stages = _sort_stages(list(all_stages), "duration")[:top_n]

    slowest_jobs = list_jobs(
        app_id, server=server, sort_by="duration", length=top_n, ctx=ctx
    )

    exec_summary = get_executor_summary(app_id, server, ctx=ctx)

    # Identify stages with high spill
    high_spill_stages = []
    for stage in all_stages:
        if (
            stage.memory_bytes_spilled
            and stage.memory_bytes_spilled > 100 * 1024 * 1024
        ):  # > 100MB
            high_spill_stages.append(
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "memory_spilled_mb": stage.memory_bytes_spilled / (1024 * 1024),
                    "disk_spilled_mb": stage.disk_bytes_spilled / (1024 * 1024)
                    if stage.disk_bytes_spilled
                    else 0,
                }
            )

    high_spill_stages = heapq.nlargest(
        len(high_spill_stages), high_spill_stages, key=lambda x: x["memory_spilled_mb"]
    )

    # Identify GC pressure
    gc_pressure = (
        exec_summary["total_gc_time"] / max(exec_summary["total_duration"], 1)
        if exec_summary["total_duration"] > 0
        else 0
    )

    # Detect a native SQL engine (Gluten/Velox). When active, most operators
    # run outside the JVM as "*Transformer" nodes, so the standard
    # memory_bytes_spilled/GC-time signals above miss native spill and
    # understate native memory pressure. Native spill only shows up in the
    # SQL plan node metrics, so pull it from the slowest SQL executions.
    try:
        engine_info = _detect_native_engine(
            client.get_environment(app_id=app_id)
        )
    except Exception:
        engine_info = NativeEngineInfo()

    # Lazy import: tools.py <-> scan_nodes.py would otherwise be a circular
    # module-load dependency (scan_nodes.py imports several helpers from
    # this module).
    from spark_history_mcp.tools.scan_nodes import collect_scan_nodes

    native_spill_hits: List[NativeEngineSpillHit] = []
    scan_nodes_found: List[Any] = []
    for sql_summary in list_sql_executions(
        app_id, server=server, sort_by="duration", limit=top_n,
        ctx=ctx,
    ):
        if sql_summary.id is None:
            continue
        execution = client.get_sql_execution(
            app_id=app_id,
            execution_id=sql_summary.id,
            details=True,
            plan_description=True,
        )
        if engine_info.is_native:
            native_spill_hits.extend(
                _find_native_spill_hits(sql_summary.id, execution.nodes)
            )
        scan_nodes_found.extend(
            collect_scan_nodes(
                sql_summary.id, execution.nodes, execution.plan_description or ""
            )
        )

    # File-based scans (Parquet/ORC/etc, native or vanilla) where Spark
    # evaluated filters (DataFilters) but none were pushed down to the
    # source (PushedFilters empty) -- a missed pushdown opportunity. Not
    # meaningful for BigQuery scans, which never report pushdown info in
    # the plan text regardless of whether it happened.
    missed_pushdown_scans = [
        n
        for n in scan_nodes_found
        if n.source_kind == "file" and n.data_filters and not n.pushed_filters
    ]

    bottlenecks = {
        "application_id": app_id,
        "performance_bottlenecks": {
            "slowest_stages": [
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "duration_seconds": _duration_seconds(
                        stage.submission_time, stage.completion_time
                    ),
                    "task_count": stage.num_tasks,
                    "failed_tasks": stage.num_failed_tasks,
                }
                for stage in slowest_stages[:top_n]
            ],
            "slowest_jobs": [
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "duration_seconds": _duration_seconds(
                        job.submission_time, job.completion_time
                    ),
                    "failed_tasks": job.num_failed_tasks,
                    "status": job.status,
                }
                for job in slowest_jobs[:top_n]
            ],
        },
        "resource_bottlenecks": {
            "memory_spill_stages": high_spill_stages[:top_n],
            "gc_pressure_ratio": gc_pressure,
        },
        "native_engine": {
            "engine": engine_info.engine,
            "backend": engine_info.backend,
            "is_native": engine_info.is_native,
            "spill_hits": [
                hit.model_dump(by_alias=True) for hit in native_spill_hits[: top_n * 5]
            ],
        },
        "scan_bottlenecks": {
            "scan_count": len(scan_nodes_found),
            "bigquery_scan_count": sum(
                1 for n in scan_nodes_found if n.source_kind == "bigquery"
            ),
            "missed_pushdown_scans": [
                n.model_dump(by_alias=True) for n in missed_pushdown_scans[:top_n]
            ],
        },
        "recommendations": [],
    }

    # Generate recommendations
    if gc_pressure > 0.1:  # More than 10% time in GC
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"High GC pressure ({gc_pressure:.1%})",
                "suggestion": (
                    "Consider increasing executor memory or reducing memory usage"
                    if not engine_info.is_native
                    else (
                        "JVM GC pressure is a weak signal under the "
                        f"{engine_info.engine}/{engine_info.backend or 'native'} "
                        "engine since most execution happens off-heap; check "
                        "native_engine.spill_hits and executor off-heap memory too"
                    )
                ),
            }
        )

    if high_spill_stages:
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"Memory spilling detected in {len(high_spill_stages)} stages",
                "suggestion": "Increase executor memory or optimize data partitioning",
            }
        )

    if native_spill_hits:
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": (
                    f"Native ({engine_info.engine}/{engine_info.backend or 'native'}) "
                    f"spill detected in {len(native_spill_hits)} operator metric(s) "
                    "not reflected in the standard stage spill fields above"
                ),
                "suggestion": (
                    "Increase spark.memory.offHeap.size / off-heap memory available "
                    "to the native engine, or reduce per-task data volume "
                    "(e.g. more partitions) to avoid native spilling"
                ),
            }
        )

    if exec_summary["failed_tasks"] > 0:
        bottlenecks["recommendations"].append(
            {
                "type": "reliability",
                "priority": "medium",
                "issue": f"{exec_summary['failed_tasks']} failed tasks",
                "suggestion": "Investigate task failures and consider increasing task retry settings",
            }
        )

    if missed_pushdown_scans:
        tables = sorted({n.table for n in missed_pushdown_scans if n.table})
        bottlenecks["recommendations"].append(
            {
                "type": "io",
                "priority": "medium",
                "issue": (
                    f"{len(missed_pushdown_scans)} file scan(s) evaluated filters "
                    f"that were not pushed down to the source: {', '.join(tables) or 'unknown table(s)'}"
                ),
                "suggestion": (
                    "Check predicate types/expressions on these scans -- some "
                    "aren't supported for pushdown by this source or engine, so "
                    "more data than necessary is read before filtering"
                ),
            }
        )

    return bottlenecks


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_resource_usage_timeline(
    app_id: str, server: Optional[str] = None, *, ctx: Context
) -> Dict[str, Any]:
    """
    Get resource usage timeline for a Spark application.

    Provides a chronological view of resource allocation and usage patterns
    including executor additions/removals and stage execution overlap.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing timeline of resource usage
    """
    client = get_client_or_default(ctx, server, app_id)

    # Get application info
    app = client.get_application(app_id)

    # Get all executors
    executors = client.list_all_executors(app_id=app_id)

    # Get stages
    stages = client.list_stages(app_id=app_id)

    # Create timeline events
    timeline_events = []

    # Add executor events
    for executor in executors:
        if executor.add_time:
            timeline_events.append(
                {
                    "timestamp": _parse_spark_datetime(executor.add_time),
                    "type": "executor_add",
                    "executor_id": executor.id,
                    "cores": executor.total_cores,
                    "memory_mb": executor.max_memory / (1024 * 1024)
                    if executor.max_memory
                    else 0,
                }
            )

        if executor.remove_time:
            timeline_events.append(
                {
                    "timestamp": _parse_spark_datetime(executor.remove_time),
                    "type": "executor_remove",
                    "executor_id": executor.id,
                    "reason": executor.remove_reason,
                }
            )

    # Add stage events
    for stage in stages:
        if stage.submission_time:
            timeline_events.append(
                {
                    "timestamp": _parse_spark_datetime(stage.submission_time),
                    "type": "stage_start",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "task_count": stage.num_tasks,
                }
            )

        if stage.completion_time:
            timeline_events.append(
                {
                    "timestamp": _parse_spark_datetime(stage.completion_time),
                    "type": "stage_end",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "status": stage.status,
                    "duration_seconds": _duration_seconds(
                        stage.submission_time, stage.completion_time
                    ),
                }
            )

    timeline_events.sort(key=lambda x: (x["timestamp"] is None, x["timestamp"] or 0))

    # Track live executors by id so removals give back their cores/memory.
    live: Dict[str, tuple] = {}
    running_stages = peak_stages = 0
    peak_executors = peak_cores = 0
    executor_timeline = []
    for event in timeline_events:
        kind = event["type"]
        if kind == "executor_add":
            live[event["executor_id"]] = (event["cores"] or 0, event["memory_mb"] or 0)
        elif kind == "executor_remove":
            live.pop(event["executor_id"], None)
        elif kind == "stage_start":
            running_stages += 1
            peak_stages = max(peak_stages, running_stages)
            continue
        else:
            running_stages = max(0, running_stages - 1)
            continue
        cores = sum(c for c, _ in live.values())
        peak_executors = max(peak_executors, len(live))
        peak_cores = max(peak_cores, cores)
        executor_timeline.append(
            {
                "t": str(event["timestamp"]),
                "event": kind,
                "executor_id": event["executor_id"],
                "live_executors": len(live),
                "live_cores": cores,
                **({"reason": event["reason"]} if event.get("reason") else {}),
            }
        )

    max_rows = 200
    return {
        "application_id": app_id,
        "application_name": app.name,
        "summary": {
            "total_events": len(timeline_events),
            "executor_additions": sum(e["type"] == "executor_add" for e in timeline_events),
            "executor_removals": sum(e["type"] == "executor_remove" for e in timeline_events),
            "stage_executions": sum(e["type"] == "stage_start" for e in timeline_events),
            "peak_executors": peak_executors,
            "peak_cores": peak_cores,
            "peak_concurrent_stages": peak_stages,
        },
        # Executor add/remove only (stage events are summarized above);
        # capped so dynamic-allocation apps don't flood the context.
        "executor_timeline": executor_timeline[:max_rows],
        **(
            {"executor_timeline_truncated": len(executor_timeline) - max_rows}
            if len(executor_timeline) > max_rows
            else {}
        ),
    }
