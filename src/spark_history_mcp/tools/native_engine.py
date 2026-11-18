"""Gluten/Velox native-engine introspection.

Full Query Fallback (FQF) cost decisions, per-operator fallback reasons, and
native-planning overhead are emitted as custom Gluten/DataPelago listener
events (``org.apache.gluten.events.*``). Spark History Server's REST API only
parses its own built-in listener event types, so none of that ever reaches
the REST endpoints the rest of this server uses -- it's only visible by
reading the raw event log directly (see ``SparkRestClient.download_event_log``).
Event type names and fields here were verified against a real Gluten/DPAS
event log, not guessed from documentation.

Bloom filter producer/consumer ownership is different: it's derived purely
from a SQL execution's physical plan text, which the standard REST API
already returns, so that detection doesn't touch the raw event log at all.
The plan-text parsing below ports the same regex-based classification used
by the project's own Gluten Bloom-filter ownership QA tooling (dopant's
core/bloomfilter/validator.py), adapted to report ambiguity via a `note`
instead of raising.

Gotchas discovered the hard way, so a later session doesn't have to re-pay
the cost of re-discovering them:

* Prefer get_native_engine_cost_summary/get_gluten_build_info/
  diagnose_sql_execution_failures (the /dpas//gluten/ UI-tab-based tools
  below) over the raw-event-log tools above (get_native_engine_events,
  get_query_fallback_summary, diagnose_native_execution,
  get_native_planning_overhead) whenever the app is reachable through a
  LIVE History Server. One small HTML page fetch beats downloading and
  decompressing a potentially large raw event log, and skips having to
  guess the log's compression framing (gzip/zstd/lz4/snappy -- Spark's
  Snappy framing in particular is NOT the standalone format `snappy.
  decompress()` expects; see `_xerial_snappy_decompress`). Fall back to the
  raw-event-log tools only for what they alone have: native-validation-memo
  stats, per-operator DPDecisionOverheadEvent overhead, and the
  executed-plan-native-node cross-check.

* /dpas/ vs /gluten/: NOT symmetric. /dpas/'s per-query table is a strict
  superset of /gluten/'s (same fallback summary/physical plan, PLUS
  Planning Time Breakdown/Smart Acceleration Summary/DPA-S+Vanilla plan
  cost trees that /gluten/ lacks entirely). /gluten/'s only unique content
  is the "Gluten Build Information" table (Velox revision, DP Product
  ID/Name/Version, Spark/Scala/Java versions) -- that's the ONLY reason
  get_gluten_build_info fetches /gluten/ instead of /dpas/. There is no
  /api/v1 route for either (/api/v1/applications/{id}/dpas and /gluten both
  404), BUT Spark serves every UI page's renderJson at "<page>/json/":
  /history/{id}/gluten/json/ returns build info + per-execution node counts
  and fallback reasons as JSON (verified live on deb11/deb12, 2026-11-08) --
  get_gluten_fallback_reasons and get_gluten_build_info use it first. /dpas/
  has no renderJson, so its cost/overhead data is still HTML-only. Neither
  supports server-side ?id= filtering -- filter client-side.

* Raw event logs are cached in memory per (server, app, attempt) once the
  attempt is completed (download_event_log_cached), so repeated raw-log
  tool calls on one app download it once.

* SparkRestClient.get_sql_list (and the raw History Server /sql endpoint it
  wraps) defaults to length=20. A real query execution can sit well past
  that (id 25 of 26, in a case that cost real back-and-forth to track down
  this session) -- silently missing it, not erroring. Never call
  client.get_sql_list(...) directly when you need "every execution"; use
  tools._fetch_all_sql_executions (paginates) or the list_sql_executions
  tool (already paginates AND sorts failed-first-then-longest-duration by
  default, so the query you actually want is usually right at the top of
  even a small `limit`).

* A LIVE History Server reached through a `kubectl port-forward` tunnel
  (as opposed to one actually running as a local process) can sit on a
  port that looks arbitrary -- e.g. this deployment's tunnel answers on
  :18081, not the more conventional :18080. `curl localhost:18080` failing
  there looks exactly like "the server isn't running," when it's actually
  just the wrong port for this particular setup. Check
  ~/.claude.json's spark-history server entries' actual URLs (there may be
  more than one server configured, and only the ``"default": true`` one
  matters for tool calls that omit ``server=``) before assuming a
  connection failure means the server process itself is down.

* When a totally vanilla-Spark app (Gluten plugin never loaded) is queried
  by any tool in this module, that must surface as an explicit, first-class
  "no native-engine data at all" signal (see
  ``SqlExecutionDiagnosis.native_engine_available`` and its docstring) --
  never as a bare ``None``/omitted field a caller could misread as "present
  and clean." A missing native-engine summary is genuinely ambiguous
  between three different situations (no Gluten at all; Gluten active but
  this specific query never triggered a cost decision; execution_id simply
  doesn't exist) unless something else disambiguates it first.
"""

import collections
import html as _html
import math
import os
import re
import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations

from spark_history_mcp.core.app import tool
from spark_history_mcp.models.mcp_types import (
    BloomFilterOwnership,
    FailedStageDiagnosis,
    GlutenBuildInfo,
    GlutenExecutionFallback,
    GlutenFallbackReasons,
    NativeEngineEvent,
    NativeEngineUiPlanCostNode,
    NativeEngineUiPlanningOverhead,
    NativeEngineUiQuerySummary,
    NativeExecutionAppSummary,
    NativeExecutionDiagnosis,
    NativeExecutionSummary,
    NativeValidationMemoStats,
    QueryFallbackSummary,
    QueryPlanningOverhead,
    SqlExecutionDiagnosis,
)
from spark_history_mcp.tools.tools import (
    _fetch_all_sql_executions,
    get_client_or_default,
    get_sql_execution,
    list_sql_executions,
    list_stage_task_failures,
)
from spark_history_mcp.utils.event_log_codec import (  # noqa: F401 -- re-exported
    _COMPRESSION_MAGIC,
    _PART_FILE_RE,
    _XERIAL_SNAPPY_MAGIC,
    _decompress,
    _event_log_member_names,
    _lz4block_decompress,
    _xerial_snappy_decompress,
    iter_event_log_records,
)

# Event type names, exactly as emitted into the "Event" field of the raw
# Spark event log.
_NATIVE_EVENT_PREFIX = "org.apache.gluten.events."
_FALLBACK_EVENT = _NATIVE_EVENT_PREFIX + "GlutenPlanFallbackEvent"
_COST_DECISION_EVENT = _NATIVE_EVENT_PREFIX + "DPQueryCostDecisionEvent"
_DECISION_OVERHEAD_EVENT = _NATIVE_EVENT_PREFIX + "DPDecisionOverheadEvent"

# ---------------------------------------------------------------------------
# Raw event-log cache
#
# Every raw-event-log tool used to download (and re-decompress) the whole log
# on each call, so asking two questions about one app -- or diagnose_regression
# comparing N apps -- re-fetched the same bytes repeatedly. A *completed*
# attempt's log never changes, so cache its zip bytes in memory (LRU, bounded
# by total size). In-progress apps are never cached: their log is still
# growing. Override the budget with SPARK_MCP_EVENT_LOG_CACHE_MB (0 disables).
# ---------------------------------------------------------------------------

_EVENT_LOG_CACHE: "collections.OrderedDict[Tuple[str, str, Optional[str]], bytes]" = (
    collections.OrderedDict()
)
_EVENT_LOG_CACHE_LOCK = threading.Lock()


def _event_log_cache_budget() -> int:
    try:
        return int(os.environ.get("SPARK_MCP_EVENT_LOG_CACHE_MB", "1024")) * 1024 * 1024
    except ValueError:
        return 1024 * 1024 * 1024


def _attempt_completed(client, app_id: str, app_attempt_id: Optional[str]) -> bool:
    """True only when the History Server reports the attempt as completed."""
    try:
        attempts = getattr(client.get_application(app_id), "attempts", None) or []
    except Exception:
        return False
    if app_attempt_id:
        attempts = [a for a in attempts if getattr(a, "attempt_id", None) == app_attempt_id]
    return bool(attempts) and all(getattr(a, "completed", False) for a in attempts)


def download_event_log_cached(
    client, app_id: str, app_attempt_id: Optional[str] = None
) -> bytes:
    """``client.download_event_log`` with an in-memory cache for completed apps."""
    key = (client.config.url, app_id, app_attempt_id)
    with _EVENT_LOG_CACHE_LOCK:
        cached = _EVENT_LOG_CACHE.get(key)
        if cached is not None:
            _EVENT_LOG_CACHE.move_to_end(key)
            return cached
    data = client.download_event_log(app_id, app_attempt_id=app_attempt_id)
    budget = _event_log_cache_budget()
    if 0 < len(data) <= budget and _attempt_completed(client, app_id, app_attempt_id):
        with _EVENT_LOG_CACHE_LOCK:
            _EVENT_LOG_CACHE[key] = data
            _EVENT_LOG_CACHE.move_to_end(key)
            total = sum(len(v) for v in _EVENT_LOG_CACHE.values())
            while total > budget and len(_EVENT_LOG_CACHE) > 1:
                _, evicted = _EVENT_LOG_CACHE.popitem(last=False)
                total -= len(evicted)
    return data


def clear_event_log_cache() -> None:
    with _EVENT_LOG_CACHE_LOCK:
        _EVENT_LOG_CACHE.clear()

def _is_root_scope(scope: Any) -> bool:
    return (isinstance(scope, dict) and scope.get("type") == "root") or (
        "RootQuery" in str(scope)
    )


def _plan_cost(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nanos_to_ms(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) / 1_000_000.0
    except (TypeError, ValueError):
        return None


def _build_fallback_summary(
    execution_id: Any, fallback_events: List[dict], cost_events: List[dict]
) -> QueryFallbackSummary:
    merged_reasons: Dict[str, str] = {}
    any_fallback = False
    for event in fallback_events:
        # Last event wins, matching the Gluten UI plugin: earlier events are
        # superseded (re-)plans whose nodes didn't all run (see
        # get_gluten_fallback_reasons).
        merged_reasons = dict(event.get("fallbackNodeToReason") or {})
        if (event.get("numFallbackNodes") or 0) > 0:
            any_fallback = True

    root_events = [e for e in cost_events if _is_root_scope(e.get("scopeType"))]
    selected_engine = None
    vanilla_cost = gluten_cost = None
    cost_fallback_summary: List[Any] = []
    if root_events:
        latest = root_events[-1]
        vanilla_cost = _plan_cost(latest.get("vanillaPlanCost"))
        gluten_cost = _plan_cost(latest.get("glutenPlanCost"))
        cost_fallback_summary = latest.get("fallbackSummary") or []
        vc = vanilla_cost if vanilla_cost is not None else float("-inf")
        gc = gluten_cost if gluten_cost is not None else float("inf")
        selected_gluten = not cost_fallback_summary and vc >= gc
        selected_engine = "gluten" if selected_gluten else "vanilla"
        any_fallback = any_fallback or bool(cost_fallback_summary)

    return QueryFallbackSummary(
        execution_id=execution_id,
        fqf_enabled=bool(root_events),
        fell_back=any_fallback,
        selected_engine=selected_engine,
        vanilla_plan_cost=vanilla_cost,
        gluten_plan_cost=gluten_cost,
        cost_decision_fallback_summary=cost_fallback_summary,
        fallback_events=len(fallback_events),
        fallback_node_reasons=merged_reasons,
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_native_engine_events(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    event_type: Optional[str] = None,
    limit: int = 200,
    *,
    ctx: Context,
) -> List[NativeEngineEvent]:
    """
    List raw Gluten/DataPelago native-engine events from an application's
    event log.

    Spark History Server's REST API only surfaces its own built-in event
    types, so custom Gluten listener events -- FQF cost decisions, fallback
    reasons, native-planning overhead, build info, and anything else
    instrumented under the ``org.apache.gluten.events.*`` namespace -- are
    only visible by reading the raw event log. This is a low-level,
    unfiltered escape hatch for that; prefer ``get_query_fallback_summary``
    or ``get_native_planning_overhead`` for a curated view of the two most
    useful event types.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        execution_id: Optional SQL execution ID to filter to
        event_type: Optional event-type suffix to filter to (e.g.
            "DPQueryCostDecisionEvent", "GlutenPlanFallbackEvent",
            "DPDecisionOverheadEvent", "GlutenBuildInfoEvent") -- matches the
            event type after "org.apache.gluten.events."
        limit: Maximum number of events to return (default: 200)

    Returns:
        List of NativeEngineEvent, each with the event's type suffix and its
        full raw field payload
    """
    client = get_client_or_default(ctx, server, app_id)
    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)

    results: List[NativeEngineEvent] = []
    for record in iter_event_log_records(zip_bytes):
        etype = record.get("Event") or ""
        if not etype.startswith(_NATIVE_EVENT_PREFIX):
            continue
        suffix = etype[len(_NATIVE_EVENT_PREFIX) :]
        if event_type and suffix != event_type:
            continue
        if execution_id is not None and record.get("executionId") != execution_id:
            continue
        results.append(
            NativeEngineEvent(
                event_type=suffix,
                fields={k: v for k, v in record.items() if k != "Event"},
            )
        )
        if len(results) >= limit:
            break
    return results


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_query_fallback_summary(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    *,
    ctx: Context,
) -> List[QueryFallbackSummary]:
    """
    Summarize Gluten Full Query Fallback (FQF) decisions and operator
    fallback for an application's SQL execution(s).

    Answers: was FQF active for this query, did any part of it fall back to
    vanilla Spark (and why, per operator), and -- when FQF compared costs --
    which engine (vanilla or native) was selected and at what cost. Reads
    GlutenPlanFallbackEvent (per-operator fallback reasons) and the
    root-scope DPQueryCostDecisionEvent (the actual FQF cost comparison) from
    the raw event log; both are invisible to the standard REST API.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        execution_id: Optional SQL execution ID to scope to; when omitted,
            every execution ID seen in the event log is summarized

    Returns:
        List of QueryFallbackSummary, one per SQL execution ID found
    """
    client = get_client_or_default(ctx, server, app_id)
    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)

    fallback_by_exec: Dict[Any, List[dict]] = {}
    cost_by_exec: Dict[Any, List[dict]] = {}
    for record in iter_event_log_records(zip_bytes):
        etype = record.get("Event")
        if etype not in (_FALLBACK_EVENT, _COST_DECISION_EVENT):
            continue
        eid = record.get("executionId")
        if execution_id is not None and eid != execution_id:
            continue
        if etype == _FALLBACK_EVENT:
            fallback_by_exec.setdefault(eid, []).append(record)
        else:
            cost_by_exec.setdefault(eid, []).append(record)

    exec_ids = sorted(
        {eid for eid in (*fallback_by_exec, *cost_by_exec) if eid is not None}
    )
    return [
        _build_fallback_summary(
            eid, fallback_by_exec.get(eid, []), cost_by_exec.get(eid, [])
        )
        for eid in exec_ids
    ]


def _memo_from_raw(memo_raw: Any) -> Optional[NativeValidationMemoStats]:
    if not isinstance(memo_raw, dict):
        return None
    return NativeValidationMemoStats(
        entry_count=memo_raw.get("entryCount"),
        retained_bytes=memo_raw.get("retainedBytes"),
        hits=memo_raw.get("hits"),
        misses=memo_raw.get("misses"),
        bound_rejections=memo_raw.get("boundRejections"),
    )


def _planning_overhead_from_record(record: dict) -> QueryPlanningOverhead:
    eid = record.get("executionId")
    scope = record.get("scopeType")
    scope_type = scope.get("type") if isinstance(scope, dict) else None
    scope_id = scope.get("id") if isinstance(scope, dict) else None
    return QueryPlanningOverhead(
        execution_id=eid,
        scope_type=scope_type,
        scope_id=scope_id,
        compare_snapshot_build_ms=_nanos_to_ms(record.get("compareSnapshotBuildNanos")),
        evaluation_ms=_nanos_to_ms(record.get("evaluationNanos")),
        fallback_build_ms=_nanos_to_ms(record.get("fallbackBuildNanos")),
        gluten_build_ms=_nanos_to_ms(record.get("glutenBuildNanos")),
        planning_total_ms=_nanos_to_ms(record.get("planningTotalNanos")),
        memo=_memo_from_raw(record.get("nativeValidationMemo")),
    )


def _merge_root_planning_overhead(
    execution_id: Any, overhead_events: List[dict]
) -> Optional[QueryPlanningOverhead]:
    """Merge a query's several partial root-scope DPDecisionOverheadEvent
    records into one composite breakdown.

    Each phase (compare-snapshot build, cost evaluation, fallback-plan
    build, gluten-plan build, the running total) is emitted as its own
    event, all tagged with the same root scope -- exactly what the Spark
    History Server's ``/dpas/`` UI tab renders as one merged "Planning Time
    Breakdown" section per SQL id. Crucially, a query can run more than one
    such decision *cycle* (e.g. a second, smaller re-check for one more
    operator after the main decision), each contributing its own nanos
    events -- confirmed against a real event log where two cycles' 3.25ms
    ``fallbackBuildNanos`` events summed to the UI's reported 6.48ms (not
    either value alone). So every nanos field here is a SUM across all
    root-scope events, not "first non-null wins" -- summing a field that
    truly only ever has one event is a no-op, so this is safe either way.
    The native-validation memo is the one exception: it's a single
    snapshot of accumulated cache state, emitted once per query, so the
    (only) non-null one is used as-is rather than summed.
    """
    root_events = [e for e in overhead_events if _is_root_scope(e.get("scopeType"))]
    if not root_events:
        return None

    nanos_fields = [
        "compareSnapshotBuildNanos",
        "evaluationNanos",
        "fallbackBuildNanos",
        "glutenBuildNanos",
        "planningTotalNanos",
    ]
    merged: Dict[str, Any] = {field: None for field in nanos_fields}
    memo_raw = None
    for event in root_events:
        for field in nanos_fields:
            value = event.get(field)
            if value is None:
                continue
            merged[field] = value if merged[field] is None else merged[field] + value
        if memo_raw is None and isinstance(event.get("nativeValidationMemo"), dict):
            memo_raw = event.get("nativeValidationMemo")

    return QueryPlanningOverhead(
        execution_id=execution_id,
        scope_type="root",
        scope_id=None,
        compare_snapshot_build_ms=_nanos_to_ms(merged["compareSnapshotBuildNanos"]),
        evaluation_ms=_nanos_to_ms(merged["evaluationNanos"]),
        fallback_build_ms=_nanos_to_ms(merged["fallbackBuildNanos"]),
        gluten_build_ms=_nanos_to_ms(merged["glutenBuildNanos"]),
        planning_total_ms=_nanos_to_ms(merged["planningTotalNanos"]),
        memo=_memo_from_raw(memo_raw),
    )


def _parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    return value.strip().lower() == "true"


# Known Gluten/Velox physical-operator name markers, for a best-effort scan
# of a plan's text. Deliberately specific (not a bare "Columnar" substring
# match) -- plain vanilla Spark plans legitimately contain their own
# "ColumnarToRow"/"BatchScan" nodes (the vectorized Parquet reader) even
# when nothing ran on Gluten at all, as seen in real fallen-back plans.
_NATIVE_OPERATOR_MARKERS = re.compile(
    r"Transformer\b|Velox[A-Z]|\bColumnarExchange\b|\bColumnarShuffle\b|"
    r"\bColumnarBroadcastExchange\b"
)


def _plan_has_native_operators(plan_text: str) -> bool:
    return bool(_NATIVE_OPERATOR_MARKERS.search(plan_text))


def _native_execution_verdict(
    summary: QueryFallbackSummary,
    native_candidate_seen: bool,
    executed_plan_has_native_nodes: Optional[bool],
    use_dpas_decision: Optional[str],
) -> str:
    if use_dpas_decision == "nodpas":
        return (
            "This run was configured as a vanilla-Spark (nodpas) baseline -- "
            "DPAS/Gluten was never in play for this execution."
        )
    if not summary.fqf_enabled:
        return (
            "No FQF cost-decision event found for this execution: cost-based "
            "fallback wasn't evaluated here (native ran unconditionally "
            "wherever an operator was supported, or DPAS wasn't active)."
        )
    if summary.selected_engine == "gluten":
        return (
            "Ran natively: FQF selected the Gluten/Velox plan over vanilla "
            "Spark on cost."
        )
    if summary.selected_engine == "vanilla":
        bits = [
            "Fell back to vanilla Spark: FQF's cost-based decision selected "
            "vanilla over the native plan for this query."
        ]
        if summary.gluten_plan_cost is not None and math.isinf(summary.gluten_plan_cost):
            bits.append(
                "The native plan's estimated cost was Infinity (the cost "
                "model considered it infeasible)."
            )
        elif (
            summary.vanilla_plan_cost
            and summary.gluten_plan_cost
            and math.isfinite(summary.vanilla_plan_cost)
        ):
            ratio = summary.gluten_plan_cost / summary.vanilla_plan_cost
            bits.append(f"Estimated native cost was {ratio:.2f}x vanilla's.")
        if native_candidate_seen:
            bits.append(
                "Gluten DID build a native candidate plan for this query "
                "along the way -- it was rejected on cost, not because "
                "native execution was unavailable."
            )
        if executed_plan_has_native_nodes is False:
            bits.append(
                "Confirmed against the executed plan: it contains zero "
                "native (Transformer/Velox) operators."
            )
        elif executed_plan_has_native_nodes is True:
            bits.append(
                "Note: the executed plan still shows some native operator "
                "markers -- the fallback may be partial rather than total; "
                "check fallback_node_reasons per operator."
            )
        reasons = sorted(set(summary.fallback_node_reasons.values()))
        if reasons:
            bits.append("Reasons seen: " + "; ".join(reasons[:3]) + ".")
        return " ".join(bits)
    return (
        "FQF's cost-decision event was present but inconclusive (no "
        "selected_engine could be determined) -- inspect fallback_summary "
        "directly."
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def diagnose_native_execution(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    description: Optional[str] = None,
    *,
    ctx: Context,
) -> List[NativeExecutionDiagnosis]:
    """
    Preliminary DPAS/Gluten native-vs-vanilla diagnosis for an application's
    SQL execution(s): one call answering "did this run natively or fall
    back to vanilla Spark, and why".

    A native-capability config flag being enabled (e.g. a workload-specific
    "native write" toggle under spark.gluten.dp.*) only means native
    execution was *possible* for that operator -- it does not mean it
    *happened*, since Full Query Fallback (FQF) can still reject the native
    plan on cost for the whole query. This tool checks the config, the FQF
    decision, whether Gluten ever built a native candidate plan at all, and
    whether the plan that actually executed has any native operator, so
    that distinction doesn't have to be worked out by hand each time.

    For each SQL execution found, combines:
      * spark.gluten.dp.cost.queryFallbackEnabled and
        spark.gluten.dp.usedpas.decision from the environment, plus every
        other spark.gluten.dp.* property (workload-specific native-write /
        capability flags don't get a dedicated field, since their names
        vary by workload -- see dp_config);
      * the FQF cost decision and per-operator fallback reasons (same data
        as get_query_fallback_summary);
      * the driver-side planning overhead FQF itself cost, merged into one
        composite breakdown (same data as get_native_planning_overhead,
        but merged the way the Spark History Server's own /dpas/ UI tab
        renders it, instead of returned as several partial events);
      * native_candidate_seen: whether any GlutenPlanFallbackEvent for this
        execution shows numGlutenNodes > 0 -- Gluten built a native plan at
        some point, whether or not it was ultimately used;
      * executed_plan_has_native_nodes: a best-effort scan of the plan that
        actually executed for known Gluten/Velox operator-name markers, as
        ground-truth confirmation of the FQF decision.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        execution_id: Optional SQL execution ID to scope to; when omitted,
            every execution ID seen in the event log is diagnosed
        description: Optional case-insensitive substring of the SQL
            execution's description (e.g. a table or query name like
            "q42" or "store_sales") -- diagnoses only matching executions,
            so you don't need a separate list_sql_executions call first

    ``dp_config`` (every spark.gluten.dp.* property) is identical for all of
    an app's executions, so it is only filled on the FIRST result to avoid
    repeating it per query.

    Returns:
        List of NativeExecutionDiagnosis, one per SQL execution ID found
    """
    client = get_client_or_default(ctx, server, app_id)

    env = client.get_environment(app_id=app_id, app_attempt_id=app_attempt_id)
    spark_props = dict(env.spark_properties or [])
    dp_config = {
        k: v for k, v in spark_props.items() if k.startswith("spark.gluten.dp.")
    }
    cost_fallback_enabled = _parse_bool(
        dp_config.get("spark.gluten.dp.cost.queryFallbackEnabled")
    )
    use_dpas_decision = dp_config.get("spark.gluten.dp.usedpas.decision")

    wanted_ids: Optional[set] = None
    if description:
        needle = description.lower()
        wanted_ids = {
            e.id
            for e in _fetch_all_sql_executions(client, app_id, app_attempt_id)
            if e.id is not None and needle in (e.description or "").lower()
        }
        if not wanted_ids:
            return []

    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)

    fallback_by_exec: Dict[Any, List[dict]] = {}
    cost_by_exec: Dict[Any, List[dict]] = {}
    overhead_by_exec: Dict[Any, List[dict]] = {}
    for record in iter_event_log_records(zip_bytes):
        etype = record.get("Event")
        if etype not in (_FALLBACK_EVENT, _COST_DECISION_EVENT, _DECISION_OVERHEAD_EVENT):
            continue
        eid = record.get("executionId")
        if execution_id is not None and eid != execution_id:
            continue
        if wanted_ids is not None and eid not in wanted_ids:
            continue
        if etype == _FALLBACK_EVENT:
            fallback_by_exec.setdefault(eid, []).append(record)
        elif etype == _COST_DECISION_EVENT:
            cost_by_exec.setdefault(eid, []).append(record)
        else:
            overhead_by_exec.setdefault(eid, []).append(record)

    exec_ids = sorted(
        {
            eid
            for eid in (*fallback_by_exec, *cost_by_exec, *overhead_by_exec)
            if eid is not None
        }
    )

    results: List[NativeExecutionDiagnosis] = []
    for eid in exec_ids:
        fallback_events = fallback_by_exec.get(eid, [])
        summary = _build_fallback_summary(eid, fallback_events, cost_by_exec.get(eid, []))
        overhead = _merge_root_planning_overhead(eid, overhead_by_exec.get(eid, []))
        native_candidate_seen = any(
            (e.get("numGlutenNodes") or 0) > 0 for e in fallback_events
        )

        executed_plan_has_native_nodes: Optional[bool] = None
        try:
            execution = client.get_sql_execution(
                app_id=app_id,
                execution_id=eid,
                app_attempt_id=app_attempt_id,
                details=True,
                plan_description=True,
            )
            plan_text = execution.plan_description or ""
            if plan_text:
                executed_plan_has_native_nodes = _plan_has_native_operators(plan_text)
        except Exception:
            pass  # noqa: S110 -- best-effort cross-check, not the primary signal

        verdict = _native_execution_verdict(
            summary, native_candidate_seen, executed_plan_has_native_nodes, use_dpas_decision
        )

        results.append(
            NativeExecutionDiagnosis(
                execution_id=eid,
                cost_fallback_enabled=cost_fallback_enabled,
                use_dpas_decision=use_dpas_decision,
                dp_config=dp_config if not results else {},
                fallback_summary=summary,
                planning_overhead=overhead,
                native_candidate_seen=native_candidate_seen,
                executed_plan_has_native_nodes=executed_plan_has_native_nodes,
                verdict=verdict,
            )
        )
    return results


_COST_FALLBACK_REASON_RE = re.compile(r"^Query DP cost fallback:")


def _classify_fallback_reason(reason: str) -> str:
    """Bucket one operator's fallback reason string into a fixed set of
    causes, so offload/fallback summaries can be counted by cause rather
    than left as free text.

    * ``cost_fallback``: FQF's cost model picked vanilla over a native plan
      it was capable of building for this operator/query.
    * ``unsupported_operator``: Gluten has no native implementation for
      this operator at all -- cost was never in play.
    * ``columnar_cache_disabled``: a cached (``InMemoryRelation``) table
      forces a row/columnar conversion boundary any native plan touching it
      has to pay for -- usually what inflates cost_fallback estimates too.
    * ``other``: anything not matching the above (reported verbatim
      elsewhere via fallback_node_reasons; this bucket just isn't
      double-guessed).
    """
    if _COST_FALLBACK_REASON_RE.search(reason):
        return "cost_fallback"
    if "does not support it" in reason or "does not touch it" in reason:
        return "unsupported_operator"
    if "Columnar table cache is disabled" in reason:
        return "columnar_cache_disabled"
    return "other"


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def summarize_native_execution(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    *,
    ctx: Context,
) -> NativeExecutionAppSummary:
    """
    DPAS/Gluten offload-vs-fallback and native-validation-memo rollup for an
    application: answers "how much of this run's TPC-DS/TPC-H workload
    actually went native, how much fell back and why (cost vs unsupported
    operator vs cache limitation), and how well the FQF memo is working" in
    one call, across every SQL execution in the app (or just one).

    Complements diagnose_native_execution (which explains one execution's
    native-vs-vanilla decision in prose) with per-operator counts and
    app-wide totals meant for scanning a whole query suite at once:

      * selected_engine / fell_back per execution -- the dpas-vs-vanilla
        tally (executions_ran_native / executions_fell_back at the app
        level), from the same FQF cost-decision data as
        get_query_fallback_summary;
      * native_operator_count / fallback_operator_count -- counted from the
        LAST GlutenPlanFallbackEvent recorded for the execution, which is
        the plan that actually executed (numGlutenNodes/numFallbackNodes on
        that event), not an earlier candidate AQE re-plan;
      * peak_native_operator_count -- the highest numGlutenNodes seen
        across ANY candidate plan for the query, i.e. the offload ceiling
        Gluten was capable of, whether or not FQF used it -- compare
        against native_operator_count to see how much offload potential
        was left on the table;
      * fallback_reason_counts / fallback_category_totals -- every
        executed-plan fallback reason bucketed into cost_fallback,
        unsupported_operator, columnar_cache_disabled, or other (see
        _classify_fallback_reason), both per execution and summed for the
        app;
      * memo_total_entries/hits/misses/bound_rejections and
        memo_overall_hit_rate -- the FQF native-validation memo's
        aggregate effectiveness across every execution in the app (each
        execution's own memo snapshot, from get_native_planning_overhead,
        summed).

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        execution_id: Optional SQL execution ID to scope to; when omitted,
            every execution ID seen in the event log is summarized

    Returns:
        NativeExecutionAppSummary with the app-level config/tallies and one
        NativeExecutionSummary per SQL execution
    """
    client = get_client_or_default(ctx, server, app_id)

    env = client.get_environment(app_id=app_id, app_attempt_id=app_attempt_id)
    spark_props = dict(env.spark_properties or [])
    dp_config = {
        k: v for k, v in spark_props.items() if k.startswith("spark.gluten.dp.")
    }
    cost_fallback_enabled = _parse_bool(
        dp_config.get("spark.gluten.dp.cost.queryFallbackEnabled")
    )
    use_dpas_decision = dp_config.get("spark.gluten.dp.usedpas.decision")

    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)

    fallback_by_exec: Dict[Any, List[dict]] = {}
    cost_by_exec: Dict[Any, List[dict]] = {}
    overhead_by_exec: Dict[Any, List[dict]] = {}
    for record in iter_event_log_records(zip_bytes):
        etype = record.get("Event")
        if etype not in (_FALLBACK_EVENT, _COST_DECISION_EVENT, _DECISION_OVERHEAD_EVENT):
            continue
        eid = record.get("executionId")
        if execution_id is not None and eid != execution_id:
            continue
        if etype == _FALLBACK_EVENT:
            fallback_by_exec.setdefault(eid, []).append(record)
        elif etype == _COST_DECISION_EVENT:
            cost_by_exec.setdefault(eid, []).append(record)
        else:
            overhead_by_exec.setdefault(eid, []).append(record)

    exec_ids = sorted(
        {
            eid
            for eid in (*fallback_by_exec, *cost_by_exec, *overhead_by_exec)
            if eid is not None
        }
    )

    executions: List[NativeExecutionSummary] = []
    category_totals: Dict[str, int] = {}
    memo_entries = memo_hits = memo_misses = memo_rejections = 0
    ran_native = fell_back_count = 0

    for eid in exec_ids:
        fallback_events = fallback_by_exec.get(eid, [])
        summary = _build_fallback_summary(eid, fallback_events, cost_by_exec.get(eid, []))
        overhead = _merge_root_planning_overhead(eid, overhead_by_exec.get(eid, []))

        last_event = fallback_events[-1] if fallback_events else None
        native_count = int((last_event or {}).get("numGlutenNodes") or 0)
        fallback_count = int((last_event or {}).get("numFallbackNodes") or 0)
        node_reasons = (last_event or {}).get("fallbackNodeToReason") or {}
        peak_native_count = max(
            (int(e.get("numGlutenNodes") or 0) for e in fallback_events), default=0
        )

        reason_counts: Dict[str, int] = {}
        for reason in node_reasons.values():
            category = _classify_fallback_reason(reason)
            reason_counts[category] = reason_counts.get(category, 0) + 1
            category_totals[category] = category_totals.get(category, 0) + 1

        denom = native_count + fallback_count
        offload_ratio = (native_count / denom) if denom else None

        if summary.selected_engine == "gluten":
            ran_native += 1
        elif summary.selected_engine == "vanilla":
            fell_back_count += 1

        if overhead is not None and overhead.memo is not None:
            memo_entries += overhead.memo.entry_count or 0
            memo_hits += overhead.memo.hits or 0
            memo_misses += overhead.memo.misses or 0
            memo_rejections += overhead.memo.bound_rejections or 0

        try:
            description = client.get_sql_execution(
                app_id, eid, details=False, plan_description=False,
                app_attempt_id=app_attempt_id,
            ).description
        except Exception:
            description = None

        executions.append(
            NativeExecutionSummary(
                execution_id=eid,
                description=description,
                selected_engine=summary.selected_engine,
                fell_back=summary.fell_back,
                vanilla_plan_cost=summary.vanilla_plan_cost,
                gluten_plan_cost=summary.gluten_plan_cost,
                native_operator_count=native_count,
                fallback_operator_count=fallback_count,
                peak_native_operator_count=peak_native_count,
                offload_ratio=offload_ratio,
                fallback_reason_counts=reason_counts,
                memo=overhead.memo if overhead is not None else None,
            )
        )

    memo_denom = memo_hits + memo_misses
    return NativeExecutionAppSummary(
        app_id=app_id,
        use_dpas_decision=use_dpas_decision,
        cost_fallback_enabled=cost_fallback_enabled,
        executions=executions,
        executions_total=len(executions),
        executions_ran_native=ran_native,
        executions_fell_back=fell_back_count,
        fallback_category_totals=category_totals,
        memo_total_entries=memo_entries,
        memo_total_hits=memo_hits,
        memo_total_misses=memo_misses,
        memo_total_bound_rejections=memo_rejections,
        memo_overall_hit_rate=(memo_hits / memo_denom) if memo_denom else None,
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_native_planning_overhead(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    *,
    ctx: Context,
) -> List[QueryPlanningOverhead]:
    """
    Get Gluten native-planning overhead timings for an application's SQL
    execution(s) -- how long FQF spent building/comparing/evaluating native
    vs vanilla plans on the driver, separate from actual query execution
    time.

    Reads DPDecisionOverheadEvent from the raw event log (invisible to the
    standard REST API). ``compare_snapshot_build_ms``/``gluten_build_ms``/
    etc are the phases the FQF native-validation memo is designed to
    shrink; ``memo`` (present on the root-scope event, once per query) is
    the memo's own entry count/retained bytes/hits/misses/bound-rejections
    for that query, from which ``memo.hit_rate`` is derived -- confirmed
    real fields, seen at 40-95% hit rates across real TPC-DS queries.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID (latest if omitted)
        execution_id: Optional SQL execution ID to scope to; when omitted,
            every scoped event seen in the event log is returned

    Returns:
        List of QueryPlanningOverhead, one per (execution ID, decision scope)
        event
    """
    client = get_client_or_default(ctx, server, app_id)
    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)

    results: List[QueryPlanningOverhead] = []
    for record in iter_event_log_records(zip_bytes):
        if record.get("Event") != _DECISION_OVERHEAD_EVENT:
            continue
        eid = record.get("executionId")
        if execution_id is not None and eid != execution_id:
            continue
        results.append(_planning_overhead_from_record(record))
    return results


# ----------------------------------------------------------------------
# Bloom filter producer/consumer ownership.
#
# Ported from this project's Gluten Bloom-filter ownership QA tooling
# (dopant's core/bloomfilter/validator.py), which classifies bloom_filter_agg
# (producer) / might_contain (consumer) ownership from a SQL plan's text.
# Adapted to report ambiguity via a returned note instead of raising, since
# this is a best-effort inspection tool, not a QA gate.
# ----------------------------------------------------------------------

# Spark 3.3.x prints a separate "===== Subqueries =====" trailer section with
# each materialized subquery's own plan. Spark 3.5.x inlines materialized
# subqueries directly in the tree as "Subquery subquery#<id>, [id=#<id>]"
# placeholders instead -- same semantic, different rendering.
_MATERIALIZED_SUBQUERY_RE = re.compile(r"Subquery subquery#\d+, \[id=#\d+\]")


def _operator_section(plan: str, operator_id: str) -> Tuple[str, str]:
    """Return (node type name, full text block) for operator number
    ``operator_id`` in ``plan``."""
    match = re.search(
        rf"^\({operator_id}\) ([^\n]+)\n(.*?)(?=\n\([0-9]+\) |\Z)",
        plan,
        flags=re.MULTILINE | re.DOTALL,
    )
    if not match:
        raise ValueError(f"Missing operator definition {operator_id}")
    return match.group(1), match.group(0)


def _classify_consumer_node(
    operator_id: str, node: str, hosts: List[str], active_host_ids: List[str], owners: set
) -> None:
    if "FilterExecTransformer" in node or "ScanTransformer" in node:
        owner = "velox"
    elif node.startswith("Filter") and "Transformer" not in node:
        owner = "spark"
    else:
        raise ValueError(f"Unknown Bloom consumer operator: {node}")
    owners.add(owner)
    hosts.append(f"{operator_id}:{node}")
    active_host_ids.append(operator_id)


def _consumer_ownership_legacy(plan: str) -> Tuple[str, List[str], List[str]]:
    """Spark 3.3.x: might_contain hosts are named by a separate 'Hosting
    operator id' trailer."""
    host_ids = sorted(
        set(
            re.findall(
                r"Subquery:\d+ Hosting operator id = (\d+) Hosting Expression = [^\n]*Subquery",
                plan,
            )
        )
    )
    if not host_ids:
        raise ValueError("No Bloom scalar-subquery host")

    owners: set = set()
    hosts: List[str] = []
    active_host_ids: List[str] = []
    for operator_id in host_ids:
        node, section = _operator_section(plan, operator_id)
        if "might_contain(" not in section:
            continue
        _classify_consumer_node(operator_id, node, hosts, active_host_ids, owners)

    if not active_host_ids:
        raise ValueError("No hosting operator contains might_contain")
    if len(owners) != 1:
        raise ValueError(f"Ambiguous Bloom consumer ownership: {sorted(owners)}")
    return owners.pop(), hosts, active_host_ids


def _strip_to_current_plan(plan: str) -> str:
    """Drop the OUTER query's own stale "Initial Plan" trailer, if present."""
    final_match = re.search(r"^\+- == Final Plan ==\s*\n", plan, re.MULTILINE)
    if final_match:
        plan = plan[final_match.end() :]
    initial_match = re.search(r"^\+- == Initial Plan ==\s*\n", plan, re.MULTILINE)
    if initial_match:
        plan = plan[: initial_match.start()]
    return plan


def _line_at(text: str, pos: int) -> str:
    line_start = text.rfind("\n", 0, pos) + 1
    line_end = text.find("\n", pos)
    if line_end == -1:
        line_end = len(text)
    return text[line_start:line_end]


def _indent_at(text: str, pos: int) -> int:
    line_start = text.rfind("\n", 0, pos) + 1
    match = re.match(r"[:\s]*", text[line_start:])
    # A `*`-quantified pattern always matches (the empty string satisfies
    # it), so `re.match` can never return None here -- this narrows the type
    # for the checker rather than masking a real failure mode.
    assert match is not None
    return len(match.group(0))


def _subquery_branch_markers(plan: str) -> List[Tuple[int, int]]:
    return [
        (m.start(), _indent_at(plan, m.start()))
        for m in re.finditer(r"Subquery subquery#\d+, \[id=#\d+\]\s*\n", plan)
    ]


def _subquery_nesting_depths(plan: str) -> List[Tuple[int, int]]:
    """Map every line's start offset to how many subquery branches are
    currently open there, via indentation-based scope tracking (a stack)."""
    marker_starts = sorted(
        m.start() for m in re.finditer(r"Subquery subquery#\d+, \[id=#\d+\]\s*\n", plan)
    )
    marker_idx = 0
    depths: List[Tuple[int, int]] = []
    stack: List[int] = []
    pos = 0
    for line in plan.split("\n"):
        line_end = pos + len(line)
        indent_match = re.match(r"[:\s]*", line)
        # Same always-matches invariant as _indent_at above.
        assert indent_match is not None
        indent = len(indent_match.group(0))
        while stack and indent <= stack[-1]:
            stack.pop()
        depths.append((pos, len(stack)))
        while marker_idx < len(marker_starts) and marker_starts[marker_idx] < line_end:
            if marker_starts[marker_idx] >= pos:
                stack.append(indent)
            marker_idx += 1
        pos = line_end + 1
    return depths


def _depth_at(depths: List[Tuple[int, int]], pos: int) -> int:
    lo, hi = 0, len(depths)
    while lo < hi:
        mid = (lo + hi) // 2
        if depths[mid][0] <= pos:
            lo = mid + 1
        else:
            hi = mid
    return depths[lo - 1][1] if lo else 0


def _consumer_ownership_inline(plan: str) -> Tuple[str, List[str], List[str]]:
    """Spark 3.5.x: might_contain(Subquery subquery#N, ...) is inlined into
    the condition text of its hosting operator, on one unbroken line."""
    depths = _subquery_nesting_depths(plan)
    owners: set = set()
    hosts: List[str] = []
    active_host_ids: List[str] = []
    for m in re.finditer(r"might_contain\(Subquery (subquery#\d+),", plan):
        pos = m.start()
        if _depth_at(depths, pos) > 0:
            continue  # nested inside a different subquery's own build
        subquery_name = m.group(1)
        line = _line_at(plan, pos)
        if "FilterExecTransformer" in line or "ScanTransformer" in line:
            owner = "velox"
        elif "Filter (" in line and "Transformer" not in line:
            owner = "spark"
        else:
            raise ValueError(f"Unknown Bloom consumer operator for line: {line[:160]}")
        owners.add(owner)
        hosts.append(f"{subquery_name}:{line.strip()[:60]}")
        active_host_ids.append(subquery_name)

    if not active_host_ids:
        raise ValueError("No hosting operator contains might_contain")
    if len(owners) != 1:
        raise ValueError(f"Ambiguous Bloom consumer ownership: {sorted(owners)}")
    return owners.pop(), hosts, active_host_ids


def consumer_ownership(plan: str) -> Tuple[str, List[str], List[str]]:
    """Find the might_contain-hosting operator(s) and classify their engine."""
    if "===== Subqueries =====" in plan:
        return _consumer_ownership_legacy(plan)
    return _consumer_ownership_inline(_strip_to_current_plan(plan))


def _classify_aggregate_section(final_plan: str, plan: str, host_id: str) -> str:
    native_ids = set(
        re.findall(
            r"(?:HashAggregate|Aggregate)[A-Za-z0-9_]*Transformer \(([0-9]+)\)",
            final_plan,
        )
    )
    spark_ids = set(re.findall(r"ObjectHashAggregate \(([0-9]+)\)", final_plan))
    if bool(native_ids) == bool(spark_ids):
        raise ValueError(f"Ambiguous Bloom producer ownership for host {host_id}")

    aggregate_ids = native_ids or spark_ids
    if not any(
        "bloom_filter_agg(" in _operator_section(plan, operator_id)[1]
        for operator_id in aggregate_ids
    ):
        raise ValueError(f"Final Bloom aggregate not found for host {host_id}")
    return "velox" if native_ids else "spark"


def _producer_for_host_legacy(plan: str, host_id: str) -> str:
    heading = re.search(
        rf"Subquery:\d+ Hosting operator id = {host_id} Hosting Expression = "
        rf"(ReusedSubquery )?Subquery (subquery#\d+)",
        plan,
    )
    if not heading:
        raise ValueError(f"Missing scalar subquery for host {host_id}")

    if heading.group(1):
        subquery_name = heading.group(2)
        original = re.search(
            rf"Subquery:\d+ Hosting operator id = \d+ Hosting Expression = Subquery {re.escape(subquery_name)}\b",
            plan,
        )
        if not original:
            raise ValueError(
                f"Missing original definition of {subquery_name} reused by host {host_id}"
            )
        heading = original

    next_heading = re.search(
        r"Subquery:\d+ Hosting operator id = \d+ Hosting Expression",
        plan[heading.end() :],
    )
    subquery_end = heading.end() + next_heading.start() if next_heading else len(plan)
    subquery = plan[heading.start() : subquery_end]

    if "+- == Final Plan ==" in subquery:
        final_plan = subquery.split("+- == Final Plan ==", 1)[1].split(
            "+- == Initial Plan ==", 1
        )[0]
    else:
        final_plan = subquery

    return _classify_aggregate_section(final_plan, plan, host_id)


def _classify_producer_line(line: str, host_id: str) -> str:
    if "Transformer" in line:
        return "velox"
    if "ObjectHashAggregate" in line or re.search(r"\bHashAggregate\b", line):
        return "spark"
    raise ValueError(f"Ambiguous Bloom producer ownership for host {host_id}: {line[:160]}")


def _producer_for_host_inline(plan: str, host_id: str) -> str:
    markers = _subquery_branch_markers(plan)
    depths = _subquery_nesting_depths(plan)
    candidates = [
        (pos, ind)
        for pos, ind in markers
        if plan[pos:].startswith(f"Subquery {host_id}, ") and _depth_at(depths, pos) == 0
    ]
    if not candidates:
        raise ValueError(f"Missing top-level definition operator for {host_id}")

    owners: set = set()
    for def_pos, def_indent in candidates:
        section_end = len(plan)
        for mpos, mind in markers:
            if mpos > def_pos and mind <= def_indent:
                section_end = mpos
                break
        section = plan[def_pos:section_end]
        agg_match = re.search(r"(?<!partial_)bloom_filter_agg\(", section)
        if agg_match:
            owners.add(_classify_producer_line(_line_at(section, agg_match.start()), host_id))

    if not owners:
        raise ValueError(f"Final Bloom aggregate not found for host {host_id}")
    if len(owners) != 1:
        raise ValueError(f"Ambiguous Bloom producer ownership for host {host_id}: {sorted(owners)}")
    return owners.pop()


def producer_for_host(plan: str, host_id: str) -> str:
    if "===== Subqueries =====" in plan:
        return _producer_for_host_legacy(plan, host_id)
    return _producer_for_host_inline(_strip_to_current_plan(plan), host_id)


def producer_ownership(plan: str, host_ids: List[str]) -> str:
    owners = {producer_for_host(plan, operator_id) for operator_id in host_ids}
    if len(owners) != 1:
        raise ValueError(f"Ambiguous Bloom producer ownership: {sorted(owners)}")
    return owners.pop()


def detect_bloom_filter_ownership(plan_text: str) -> Dict[str, Any]:
    """Detect Bloom-filter producer/consumer engine ownership from a
    physical plan's text. Never raises -- an ambiguous or unrecognized plan
    shape is reported via ``note`` rather than failing the caller."""
    if "bloom_filter_agg(" not in plan_text or "might_contain(" not in plan_text:
        return {"used": False}
    if not (
        "===== Subqueries =====" in plan_text
        or _MATERIALIZED_SUBQUERY_RE.search(plan_text)
    ):
        return {
            "used": True,
            "note": "Bloom filter expressions found but no materialized "
            "subquery marker -- ownership couldn't be classified",
        }
    try:
        consumer, hosts, host_ids = consumer_ownership(plan_text)
        producer = producer_ownership(plan_text, host_ids)
    except Exception as error:
        return {"used": True, "note": str(error)}
    return {"used": True, "producer": producer, "consumer": consumer, "hosts": hosts}


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_bloom_filter_ownership(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    top_n: int = 10,
    *,
    ctx: Context,
) -> List[BloomFilterOwnership]:
    """
    Detect Spark runtime Bloom filter usage and producer/consumer engine
    ownership for an application's SQL execution(s).

    Spark's runtime Bloom filter join optimization builds the filter with a
    bloom_filter_agg aggregate and evaluates it with might_contain; under
    Gluten each side can independently run on Spark or the native (Velox)
    engine depending on expression support and fallback config. Detected
    purely from each execution's physical plan text (already available via
    the standard REST API) by matching each side's hosting operator name --
    e.g. FilterExecTransformer/ScanTransformer = native, plain Filter =
    Spark -- no raw event log access needed.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID
        execution_id: Optional SQL execution ID to scope to. When omitted,
            the ``top_n`` slowest SQL executions are scanned instead.
        top_n: Number of slowest SQL executions to scan when execution_id is
            not given (default: 10)

    Returns:
        List of BloomFilterOwnership: whether a Bloom filter was used per
        execution, and if so its producer/consumer engine ownership (or a
        note when the plan shape couldn't be classified unambiguously)
    """
    client = get_client_or_default(ctx, server, app_id)

    if execution_id is not None:
        sql_ids = [execution_id]
    else:
        sql_ids = [
            s.id
            for s in list_sql_executions(
                app_id, server=server, app_attempt_id=app_attempt_id,
                sort_by="duration", limit=top_n, ctx=ctx,
            )
            if s.id is not None
        ]

    results: List[BloomFilterOwnership] = []
    for sql_id in sql_ids:
        execution = client.get_sql_execution(
            app_id=app_id,
            execution_id=sql_id,
            app_attempt_id=app_attempt_id,
            details=True,
            plan_description=True,
        )
        detection = detect_bloom_filter_ownership(execution.plan_description or "")
        results.append(
            BloomFilterOwnership(
                execution_id=sql_id,
                used=detection.get("used", False),
                producer=detection.get("producer"),
                consumer=detection.get("consumer"),
                hosts=detection.get("hosts") or [],
                note=detection.get("note"),
            )
        )
    return results


# ---------------------------------------------------------------------------
# Gluten/DPAS UI tabs (``/{app_id}/gluten/`` and ``/{app_id}/dpas/``)
#
# The History Server's own Gluten plugin registers these as ordinary
# server-rendered SparkUITab pages -- confirmed live (not guessed) that:
#   * Neither has a JSON API: /api/v1/applications/{id}/dpas and .../gluten
#     both 404 on a real server.
#   * The "+details" expansion is plain CSS (a `.stage-details.collapsed`
#     div toggled by an onclick handler) -- the content is already present
#     in one plain HTTP GET, no JS execution/Playwright required.
#   * /dpas/ is a strict superset of /gluten/'s per-query table: both carry
#     "Fallback Summary"/"Physical Plan"/"Initial Plan" for every query,
#     but only /dpas/ additionally carries "Planning Time Breakdown",
#     "Smart Acceleration Summary", "DPA-S Plan"/"Vanilla Plan", and
#     "Subquery Cost Evaluations" -- so /dpas/ is used here as the sole
#     source for per-query data. /gluten/ is used only for its OTHER
#     content: a small, separate "Gluten Build Information" key/value
#     table that /dpas/ does not carry at all.
#   * Table rows carry the row's execution id twice, redundantly: as the
#     first <td>'s bare text, and inside the description link's
#     ".../SQL/execution/?id={id}" href -- the latter is used here since
#     it can't be confused with any other numeric cell.
#   * "Num Gluten Nodes"/"Num Fallback Nodes" are also given as clean
#     ``sorttable_customkey="N"`` attributes on their <td> cells, which is
#     used here instead of parsing the padded/indented cell text.
# ---------------------------------------------------------------------------

_SQL_EXECUTION_ID_RE = re.compile(r"SQL/execution/\?id=(\d+)")
_SORTTABLE_KEY_RE = re.compile(r'sorttable_customkey="(-?\d+)"')
_PRE_BLOCK_RE = re.compile(r"<pre>(.*?)</pre>", re.DOTALL)
_SECTION_SPLIT_RE = re.compile(r"\n?== (.+?) ==\n")

_PLANNING_TIME_FIELD_RE = re.compile(
    r"^\s*(Total Planning Time|Smart-Fallback Overhead|compare snapshot build|"
    r"cost evaluation|Gluten plan build):\s*([-+]?(?:\\d[\\d,]*\\.?\\d*(?:[eE][-+]?\\d+)?|Infinity|NaN))\s*ms",
    re.MULTILINE,
)
_NUM = r"[-+]?(?:\d[\d,]*\.?\d*(?:[eE][-+]?\d+)?|Infinity|NaN)"
_TOTAL_COST_RE = re.compile(rf"Total Cost:\s*vanilla=({_NUM}),\s*dpa-s=({_NUM})")
_PLAN_COST_LINE_RE = re.compile(
    r"^[\s:+\-|^]*(?P<name>.+?)\s+\((?P<id>\d+)\):\s*(?P<rest>.+?)\s*$"
)
_STATS_RE = re.compile(r"stats=\((?P<stats>.*)\)\s*$")


def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _ui_base_url(client) -> str:
    """The History Server's UI base, e.g. "http://host:18080/history/" --
    distinct from SparkRestClient.base_url, which points at "/api/v1".
    An event-log server has no UI of its own: use (and if needed start) the
    local History Server over its directory."""
    shs = getattr(client, "history_server_client", None)
    if callable(shs):
        client = shs()
    return client.config.url.rstrip("/") + "/history/"


def _ui_request_kwargs(client) -> Dict[str, Any]:
    """Auth/TLS kwargs for a plain ``requests`` call, mirroring how
    SparkRestClient._build_api_client applies the same ServerConfig fields
    to the generated OpenAPI client -- this just isn't going through that
    client, since it's fetching a UI page, not an /api/v1 endpoint."""
    kwargs: Dict[str, Any] = {
        "timeout": client.config.timeout,
        "verify": client.config.ssl_ca_cert or client.config.verify_ssl,
    }
    auth = client.config.auth
    if auth:
        if auth.username and auth.password:
            kwargs["auth"] = (auth.username, auth.password)
        elif auth.token:
            kwargs["headers"] = {"Authorization": f"Bearer {auth.token}"}
    return kwargs


def _cost_summary_without_dpas_tab(
    client, app_id, execution_id, server, app_attempt_id, ctx
) -> Optional[NativeEngineUiQuerySummary]:
    """No /dpas/ tab (plugin build without it): rebuild the summary from
    /gluten/json (node counts, reasons) plus the event log's root cost
    decision, instead of wrongly reporting "no cost decision"."""
    ui = _gluten_ui_summary_or_none(client, app_id, app_attempt_id) or {}
    row = next(
        (
            e
            for e in (ui.get("queries") or {}).get("executions") or []
            if _int_or_none(e.get("executionId")) == execution_id
        ),
        None,
    )
    fqf = get_query_fallback_summary(
        app_id, server=server, app_attempt_id=app_attempt_id,
        execution_id=execution_id, ctx=ctx,
    )
    fqf = fqf[0] if fqf else None
    if row is None and fqf is None:
        return None
    reasons = (row or {}).get("fallbackNodeToReason") or (fqf.fallback_node_reasons if fqf else {})
    return NativeEngineUiQuerySummary(
        execution_id=execution_id,
        num_gluten_nodes=_int_or_none((row or {}).get("numGlutenNodes")) or 0,
        num_fallback_nodes=_int_or_none((row or {}).get("numFallbackNodes")) or 0,
        fallback_summary_text="\n".join(f"{k}: {v}" for k, v in reasons.items()) or None,
        has_cost_decision=bool(fqf and fqf.selected_engine),
        vanilla_cost=fqf.vanilla_plan_cost if fqf else None,
        dpa_s_cost=fqf.gluten_plan_cost if fqf else None,
    )


def _fetch_ui_page(client, app_id: str, tab: str, app_attempt_id: Optional[str] = None) -> str:
    from spark_history_mcp.api.spark_client import SparkRestClient

    app_path = SparkRestClient._app_path(app_id, app_attempt_id)
    url = f"{_ui_base_url(client)}{app_path}/{tab}/"
    # A missing plugin tab 302-redirects to /jobs/; following it would
    # silently parse the wrong page and report "no data".
    response = requests.get(url, allow_redirects=False, **_ui_request_kwargs(client))
    if 300 <= response.status_code < 400 or response.status_code == 404:
        raise UiTabUnavailableError(f"/{tab}/ tab not available for {app_id}")
    response.raise_for_status()
    return response.text


class UiTabUnavailableError(LookupError):
    """The History Server has no such plugin tab for this app."""


def _iter_ui_table_rows(html_text: str) -> Iterator[str]:
    """Split a page's FIRST <tbody>...</tbody> block into individual <tr>
    row fragments. Only the first tbody is used: on /dpas/ that's the only
    one (the per-query table); on /gluten/ it's the build-info table, which
    is why that page is never used here for per-query data (see module-
    level comment above)."""
    match = re.search(r"<tbody>(.*?)</tbody>", html_text, re.DOTALL)
    if not match:
        return
    for row in re.split(r"</tr>\s*<tr>", match.group(1)):
        yield row


def _parse_planning_overhead(pre_text: str) -> Optional[NativeEngineUiPlanningOverhead]:
    fields: Dict[str, float] = {}
    for name, value in _PLANNING_TIME_FIELD_RE.findall(pre_text):
        fields[name] = float(value.replace(",", ""))
    if not fields:
        return None
    return NativeEngineUiPlanningOverhead(
        total_planning_time_ms=fields.get("Total Planning Time"),
        smart_fallback_overhead_ms=fields.get("Smart-Fallback Overhead"),
        compare_snapshot_build_ms=fields.get("compare snapshot build"),
        cost_evaluation_ms=fields.get("cost evaluation"),
        gluten_plan_build_ms=fields.get("Gluten plan build"),
    )


def _parse_plan_cost_nodes(section_text: str) -> List[NativeEngineUiPlanCostNode]:
    nodes: List[NativeEngineUiPlanCostNode] = []
    for line in section_text.splitlines():
        if not line.strip() or ": cost=" not in line:
            continue
        match = _PLAN_COST_LINE_RE.match(line)
        if not match:
            continue
        rest = match.group("rest")
        stats_match = _STATS_RE.search(rest)
        stats = stats_match.group("stats") if stats_match else None
        head = rest[: stats_match.start()] if stats_match else rest
        kv: Dict[str, str] = {}
        for part in head.split(","):
            part = part.strip().rstrip(",")
            if "=" in part:
                key, _, value = part.partition("=")
                kv[key.strip()] = value.strip()
        nodes.append(
            NativeEngineUiPlanCostNode(
                node_id=match.group("id"),
                node_name=match.group("name").strip(),
                cost=_to_float(kv.get("cost")),
                cumulative_cost=_to_float(kv.get("cumulativeCost")),
                cost_factor=_to_float(kv.get("cf")),
                stats=stats,
            )
        )
    return nodes


def _parse_dpas_row(row_html: str) -> Optional[NativeEngineUiQuerySummary]:
    id_match = _SQL_EXECUTION_ID_RE.search(row_html)
    if not id_match:
        return None
    execution_id = int(id_match.group(1))

    keys = _SORTTABLE_KEY_RE.findall(row_html)
    num_gluten_nodes = int(keys[0]) if len(keys) >= 1 else 0
    num_fallback_nodes = int(keys[1]) if len(keys) >= 2 else 0

    pre_match = _PRE_BLOCK_RE.search(row_html)
    pre_text = _html.unescape(pre_match.group(1)) if pre_match else ""

    parts = _SECTION_SPLIT_RE.split(pre_text)
    # re.split with a capturing group yields [preamble, name1, body1, name2, body2, ...]
    sections: Dict[str, str] = {}
    for i in range(1, len(parts) - 1, 2):
        sections[parts[i]] = parts[i + 1]

    fallback_summary_text = sections.get("Fallback Summary")
    description = None
    desc_match = re.search(r'class="description-input">\s*(.*?)\s*</a>', row_html, re.DOTALL)
    if desc_match:
        description = _html.unescape(re.sub(r"\s+", " ", desc_match.group(1)).strip())

    total_cost_match = _TOTAL_COST_RE.search(sections.get("Smart Acceleration Summary", ""))
    vanilla_cost = _to_float(total_cost_match.group(1)) if total_cost_match else None
    dpa_s_cost = _to_float(total_cost_match.group(2)) if total_cost_match else None

    return NativeEngineUiQuerySummary(
        execution_id=execution_id,
        description=description,
        num_gluten_nodes=num_gluten_nodes,
        num_fallback_nodes=num_fallback_nodes,
        fallback_summary_text=fallback_summary_text,
        planning_overhead=_parse_planning_overhead(sections.get("Planning Time Breakdown", "")),
        has_cost_decision="Smart Acceleration Summary" in sections,
        vanilla_cost=vanilla_cost,
        dpa_s_cost=dpa_s_cost,
        dpas_plan_cost_breakdown=_parse_plan_cost_nodes(sections.get("DPA-S Plan", "")),
        subquery_cost_text=sections.get("Subquery Cost Evaluations"),
    )


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_native_engine_cost_summary(
    app_id: str,
    execution_id: int,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    *,
    ctx: Context,
) -> Optional[NativeEngineUiQuerySummary]:
    """
    Gluten/DPAS fallback-and-cost data for ONE SQL execution, read directly
    from the History Server's own ``/{app_id}/dpas/`` UI page instead of
    downloading and grepping the raw event log.

    This is the fast path for the same class of question
    get_query_fallback_summary/diagnose_native_execution answer from the raw
    event log (did this query fall back, and to which cost?) -- one small
    HTML page fetch, parsed down to just the requested execution_id, rather
    than downloading/decompressing a potentially large event log file. It
    covers less ground than those tools (no native-validation-memo stats,
    no executed-plan-native-node cross-check) -- use this first, and fall
    back to the raw-event-log tools when you need what only they have.

    Returns None if the app has no Gluten/DPAS instrumentation at all (the
    ``/dpas/`` page has no matching row for a plain-vanilla-Spark app) or
    if execution_id doesn't exist. ``has_cost_decision=False`` on a returned
    summary means Gluten was active but this particular query never
    triggered a cost comparison (e.g. a trivial DDL statement) -- distinct
    from no Gluten instrumentation at all.

    Args:
        app_id: The Spark application ID
        execution_id: The SQL execution ID to look up
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID

    Returns:
        NativeEngineUiQuerySummary, or None if not found
    """
    client = get_client_or_default(ctx, server, app_id)
    try:
        page = _fetch_ui_page(client, app_id, "dpas", app_attempt_id)
    except UiTabUnavailableError:
        return _cost_summary_without_dpas_tab(
            client, app_id, execution_id, server, app_attempt_id, ctx
        )
    for row_html in _iter_ui_table_rows(page):
        if f"?id={execution_id}" not in row_html and f"?id={execution_id}&" not in row_html:
            # Cheap pre-check before the fuller regex parse below -- most
            # rows aren't the one being asked for.
            if not re.search(rf"SQL/execution/\?id={execution_id}(?:&|\")", row_html):
                continue
        summary = _parse_dpas_row(row_html)
        if summary is not None and summary.execution_id == execution_id:
            return summary
    return None


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_gluten_build_info(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    *,
    ctx: Context,
) -> Optional[GlutenBuildInfo]:
    """
    The "Gluten Build Information" table from the History Server's own
    ``/{app_id}/gluten/`` UI tab (read from its ``/gluten/json/`` endpoint
    when available, else parsed from the HTML page) (Velox/DPAS revision, product name/
    version, Spark/Scala/Java versions) -- per-app, not per-query.

    Returns None if the app has no Gluten instrumentation (the /gluten/
    page renders no build-info table for a plain-vanilla-Spark app).

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID

    Returns:
        GlutenBuildInfo, or None if not found
    """
    client = get_client_or_default(ctx, server, app_id)
    summary = _gluten_ui_summary_or_none(client, app_id, app_attempt_id)
    if summary is not None:
        build = {str(k): str(v) for k, v in (summary.get("buildInfo") or {}).items()}
        return GlutenBuildInfo(app_id=app_id, fields=build) if build else None
    page = _fetch_ui_page(client, app_id, "gluten", app_attempt_id)
    match = re.search(r"<tbody>(.*?)</tbody>", page, re.DOTALL)
    if not match:
        return None
    fields: Dict[str, str] = {}
    for row in re.split(r"</tr>\s*<tr>", match.group(1)):
        cells = re.findall(r"<td[^>]*>\s*(.*?)\s*</td>", row, re.DOTALL)
        if len(cells) == 2:
            key = _html.unescape(re.sub(r"\s+", " ", cells[0]).strip())
            value = _html.unescape(re.sub(r"\s+", " ", cells[1]).strip())
            if key:
                fields[key] = value
    if not fields:
        return None
    return GlutenBuildInfo(app_id=app_id, fields=fields)


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def diagnose_sql_execution_failures(
    app_id: str,
    execution_id: int,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    verbose: bool = False,
    *,
    ctx: Context,
) -> SqlExecutionDiagnosis:
    """
    One-call diagnosis for a single SQL execution, answering both "did this
    fail at the job/stage/task level, and why" and "did it fall back from
    native execution, and why" -- the two questions this session answered
    by hand, separately, for two real incidents:

    * A GKE SparkApplication whose driver and executors were OOMKilled
      (ExitCode 137), cascading into a driver failure -- a job/stage/task
      -level failure with Gluten disabled entirely (no native-engine data
      involved at all).
    * A Dataproc Serverless batch whose task failed exactly 4 times with
      BigQuery's ``RESOURCE_EXHAUSTED: Quota exceeded ... Read API data
      plane requests`` -- exhausting Spark's default task-retry budget
      (``spark.task.maxFailures``) and aborting the stage, with the real
      cause buried in one Lost-task exception's text, not any top-level
      error field.

    Chains three existing tools instead of requiring a caller to run them
    one at a time and reason about the results by hand:
      1. ``get_sql_execution(..., include_stages=True)`` for this
         execution's stages.
      2. ``list_stage_task_failures`` for every stage with a FAILED status
         or any failed tasks, to get the real per-task exception/stack
         trace text (not just "FAILED").
      3. ``get_native_engine_cost_summary`` for this execution's Gluten/DPAS
         fallback-and-cost data, gated on ``get_gluten_build_info`` showing
         the app has native-engine instrumentation at all.

    By default (``verbose=False``) only failed stages/tasks are returned,
    not the full stage list, and no physical-plan text -- most callers only
    want to know what broke, and pulling every stage back at full token
    cost when 1 of 40 failed is rarely what's wanted. Pass ``verbose=True``
    for the full stage list and the full native-engine plan text too.

    ``native_engine_available`` is a first-class, unambiguous per-APP signal
    for "is there any Gluten/DPAS data here at all" -- False for a plain
    vanilla-Spark run where the plugin was never loaded. This is
    deliberately distinct from ``native_engine_summary`` being absent,
    which can *also* happen for a Gluten-enabled app whose specific query
    never triggered a cost decision (a trivial statement), or an
    execution_id that doesn't exist. A missing ``native_engine_summary``
    with ``native_engine_available=True`` means "Gluten was active, this
    query just didn't need a cost decision" -- NOT "no data was found," so
    never read that absence as evidence of anything by itself; always check
    ``native_engine_available`` first.

    Args:
        app_id: The Spark application ID
        execution_id: The SQL execution ID to diagnose
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID
        verbose: If True, include every stage (not just failed ones) and
            the full native-engine physical-plan text

    Returns:
        SqlExecutionDiagnosis
    """
    detail = get_sql_execution(
        app_id,
        execution_id,
        server=server,
        app_attempt_id=app_attempt_id,
        include_stages=True,
        include_plan=verbose,
        ctx=ctx,
    )

    failed_stages: List[FailedStageDiagnosis] = []
    for stage in detail.stages or []:
        if stage.status != "FAILED" and not (stage.num_failed_tasks or 0):
            continue
        try:
            failures = list_stage_task_failures(
                app_id,
                stage.stage_id,
                server=server,
                app_attempt_id=app_attempt_id,
                ctx=ctx,
            )
        except ValueError:
            # Stage vanished (e.g. skipped by AQE) between the include_stages
            # listing above and this lookup -- nothing to report for it.
            failures = []
        failed_stages.append(
            FailedStageDiagnosis(
                stage_id=stage.stage_id,
                status=stage.status,
                num_failed_tasks=stage.num_failed_tasks,
                failed_tasks=failures,
            )
        )

    # Per-app signal, checked before the per-query one below -- see the
    # native_engine_available docstring note on why these must not be
    # conflated.
    build_info = get_gluten_build_info(
        app_id, server=server, app_attempt_id=app_attempt_id,
        ctx=ctx,
    )
    native_engine_available = build_info is not None
    native_summary = None
    if native_engine_available:
        native_summary = get_native_engine_cost_summary(
            app_id, execution_id, server=server, app_attempt_id=app_attempt_id,
            ctx=ctx,
        )

    return SqlExecutionDiagnosis(
        app_id=app_id,
        execution_id=execution_id,
        status=detail.execution.status,
        duration=detail.execution.duration,
        has_stage_failures=bool(failed_stages),
        failed_stages=failed_stages,
        all_stages=detail.stages if verbose else None,
        native_engine_available=native_engine_available,
        native_engine_summary=native_summary,
    )


def _gluten_ui_summary_or_none(
    client, app_id: str, app_attempt_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    """The Gluten UI plugin's /gluten/json/ summary, or None if unavailable
    (plugin not installed, older server, or any transport/parse error) --
    callers then fall back to the HTML page or the raw event log."""
    getter = getattr(client, "get_gluten_ui_summary", None)
    if getter is None:
        return None
    try:
        return getter(app_id, app_attempt_id=app_attempt_id)
    except Exception:
        return None


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_gluten_fallback_reasons(
    app_id: str,
    server: Optional[str] = None,
    execution_id: Optional[int] = None,
    app_attempt_id: Optional[str] = None,
    *,
    ctx: Context,
) -> GlutenFallbackReasons:
    """
    Per-SQL-execution Gluten fallback reasons, cheaply.

    Reads the Gluten History Server plugin's ``/gluten/json/`` endpoint
    (native vs fallback node counts and the reason each fallback node fell
    back, plus Gluten build info) -- no raw event-log download. Only when
    that endpoint is unavailable does it fall back to scanning the (cached)
    raw event log for ``GlutenPlanFallbackEvent`` records. ``source`` in the
    result says which path was used.

    Use this for "why did query X fall back"; use get_query_fallback_summary
    when you also need the FQF cost decision (vanilla vs Gluten plan cost).

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        execution_id: Optional SQL execution ID to return only that query
        app_attempt_id: Optional application attempt ID

    Returns:
        GlutenFallbackReasons with per-execution fallback details
    """
    client = get_client_or_default(ctx, server, app_id)
    summary = _gluten_ui_summary_or_none(client, app_id, app_attempt_id)
    if summary is not None:
        executions = []
        for e in (summary.get("queries") or {}).get("executions") or []:
            eid = _int_or_none(e.get("executionId"))
            if execution_id is not None and eid != execution_id:
                continue
            executions.append(
                GlutenExecutionFallback(
                    execution_id=eid,
                    description=e.get("description") or None,
                    num_gluten_nodes=_int_or_none(e.get("numGlutenNodes")),
                    num_fallback_nodes=_int_or_none(e.get("numFallbackNodes")),
                    fallback_description=e.get("fallbackDescription") or None,
                    fallback_node_to_reason={
                        str(k): str(v)
                        for k, v in (e.get("fallbackNodeToReason") or {}).items()
                    },
                )
            )
        return GlutenFallbackReasons(
            app_id=app_id,
            source="history_server_plugin",
            build_info={
                str(k): str(v) for k, v in (summary.get("buildInfo") or {}).items()
            },
            executions=executions,
        )

    by_exec: Dict[int, GlutenExecutionFallback] = {}
    zip_bytes = download_event_log_cached(client, app_id, app_attempt_id)
    for record in iter_event_log_records(zip_bytes):
        if record.get("Event") != _FALLBACK_EVENT:
            continue
        eid = _int_or_none(record.get("executionId"))
        if eid is None or (execution_id is not None and eid != execution_id):
            continue
        entry = by_exec.setdefault(eid, GlutenExecutionFallback(execution_id=eid))
        # A query emits one fallback event per (re-)plan -- e.g. the FQF
        # all-vanilla candidate, then each AQE re-plan. The Gluten UI plugin
        # keeps only the latest (each event overwrites the stored row), so do
        # the same: the last event describes the plan that actually ran, and
        # its reasons line up with its numFallbackNodes.
        entry.num_gluten_nodes = _int_or_none(record.get("numGlutenNodes"))
        entry.num_fallback_nodes = _int_or_none(record.get("numFallbackNodes"))
        entry.fallback_node_to_reason = {
            str(k): str(v) for k, v in (record.get("fallbackNodeToReason") or {}).items()
        }
    return GlutenFallbackReasons(
        app_id=app_id,
        source="event_log",
        executions=[by_exec[k] for k in sorted(by_exec)],
    )
