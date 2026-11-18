"""Workflow-level tools built from how this server is actually used.

Usage history showed two loops dominating sessions:

* "Which server/app is this?" -- many list_applications calls, one server at
  a time, each returning verbose attempt objects (~150 tokens per app).
  ``find_applications`` searches every configured server in one call and
  returns one compact row per app.
* "Did the fallback hurt because executors were sized for native?" --
  ``check_executor_memory_gc``.
* "Give me the per-query pass/fail/native table for the report" --
  ``query_suite_report``.
* "What config changed across rc-1..rc-N?" -- ``config_changes_across_runs``.
* "How does run B compare to run A, query by query?" (VSpark vs DPAS, rc-N vs
  rc-N+1, deb11 vs deb12) -- many list_sql_executions + compare_sql_executions
  calls. ``compare_benchmark_runs`` pairs every SQL execution of two apps by
  description and returns only the summary plus the biggest movers, each
  annotated with whether it ran natively (from the cheap /gluten/json
  endpoint, no event-log download).
"""

import math
import re
from typing import Any, Dict, List, Optional

from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations

from spark_history_mcp.core.app import tool
from spark_history_mcp.tools.regression import _ENV_NOISE_KEY_RE
from spark_history_mcp.tools.tools import (
    _fetch_all_sql_executions,
    get_client_or_default,
)


def _attempt_row(server_name: str, app) -> Dict[str, Any]:
    attempts = getattr(app, "attempts", None) or []
    a = attempts[0] if attempts else None  # History Server lists newest first
    return {
        "server": server_name,
        "id": app.id,
        "name": app.name,
        "start": str(a.start_time) if a and a.start_time else None,
        "duration_s": round(a.duration / 1000, 1) if a and a.duration else None,
        "completed": a.completed if a else None,
        "spark": a.app_spark_version if a else None,
        "attempts": len(attempts),
    }


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def find_applications(
    name_contains: Optional[str] = None,
    servers: Optional[List[str]] = None,
    status: Optional[List[str]] = None,
    min_date: Optional[str] = None,
    limit_per_server: int = 20,
    *,
    ctx: Context,
) -> List[Dict[str, Any]]:
    """
    Search applications across ALL configured History Servers at once,
    returning one compact row per app (server, id, name, start, duration_s,
    completed, spark version, attempt count) -- ~5x fewer tokens than
    list_applications and no need to know which server an app lives on.

    Args:
        name_contains: Optional case-insensitive substring of the app name
        servers: Optional server names to search (default: every configured server)
        status: Optional ["completed"] or ["running"]
        min_date: Optional earliest start date, e.g. "2026-11-01"
        limit_per_server: Max matching apps returned per server (default 20).
            With name_contains every app on the server is scanned, so older
            matches aren't missed. Servers sharing a URL (e.g. a default
            "local" alias) are searched once.

    Returns:
        Matching apps, newest first within each server
    """
    clients = ctx.request_context.lifespan_context.clients
    # Named servers before the generic default alias, so a duplicate URL is
    # reported under its meaningful name.
    names = servers or sorted(clients, key=lambda n: n == "local")
    needle = (name_contains or "").lower()
    rows: List[Dict[str, Any]] = []
    seen_urls: set = set()
    for name in names:
        client = clients.get(name)
        if client is None:
            rows.append({"server": name, "error": "unknown server"})
            continue
        url = getattr(getattr(client, "config", None), "url", None)
        if isinstance(url, str):
            if url.rstrip("/") in seen_urls:
                continue
            seen_urls.add(url.rstrip("/"))
        try:
            apps = client.list_applications(
                status=status,
                min_date=min_date,
                limit=None if needle else limit_per_server,
            )
        except Exception as exc:  # one unreachable server shouldn't hide the rest
            rows.append({"server": name, "error": f"{type(exc).__name__}: {exc}"[:200]})
            continue
        matched = [a for a in apps if needle in (a.name or "").lower()]
        rows.extend(_attempt_row(name, a) for a in matched[:limit_per_server])
    return rows


def _retention_warning(app_id: str, execs) -> Optional[str]:
    """SQL execution ids start at 0 in every app; if the lowest id the
    History Server still returns is higher, older executions were evicted
    (spark.sql.ui.retainedExecutions, default 1000) and are invisible."""
    ids = [e.id for e in execs if e.id is not None]
    if ids and min(ids) > 0:
        return (
            f"{app_id}: executions 0..{min(ids) - 1} were evicted by the History "
            f"Server (only {len(ids)} retained; spark.sql.ui.retainedExecutions) -- "
            "results cover the retained ones only"
        )
    return None


def _norm_desc(desc: Optional[str]) -> str:
    return " ".join((desc or "").split()).lower()


_REASON_NUM_RE = re.compile(r"[-+]?\d[\d.,]*(?:[eE][-+]?\d+)?|Infinity")


def _reason_kind(reason: str) -> str:
    """Group reasons that differ only in numbers (costs, ids)."""
    return _REASON_NUM_RE.sub("#", reason or "").strip()[:160]


def _public(native: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return {k: v for k, v in native.items() if not k.startswith("_")} if native else None


def _native_diff(pairs, n1, n2, top_n: int) -> Dict[str, Any]:
    """Per paired query: did native coverage change, and which fallback
    reason kinds appeared/disappeared between the two runs."""
    became_fallback, became_native = [], []
    kinds1: Dict[str, int] = {}
    kinds2: Dict[str, int] = {}
    for a, b in pairs:
        x, y = n1.get(a.id), n2.get(b.id)
        if not x or not y:
            continue
        for r in x["_reasons"].values():
            k = _reason_kind(r)
            kinds1[k] = kinds1.get(k, 0) + 1
        for r in y["_reasons"].values():
            k = _reason_kind(r)
            kinds2[k] = kinds2.get(k, 0) + 1
        f1, f2 = x.get("fallback_nodes") or 0, y.get("fallback_nodes") or 0
        row = {"description": (a.description or "")[:120], "exec_ids": [a.id, b.id],
               "fallback_nodes": [f1, f2], "gluten_nodes": [x.get("gluten_nodes"), y.get("gluten_nodes")]}
        if f2 > f1:
            new = {_reason_kind(r) for r in y["_reasons"].values()} - {
                _reason_kind(r) for r in x["_reasons"].values()}
            row["new_reasons"] = sorted(new)[:3]
            became_fallback.append(row)
        elif f2 < f1:
            became_native.append(row)
    key = lambda r: -abs(r["fallback_nodes"][1] - r["fallback_nodes"][0])  # noqa: E731
    return {
        "queries_more_fallback": len(became_fallback),
        "queries_less_fallback": len(became_native),
        "top_more_fallback": sorted(became_fallback, key=key)[:top_n],
        "top_less_fallback": sorted(became_native, key=key)[:top_n],
        "reason_kinds_only_in_baseline": {k: v for k, v in kinds1.items() if k not in kinds2},
        "reason_kinds_only_in_candidate": {k: v for k, v in kinds2.items() if k not in kinds1},
    }


def _native_by_exec(client, app_id: str) -> Dict[int, Dict[str, Any]]:
    """execution_id -> {gluten, fallback} node counts from /gluten/json; {} if unavailable."""
    try:
        summary = client.get_gluten_ui_summary(app_id)
    except Exception:
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for e in ((summary or {}).get("queries") or {}).get("executions") or []:
        try:
            out[int(e.get("executionId"))] = {
                "gluten_nodes": e.get("numGlutenNodes"),
                "fallback_nodes": e.get("numFallbackNodes"),
                "_reasons": e.get("fallbackNodeToReason") or {},
            }
        except (TypeError, ValueError):
            continue
    return out


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def compare_benchmark_runs(
    app_id1: str,
    app_id2: str,
    server1: Optional[str] = None,
    server2: Optional[str] = None,
    threshold_pct: float = 10.0,
    top_n: int = 10,
    min_duration_ms: int = 500,
    include_native_diff: bool = True,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Compare two runs of the same workload query-by-query (e.g. VSpark vs
    DPAS, rc-N vs rc-N+1, deb11 vs deb12) in ONE call.

    Pairs every SQL execution of app 1 with app 2 by normalized description
    (the i-th occurrence of a repeated description pairs with the i-th), then
    returns: totals, the geometric-mean speed ratio, counts of regressions /
    improvements beyond ``threshold_pct``, and only the ``top_n`` biggest
    regressions and improvements -- each with both durations and native
    node counts per side (from the Gluten plugin's /gluten/json; no event-log
    download). Drill into a flagged query with diagnose_native_execution
    (pass its description) or compare_sql_executions.

    Args:
        app_id1: Baseline application ID
        app_id2: Candidate application ID
        server1: Server for app_id1 (default server if omitted)
        server2: Server for app_id2 (defaults to server1)
        threshold_pct: % slowdown/speedup to count as a regression/improvement (default 10)
        top_n: How many top regressions and improvements to list (default 10)
        min_duration_ms: Ignore pairs where both sides ran faster than this
            -- sub-second queries make ratios noisy (default 500)
        include_native_diff: Also report queries whose fallback-node count
            changed between runs and fallback reason kinds (numbers
            stripped) that exist in only one run -- "did the new build
            introduce or remove fallbacks?" (default True; two small
            /gluten/json fetches)

    Returns:
        Summary dict with totals, ratio stats and the top movers
    """
    c1 = get_client_or_default(ctx, server1, app_id1)
    c2 = get_client_or_default(ctx, server2 or server1, app_id2)
    e1 = _fetch_all_sql_executions(c1, app_id1)
    e2 = _fetch_all_sql_executions(c2, app_id2)
    warnings = [w for w in (_retention_warning(app_id1, e1), _retention_warning(app_id2, e2)) if w]

    def by_desc(execs) -> Dict[tuple, Any]:
        seen: Dict[str, int] = {}
        out: Dict[tuple, Any] = {}
        for e in sorted(execs, key=lambda x: x.id if x.id is not None else -1):
            d = _norm_desc(e.description)
            n = seen.get(d, 0)
            seen[d] = n + 1
            out[(d, n)] = e
        return out

    m1, m2 = by_desc(e1), by_desc(e2)
    pairs = [(m1[k], m2[k]) for k in m1 if k in m2]

    rows: List[Dict[str, Any]] = []
    log_ratios: List[float] = []
    skipped_fast = 0
    for a, b in pairs:
        d1, d2 = a.duration or 0, b.duration or 0
        if max(d1, d2) < min_duration_ms:
            skipped_fast += 1
            continue
        if d1 <= 0 or d2 <= 0:
            continue
        ratio = d2 / d1
        log_ratios.append(math.log(ratio))
        rows.append(
            {
                "description": (a.description or "")[:120],
                "exec_ids": [a.id, b.id],
                "ms": [d1, d2],
                "ratio": round(ratio, 3),
                "status": [a.status, b.status],
            }
        )

    thr = 1 + threshold_pct / 100.0
    regressions = sorted((r for r in rows if r["ratio"] > thr), key=lambda r: -r["ratio"])
    improvements = sorted((r for r in rows if r["ratio"] < 1 / thr), key=lambda r: r["ratio"])

    top_reg, top_imp = regressions[:top_n], improvements[:top_n]
    n1 = n2 = None
    if top_reg or top_imp or include_native_diff:
        n1, n2 = _native_by_exec(c1, app_id1), _native_by_exec(c2, app_id2)
        for r in (*top_reg, *top_imp):
            i1, i2 = r["exec_ids"]
            r["native"] = [_public(n1.get(i1)), _public(n2.get(i2))]

    total1 = sum(r["ms"][0] for r in rows)
    total2 = sum(r["ms"][1] for r in rows)
    failed = [
        {"description": (a.description or "")[:120], "status": [a.status, b.status]}
        for a, b in pairs
        if (a.status or "").upper() != (b.status or "").upper()
    ]
    result = {
        "apps": {"baseline": [server1, app_id1], "candidate": [server2 or server1, app_id2]},
        "executions": {
            "baseline": len(e1),
            "candidate": len(e2),
            "paired": len(pairs),
            "compared": len(rows),
            "skipped_under_min_duration": skipped_fast,
            "only_in_baseline": len(m1.keys() - m2.keys()),
            "only_in_candidate": len(m2.keys() - m1.keys()),
        },
        "total_ms": [total1, total2],
        "total_ratio": round(total2 / total1, 3) if total1 else None,
        "geomean_ratio": round(math.exp(sum(log_ratios) / len(log_ratios)), 3)
        if log_ratios
        else None,
        "regressions": len(regressions),
        "improvements": len(improvements),
        "status_mismatches": failed[:top_n],
        "top_regressions": top_reg,
        "top_improvements": top_imp,
    }
    if warnings:
        result["warnings"] = warnings
    if include_native_diff:
        result["native_diff"] = (
            _native_diff(pairs, n1, n2, top_n) if n1 and n2
            else "unavailable (no /gluten/json on one side)"
        )
    return result


# ---------------------------------------------------------------------------
# Executor memory vs GC

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgtp]?)b?\s*$", re.I)
_SIZE_MULT = {"": 1, "k": 2**10, "m": 2**20, "g": 2**30, "t": 2**40, "p": 2**50}


def _size_mb(value: Optional[str], default_unit: str = "") -> Optional[float]:
    """Spark size string ("4g", "4096m", "512k", "1073741824") -> MiB."""
    if not value:
        return None
    m = _SIZE_RE.match(str(value))
    if not m:
        return None
    unit = (m.group(2) or default_unit).lower()
    return round(float(m.group(1)) * _SIZE_MULT[unit] / 2**20, 1)


def _classify(native: Optional[Dict[str, Any]]) -> str:
    if not native:
        return "unknown"
    g, f = native.get("gluten_nodes") or 0, native.get("fallback_nodes") or 0
    if g == 0 and f == 0:
        return "no_plan"  # SET/DDL/commands: nothing to offload
    if g == 0:
        return "vanilla"
    return "partial" if f else "native"


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def check_executor_memory_gc(
    app_id: str,
    server: Optional[str] = None,
    gc_ratio_threshold: float = 0.10,
    top_n: int = 5,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Check whether GC pressure is explained by executor memory sizing --
    the classic DPAS trap: executors sized for native execution (large
    off-heap, small JVM heap) that then run vanilla Spark after a fallback
    and spend their time in GC.

    One call: executor memory config (heap, off-heap, overhead), app-wide
    and per-executor GC share of task time, peak JVM heap / off-heap usage,
    the top stages by GC share, how many SQL executions ran vanilla /
    partially native (from /gluten/json), and a plain-language verdict.

    Args:
        app_id: The Spark application ID
        server: Optional server name (default server if omitted)
        gc_ratio_threshold: GC time / task time above which GC counts as
            pressure (default 0.10 = 10%)
        top_n: How many top-GC stages to list (default 5)

    Returns:
        Dict with memory_config, gc, top_gc_stages, executions, verdict
    """
    client = get_client_or_default(ctx, server, app_id)
    props = dict(client.get_environment(app_id=app_id).spark_properties or [])
    heap = _size_mb(props.get("spark.executor.memory", "1g"), "m")
    offheap_on = str(props.get("spark.memory.offHeap.enabled", "false")).lower() == "true"
    spark_offheap = _size_mb(props.get("spark.memory.offHeap.size"), "") if offheap_on else 0
    # Gluten records the native (Velox) memory it actually reserved here; with
    # dynamic sizing it is carved out of the JVM heap rather than added to it.
    gluten_native = _size_mb(props.get("spark.gluten.memory.offHeap.size.in.bytes"), "")
    dynamic = str(props.get("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "false")).lower() == "true"
    offheap = gluten_native or spark_offheap or 0
    overhead = _size_mb(props.get("spark.executor.memoryOverhead"), "m")
    memory_config = {
        "executor_heap_mb": heap,
        "native_memory_mb": offheap,
        "native_memory_from_heap": dynamic,
        "spark_offheap_enabled": offheap_on,
        "overhead_mb": overhead,
        "executor_cores": props.get("spark.executor.cores"),
        "native_to_heap": round(offheap / heap, 2) if heap and offheap else None,
    }

    executors = [e for e in client.list_all_executors(app_id=app_id) if e.id != "driver"]
    task_ms = sum(e.total_duration or 0 for e in executors)
    gc_ms = sum(e.total_gc_time or 0 for e in executors)
    peak_heap = max((e.peak_memory_metrics.jvm_heap_memory or 0 for e in executors
                     if e.peak_memory_metrics), default=0)
    peak_off = max((e.peak_memory_metrics.off_heap_execution_memory or 0 for e in executors
                    if e.peak_memory_metrics), default=0)
    worst = sorted(
        (e for e in executors if e.total_duration),
        key=lambda e: -(e.total_gc_time or 0) / e.total_duration,
    )[:3]
    gc = {
        "gc_ratio": round(gc_ms / task_ms, 3) if task_ms else None,
        "gc_ms": gc_ms,
        "task_ms": task_ms,
        "executors": len(executors),
        "peak_jvm_heap_mb": round(peak_heap / 2**20, 1),
        "peak_offheap_execution_mb": round(peak_off / 2**20, 1),
        "worst_executors": [
            {"id": e.id, "gc_ratio": round((e.total_gc_time or 0) / e.total_duration, 3)}
            for e in worst
        ],
    }

    seen, stages = set(), []
    for st in client.list_stages(app_id=app_id):
        if st.stage_id in seen or st.status == "SKIPPED":
            continue
        seen.add(st.stage_id)
        run = st.executor_run_time or 0
        if run >= 1000:
            stages.append((st, (st.jvm_gc_time or 0) / run))
    stages.sort(key=lambda t: -t[1])
    top_gc_stages = [
        {"stage_id": st.stage_id, "gc_ratio": round(r, 3), "gc_ms": st.jvm_gc_time,
         "run_ms": st.executor_run_time, "name": (st.name or "")[:80]}
        for st, r in stages[:top_n]
    ]

    native = _native_by_exec(client, app_id)
    kinds: Dict[str, int] = {}
    for n in native.values():
        k = _classify(n)
        kinds[k] = kinds.get(k, 0) + 1

    verdict = []
    ratio = gc["gc_ratio"] or 0
    # Dynamic sizing: native memory comes out of the heap, so >=50% leaves
    # vanilla/fallback work at most half the heap. Static: off-heap >= heap.
    sized_for_native = bool(offheap and heap and offheap >= heap * (0.5 if dynamic else 1.0))
    if ratio >= gc_ratio_threshold:
        verdict.append(f"GC pressure: {ratio:.0%} of executor task time is GC.")
    else:
        verdict.append(f"No app-wide GC pressure ({ratio:.0%} of task time).")
    if sized_for_native:
        verdict.append(
            f"Executors are sized for native execution: {offheap:.0f} MiB native memory "
            + (f"carved out of a {heap:.0f} MiB heap." if dynamic else f"vs {heap:.0f} MiB JVM heap.")
        )
    if kinds.get("vanilla") or kinds.get("partial"):
        verdict.append(
            f"{kinds.get('vanilla', 0)} execution(s) ran fully vanilla and "
            f"{kinds.get('partial', 0)} partially native."
        )
    if sized_for_native and ratio >= gc_ratio_threshold and (kinds.get("vanilla") or kinds.get("partial")):
        verdict.append(
            "Likely cause: vanilla/fallback work running on a small JVM heap meant "
            "for native execution. Compare against a VSpark baseline's heap size "
            "and GC, or rebalance heap vs off-heap for fallback-heavy workloads."
        )
    if top_gc_stages and top_gc_stages[0]["gc_ratio"] >= gc_ratio_threshold * 2:
        t = top_gc_stages[0]
        verdict.append(f"Worst stage {t['stage_id']}: {t['gc_ratio']:.0%} GC.")
    return {
        "app_id": app_id,
        "memory_config": memory_config,
        "gc": gc,
        "top_gc_stages": top_gc_stages,
        "executions": kinds or "unavailable (no /gluten/json)",
        "verdict": " ".join(verdict),
    }


# ---------------------------------------------------------------------------
# Query suite report


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def query_suite_report(
    app_id: str,
    server: Optional[str] = None,
    description_contains: Optional[str] = None,
    only: Optional[str] = None,
    min_duration_ms: int = 0,
    markdown: bool = False,
    limit: int = 200,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    One row per SQL execution -- id, status, duration, engine (native /
    partial / vanilla from /gluten/json) and a short description -- plus
    totals, for benchmark/test-suite reports. Rows are compact lists
    (header given once); ``markdown=True`` returns a ready-to-paste table
    instead.

    Args:
        app_id: The Spark application ID
        server: Optional server name (default server if omitted)
        description_contains: Optional case-insensitive filter on the description
        only: Optional row filter: "failed", "vanilla", "partial", "native", or
            "not_native" (vanilla + partial; excludes no_plan commands like SET)
        min_duration_ms: Skip executions shorter than this (default 0)
        markdown: Return rows as a markdown table string (default False)
        limit: Max rows returned (default 200); totals always cover everything

    Returns:
        Dict with totals (by status and engine, total ms) and rows
    """
    if only not in (None, "failed", "vanilla", "partial", "native", "not_native"):
        raise ValueError("only must be failed, vanilla, partial, native or not_native")
    client = get_client_or_default(ctx, server, app_id)
    execs = sorted(_fetch_all_sql_executions(client, app_id), key=lambda e: e.id or 0)
    native = _native_by_exec(client, app_id)
    needle = (description_contains or "").lower()

    rows, by_status, by_engine, total_ms = [], {}, {}, 0
    for e in execs:
        desc = " ".join((e.description or "").split())
        if needle and needle not in desc.lower():
            continue
        if (e.duration or 0) < min_duration_ms:
            continue
        engine = _classify(native.get(e.id))
        status = (e.status or "").upper()
        by_status[status] = by_status.get(status, 0) + 1
        by_engine[engine] = by_engine.get(engine, 0) + 1
        total_ms += e.duration or 0
        if only == "failed" and status != "FAILED":
            continue
        if only == "not_native" and engine not in ("vanilla", "partial"):
            continue
        if only in ("vanilla", "partial", "native") and engine != only:
            continue
        rows.append([e.id, status, e.duration, engine, desc[:80]])

    header = ["id", "status", "ms", "engine", "description"]
    out: Dict[str, Any] = {
        "app_id": app_id,
        "totals": {"executions": sum(by_status.values()), "total_ms": total_ms,
                   "by_status": by_status, "by_engine": by_engine},
        "rows_matched": len(rows),
    }
    warning = _retention_warning(app_id, execs)
    if warning:
        out["warning"] = warning
    rows = rows[:limit]
    if markdown:
        lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        lines += ["| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |" for r in rows]
        out["table"] = "\n".join(lines)
    else:
        out["header"] = header
        out["rows"] = rows
    return out


# ---------------------------------------------------------------------------
# Config changes across a series of runs


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def config_changes_across_runs(
    app_ids: List[str],
    servers: Optional[List[Optional[str]]] = None,
    prefixes: Optional[List[str]] = None,
    key_filter: Optional[str] = None,
    *,
    ctx: Context,
) -> Dict[str, Any]:
    """
    Across a series of runs (rc-1, rc-2, ... or deb11 vs deb12 vs ...),
    return ONLY the Spark properties whose value differs in at least one
    run, as {key: [value per run]} (None = unset). Run-identity noise
    (app ids, hosts, ports, log dirs, yarn tags) is dropped.

    Args:
        app_ids: Application IDs in run order (2 or more)
        servers: Optional server per app_id (None entries = default server)
        prefixes: Key prefixes to consider (default: spark.gluten., spark.dataproc.,
            spark.executor., spark.memory., spark.sql., spark.driver.memory)
        key_filter: Optional extra case-insensitive substring on the key

    Returns:
        Dict with runs and changed keys
    """
    if len(app_ids) < 2:
        raise ValueError("need at least 2 app_ids")
    if servers is not None and len(servers) != len(app_ids):
        raise ValueError("servers must have one entry per app_id")
    prefixes = prefixes or ["spark.gluten.", "spark.dataproc.", "spark.executor.",
                            "spark.memory.", "spark.sql.", "spark.driver.memory"]
    kf = (key_filter or "").lower()
    props = []
    for i, app_id in enumerate(app_ids):
        srv = servers[i] if servers else None
        client = get_client_or_default(ctx, srv, app_id)
        env = dict(client.get_environment(app_id=app_id).spark_properties or [])
        props.append({
            k: v for k, v in env.items()
            if any(k.startswith(p) for p in prefixes)
            and not _ENV_NOISE_KEY_RE.search(k)
            and kf in k.lower()
        })
    keys = sorted(set().union(*props))
    changed: Dict[str, Any] = {}
    for k in keys:
        vals = [p.get(k) for p in props]
        if len(set(vals)) <= 1:
            continue
        if max(len(v or "") for v in vals) > 120:
            # Long option strings (extraJavaOptions...): only the differing
            # space-separated options, relative to the first run.
            base = set((vals[0] or "").split())
            changed[k] = {"diff_vs_run_0": [
                None if v is None else {
                    "added": sorted(set(v.split()) - base)[:10],
                    "removed": sorted(base - set(v.split()))[:10],
                }
                for v in vals[1:]
            ]}
        else:
            changed[k] = vals
    return {
        "runs": [[servers[i] if servers else None, a] for i, a in enumerate(app_ids)],
        "keys_compared": len(keys),
        "changed": changed,
    }
