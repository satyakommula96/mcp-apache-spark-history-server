"""Multi-app regression diagnosis.

Codifies the manual investigation pattern this project's own regression
work follows by hand (see skills/dpas-fallback-check and the
compare-workflow examples under skills/cli/examples/compare): match each
app's SQL execution, compare duration/stage-time/GC against a baseline,
diff non-infra environment config, run the DPAS native-vs-vanilla
diagnosis, and synthesize a findings narrative -- one call instead of a
long manual chain of list_sql_executions/compare_job_environments/
compare_stages/diagnose_native_execution per app pair.
"""

from mcp.types import ToolAnnotations
from mcp.server.mcpserver import Context
import re
from typing import Dict, List, Optional

from spark_history_mcp.api_client.models.sql_execution import SQLExecution
from spark_history_mcp.core.app import tool
from spark_history_mcp.models.mcp_types import RegressionAppMetrics, RegressionDiagnosis
from spark_history_mcp.tools.native_engine import diagnose_native_execution
from spark_history_mcp.tools.tools import (
    _collect_stage_side,
    _jobs_and_stage_agg_for_executions,
    get_client_or_default,
    list_sql_executions,
)

# Spark property keys that vary between any two runs purely because of
# where/when they happened (ports, hostnames, run ids, timestamps) rather
# than because of anything that affects execution -- filtered out of
# material_env_diffs so the signal isn't buried in noise. Deliberately
# narrow: spark.app.name, spark.gluten.dp.sw.location and similar labels
# that reveal *which build/config ran* are kept, even though they're also
# run identifiers, since that's usually exactly what a reader needs to see.
_ENV_NOISE_KEY_RE = re.compile(
    r"^spark\.app\.(id|startTime|submitTime|initial\.(file|jar)\.urls)$"
    r"|^spark\.driver\.(host|port|appUIAddress)$"
    r"|^spark\.dataproc\.autotuning\.cohortId$"
    r"|^spark\.history\.fs\.logDirectory$"
    r"|^spark\.master$"
    r"|^spark\.dataproc\.metrics\.listener\.metrics\.collector\.hostname$"
    r"|^spark\.yarn\.tags$"
    r"|^spark\.metrics\.namespace$"
    r"|^spark\.eventLog\.dir$"
    r"|\.PROXY_(HOSTS|URI_BASES)$"
)


def _material_env_diff(
    baseline_props: Dict[str, str], candidate_props: Dict[str, str]
) -> Dict[str, Dict[str, Optional[str]]]:
    diffs: Dict[str, Dict[str, Optional[str]]] = {}
    for key in sorted(set(baseline_props) | set(candidate_props)):
        if _ENV_NOISE_KEY_RE.search(key):
            continue
        baseline_value = baseline_props.get(key)
        candidate_value = candidate_props.get(key)
        if baseline_value != candidate_value:
            diffs[key] = {"baseline": baseline_value, "candidate": candidate_value}
    return diffs


def _pct_change(baseline: Optional[float], candidate: Optional[float]) -> Optional[float]:
    if baseline is None or candidate is None or baseline == 0:
        return None
    return (candidate - baseline) / baseline * 100.0


def _resolve_execution_group(
    client,
    app_id: str,
    execution_id: Optional[int],
    description: Optional[str],
    *,
    ctx: Context,
    server: Optional[str] = None,
) -> tuple:
    """Find the SQL execution(s) matching ``execution_id``/``description``.

    The "primary" execution (the one whose duration/description heads the
    report) is whichever match has the greatest duration -- an outer
    wrapper command's elapsed time necessarily includes any sibling/child
    execution it triggers, so it's always the longest match within a
    matching group. All matches are returned too, for pooling jobs/stages.
    """
    if execution_id is not None:
        ids = [execution_id]
    else:
        if not description:
            raise ValueError(
                f"No execution_id given for {app_id} and no description filter provided"
            )
        candidates = list_sql_executions(
            app_id, server=server, description=description, limit=0, ctx=ctx
        )
        ids = [c.id for c in candidates if c.id is not None]
        if not ids:
            raise ValueError(
                f"No SQL executions found for {app_id} matching description {description!r}"
            )

    executions: List[SQLExecution] = [
        client.get_sql_execution(app_id, eid, details=False, plan_description=False)
        for eid in ids
    ]
    primary = max(executions, key=lambda e: e.duration or 0)
    return primary, executions


def _findings_for(
    metrics: RegressionAppMetrics,
    baseline: RegressionAppMetrics,
    duration_pct: Optional[float],
    gc_pct: Optional[float],
    env_diffs: Dict[str, Dict[str, Optional[str]]],
) -> List[str]:
    label = metrics.label or metrics.app_id
    out: List[str] = []

    if duration_pct is not None and abs(duration_pct) >= 3:
        direction = "slower" if duration_pct > 0 else "faster"
        out.append(
            f"{label}: {abs(duration_pct):.1f}% {direction} than baseline "
            f"({metrics.duration_ms}ms vs {baseline.duration_ms}ms)."
        )
        if gc_pct is not None and duration_pct > 0 and gc_pct > duration_pct * 1.5:
            out.append(
                f"{label}: cumulative JVM GC time grew {gc_pct:.1f}% -- "
                f"disproportionate to the {duration_pct:.1f}% duration "
                "regression, and worth checking as a primary driver "
                "(executor on-heap vs off-heap memory sizing)."
            )

    if (
        metrics.unattributed_ms
        and metrics.unattributed_ms > 0
        and metrics.duration_ms
        and metrics.duration_ms > 0
    ):
        fraction = metrics.unattributed_ms / metrics.duration_ms * 100
        if fraction >= 15:
            out.append(
                f"{label}: {metrics.unattributed_ms}ms ({fraction:.0f}%) of "
                "the matched execution's duration falls outside measured "
                "Spark stage time -- likely external work (e.g. a BigQuery "
                "load job the connector waits on), which stage/GC metrics "
                "won't explain."
            )

    diag = metrics.native_diagnosis
    if diag is not None and diag.fallback_summary is not None:
        if diag.fallback_summary.selected_engine == "vanilla":
            out.append(f"{label}: {diag.verdict}")

    if env_diffs:
        sample = list(env_diffs)[:5]
        out.append(
            f"{label}: {len(env_diffs)} non-infra config difference(s) vs "
            f"baseline, including: {', '.join(sample)}."
        )

    return out


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def diagnose_regression(
    app_ids: List[str],
    labels: Optional[List[str]] = None,
    description: Optional[str] = None,
    execution_ids: Optional[List[Optional[int]]] = None,
    server: Optional[str] = None,
    baseline_index: int = 0,
    servers: Optional[List[Optional[str]]] = None,
    *,
    ctx: Context,
) -> RegressionDiagnosis:
    """
    Preliminary regression diagnosis across 2+ Spark applications: matches
    each app's SQL execution, compares duration/stage-time/GC, diffs
    non-infra environment config against a baseline, runs the DPAS
    native-vs-vanilla diagnosis (diagnose_native_execution), and
    synthesizes a findings narrative.

    One call instead of manually chaining list_sql_executions,
    compare_job_environments, compare_stages, and
    diagnose_native_execution per app pair.

    Matching: pass ``execution_ids`` (one per app_id, in order; an entry of
    None falls back to ``description`` for that app) to pin specific
    executions, or rely purely on ``description`` (a case-insensitive
    substring, same semantics as list_sql_executions) to find them
    automatically in every app. When multiple executions match in one app
    (e.g. a DataSourceV2 write command and a sibling execution carrying its
    actual jobs), the greatest-duration match is reported as that app's
    headline duration, but ALL matches' jobs/stages are pooled for
    stage_time_ms/tasks/jvm_gc_time_ms -- see RegressionAppMetrics and its
    unattributed_ms field, which flags time the pooled stages still don't
    account for (e.g. an external BigQuery load job).

    Args:
        app_ids: 2 or more Spark application IDs to compare
        servers: Optional History Server name per app_id, same order, for
            cross-cluster comparisons (e.g. ["deb11", "deb12"]); an entry
            of None uses ``server``
        labels: Optional friendly name per app_id, same order (e.g.
            ["VSpark", "DPAS-BQ"]) -- used in findings text and easier to
            read than raw app IDs
        description: Case-insensitive substring to match each app's SQL
            execution(s) by (e.g. "table=web_sales"). Required unless every
            entry in execution_ids is given.
        execution_ids: Optional explicit execution ID per app_id, same
            order; an entry of None falls back to description matching for
            that app
        server: Optional server name to use (uses default if not specified)
        baseline_index: Index into app_ids of the baseline/"before" run
            that deltas and env diffs are computed against (default 0)

    Returns:
        RegressionDiagnosis with per-app metrics, deltas vs baseline,
        material environment diffs, and heuristic findings
    """
    if len(app_ids) < 2:
        raise ValueError("diagnose_regression needs at least 2 app_ids")
    if not (0 <= baseline_index < len(app_ids)):
        raise ValueError(
            f"baseline_index {baseline_index} out of range for {len(app_ids)} app_ids"
        )
    if execution_ids is not None and len(execution_ids) != len(app_ids):
        raise ValueError("execution_ids must have the same length as app_ids")
    if labels is not None and len(labels) != len(app_ids):
        raise ValueError("labels must have the same length as app_ids")

    metrics: List[RegressionAppMetrics] = []
    spark_props_by_index: List[Dict[str, str]] = []

    if servers is not None and len(servers) != len(app_ids):
        raise ValueError("servers must have one entry per app_id")

    for i, app_id in enumerate(app_ids):
        app_server = (servers[i] if servers else None) or server
        client = get_client_or_default(ctx, app_server, app_id)
        eid = execution_ids[i] if execution_ids else None
        primary, group = _resolve_execution_group(
            client, app_id, eid, description, ctx=ctx, server=app_server
        )

        _jobs, stage_ids, agg, all_stages = _jobs_and_stage_agg_for_executions(
            client, app_id, group
        )

        primary_stage_id = None
        primary_stage_quantiles = None
        driving_stages = [s for s in all_stages if s.stage_id in stage_ids]
        if driving_stages:
            top_stage = max(driving_stages, key=lambda s: s.num_tasks or 0)
            primary_stage_id = top_stage.stage_id
            try:
                side = _collect_stage_side(client, app_id, primary_stage_id)
                primary_stage_quantiles = side.task_quantiles
            except Exception:
                pass  # noqa: S110 -- best-effort enrichment, not required

        env = client.get_environment(app_id=app_id)
        spark_props = dict(env.spark_properties or [])
        spark_props_by_index.append(spark_props)

        unattributed_ms = None
        if primary.duration is not None and agg.duration is not None:
            # Overlapping stages can sum past wall time; clamp, it means
            # "nothing unaccounted for", not negative time.
            unattributed_ms = max(0, primary.duration - agg.duration)

        native_diagnosis = None
        try:
            # Scan every execution in the app, not just `primary`: the FQF
            # decision commonly lives on a sibling execution (e.g. the
            # actual InsertIntoHadoopFsRelationCommand job), not on the
            # outer wrapper command `primary` was chosen for (see
            # _resolve_execution_group). Prefer whichever matched-group
            # execution actually has an FQF decision recorded.
            # Scoped per matched id (event log is cached, so this costs
            # one download per app, not one per id).
            diagnoses = []
            for gid in sorted({e.id for e in group if e.id is not None}):
                diagnoses.extend(
                    diagnose_native_execution(
                        app_id, server=app_server, execution_id=gid, ctx=ctx
                    )
                )
            native_diagnosis = next(
                (
                    d
                    for d in diagnoses
                    if d.fallback_summary and d.fallback_summary.fqf_enabled
                ),
                diagnoses[0] if diagnoses else None,
            )
        except Exception:
            pass  # noqa: S110 -- not every app has Gluten instrumentation

        metrics.append(
            RegressionAppMetrics(
                app_id=app_id,
                label=labels[i] if labels else None,
                is_baseline=(i == baseline_index),
                matched_execution_id=primary.id,
                matched_execution_ids=[e.id for e in group if e.id is not None],
                description=primary.description,
                duration_ms=primary.duration,
                stage_time_ms=agg.duration,
                tasks=agg.tasks,
                jvm_gc_time_ms=agg.jvm_gc_time,
                unattributed_ms=unattributed_ms,
                primary_stage_id=primary_stage_id,
                primary_stage_task_quantiles=primary_stage_quantiles,
                executor_memory=spark_props.get("spark.executor.memory"),
                off_heap_size=(
                    spark_props.get("spark.memory.offHeap.size")
                    or spark_props.get("spark.gluten.memory.offHeap.size.in.bytes")
                ),
                native_diagnosis=native_diagnosis,
            )
        )

    baseline = metrics[baseline_index]
    baseline_props = spark_props_by_index[baseline_index]

    # Result dicts are keyed per entry. Keying by app_id alone collapsed
    # entries when one app appears twice (two executions of the same app,
    # or the same app id on two servers): prefer the label, and suffix
    # "#<position>" whenever a key would repeat.
    raw_keys = [m.label or m.app_id for m in metrics]
    keys = [
        k if raw_keys.count(k) == 1 else f"{k}#{i}" for i, k in enumerate(raw_keys)
    ]

    duration_delta_pct: Dict[str, float] = {}
    stage_time_delta_pct: Dict[str, float] = {}
    gc_time_delta_pct: Dict[str, float] = {}
    material_env_diffs: Dict[str, Dict[str, Dict[str, Optional[str]]]] = {}
    findings: List[str] = []

    for i, m in enumerate(metrics):
        if i == baseline_index:
            continue
        d_pct = _pct_change(baseline.duration_ms, m.duration_ms)
        s_pct = _pct_change(baseline.stage_time_ms, m.stage_time_ms)
        g_pct = _pct_change(baseline.jvm_gc_time_ms, m.jvm_gc_time_ms)
        if d_pct is not None:
            duration_delta_pct[keys[i]] = d_pct
        if s_pct is not None:
            stage_time_delta_pct[keys[i]] = s_pct
        if g_pct is not None:
            gc_time_delta_pct[keys[i]] = g_pct

        env_diffs = _material_env_diff(baseline_props, spark_props_by_index[i])
        if env_diffs:
            material_env_diffs[keys[i]] = env_diffs

        findings.extend(_findings_for(m, baseline, d_pct, g_pct, env_diffs))

    return RegressionDiagnosis(
        baseline_app_id=baseline.app_id,
        metrics=metrics,
        duration_delta_pct=duration_delta_pct,
        stage_time_delta_pct=stage_time_delta_pct,
        gc_time_delta_pct=gc_time_delta_pct,
        material_env_diffs=material_env_diffs,
        findings=findings,
    )
