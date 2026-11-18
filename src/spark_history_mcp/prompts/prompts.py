from typing import Optional

from spark_history_mcp.core.app import mcp


@mcp.prompt(
    title="Investigate Spark application failure",
)
def investigate_failure(app_id: str, server: Optional[str] = None) -> str:
    """Guided root-cause investigation for a failed Spark application.

    Walks from the failed application down to the individual task exceptions,
    using the Spark History Server tools exposed by this server.

    Args:
        app_id: The Spark application ID to investigate.
        server: Optional configured server name to query. When omitted, the
            tools discover which configured server hosts the application.
    """
    server_arg = f', server="{server}"' if server else ""

    if server:
        server_note = f"Target the `{server}` server on every call below."
    else:
        server_note = (
            "Omitting `server` makes each tool search every configured server "
            "for the application and use the one that has it; add "
            '`server="<name>"` only to disambiguate an id that exists on more '
            "than one server."
        )

    return f"""Investigate why Spark application `{app_id}` failed. Work through the steps in order; stop early only if the root cause becomes unambiguous.

{server_note}

1. Confirm the application: `list_applications(app_id="{app_id}"{server_arg})` — note status, attempts, and duration. If there are multiple attempts, the later tools default to the latest; pass `app_attempt_id` to those that accept it (e.g. `list_jobs`, `get_stage`, `get_environment`) to target a specific one.
2. Find failed jobs: `list_jobs(app_id="{app_id}", status=["FAILED"]{server_arg})`. If none report FAILED, list without a status filter and look for non-zero `num_failed_tasks` / `num_failed_stages`.
3. Find failed stages: `list_stages(app_id="{app_id}", status=["FAILED"]{server_arg})`, then `get_stage(app_id="{app_id}", stage_id=<id>, with_summaries=true{server_arg})` for detail.
4. Read the task exceptions (key step): `list_stage_task_failures(app_id="{app_id}", stage_id=<id>{server_arg})` for the per-task stack traces.
5. Check the environment when a version or config cause is suspected: `get_environment(app_id="{app_id}", section="runtime"{server_arg})` and `section="classpath_entries"` for Java/Scala/Spark and library versions; `section="spark_properties"` for relevant config (memory, serializer, shuffle).
6. Optional: if the job's source is in your workspace, map the failing class/method/line and any SQL call site to the code. Use it only to explain errors already in the data; do not assume it exists or speculate.
7. Summarize the root cause with specific evidence (stage, task, exception, version/config) and recommend a fix. For a resource issue (OOM, GC pressure), confirm first via `get_executor_summary`, `list_executors`, or `get_job_bottlenecks`."""


@mcp.prompt(
    title="Compare two Spark applications",
)
def compare_applications(
    app_a: str,
    app_b: str,
    server: Optional[str] = None,
    context: Optional[str] = None,
) -> str:
    """Guided, evidence-driven comparison of two Spark applications.

    Layered analysis (configuration, application-level performance, SQL
    executions or jobs, then stages) describing how two runs differ. Descriptive
    only: it reports what the data shows and does not diagnose causes or
    recommend changes.

    Args:
        app_a: The first Spark application ID (referred to as "A").
        app_b: The second Spark application ID (referred to as "B").
        server: Optional configured server name to query. When omitted, each
            application is discovered independently across all configured
            servers (so A and B may live on different servers).
        context: Optional free-text description of what the two runs represent
            (e.g. "A is yesterday's run, B is today's after raising shuffle
            partitions").
    """
    server_arg = f', server="{server}"' if server else ""

    if server:
        server_note = f"Target the `{server}` server on every call below."
    else:
        server_note = (
            "Omitting `server` locates each application independently across "
            "all configured servers, so A and B may live on different servers "
            '(e.g. prod vs staging); add `server="<name>"` only to force one '
            "server or to disambiguate an id present on more than one."
        )

    context_note = (
        f"\nContext: {context} — use it to focus, but back every observation "
        "with tool data.\n"
        if context
        else ""
    )

    return f"""Compare Spark applications A (`{app_a}`) and B (`{app_b}`) and report how they differ. Analysis only: describe what the data shows; do not diagnose why a difference occurred and do not recommend changes.

{server_note}
{context_note}
1. Configuration: `compare_job_environments(app_id1="{app_a}", app_id2="{app_b}"{server_arg})`. Focus on execution-affecting properties (shuffle partitions, executor/driver memory and cores, AQE, plugins/extensions, shuffle manager); ignore infrastructure noise (application ids, host/pod names, timestamps, ports).
2. Application-level metrics: `compare_job_performance(app_id1="{app_a}", app_id2="{app_b}"{server_arg})` — note deltas in executors, jobs, stages, tasks, input, shuffle read/write, disk spill, and GC time.
3. Unit of work — first call `list_sql_executions` on each app:
   - SQL/DataFrame apps (executions returned): match by description; for the largest gaps call `compare_sql_executions(app_id1="{app_a}", app_id2="{app_b}", execution_id1=<id_a>, execution_id2=<id_b>, include_plan_diff=true{server_arg})` for aggregated metrics and the plan-structure diff.
   - RDD / low-level apps (none returned): compare jobs instead via `list_jobs` on each app, matched by name and order; the plan diff is unavailable.
4. Stages: for the most divergent stages, call `get_stage(app_id="{app_a}", stage_id=<id>, with_summaries=true{server_arg})` (and the match for B) and compare task-metric quantiles for skew, spill, and GC (p25/p50/p75/max).
5. Summarize the differences side by side across configuration, application metrics, SQL/jobs, and stages, backing each with a specific number from the output above."""


@mcp.prompt(
    title="Investigate Spark slowness (DPAS vs baseline)",
)
def investigate_slowness(
    dpas_app_id: str,
    baseline_app_id: str,
    server: Optional[str] = None,
    context: Optional[str] = None,
) -> str:
    """Guided investigation of why a DPAS-accelerated Spark run is slow
    relative to its non-accelerated baseline.

    Unlike `compare_applications` (descriptive only), this prompt diagnoses a
    root cause: it first verifies that native/columnar acceleration (the
    Gluten-based engine DPAS builds on) actually engaged, then checks whether
    a cost-based decision fell a specific query or stage back to row-based
    execution, before narrowing down to an operator fallback or resource
    constraint.

    Args:
        dpas_app_id: The Spark application ID run with DPAS acceleration
            enabled.
        baseline_app_id: The Spark application ID run without DPAS
            acceleration (e.g. Dataproc's own default Spark runtime — not
            necessarily plain open-source Spark, so it may have its own
            optimizations active), to compare against.
        server: Optional configured server name to query. When omitted, each
            application is discovered independently across all configured
            servers.
        context: Optional free-text description of the two runs (e.g. "same
            TPC-DS query, same cluster size, DPAS run is 40% slower").
    """
    server_arg = f', server="{server}"' if server else ""

    if server:
        server_note = f"Target the `{server}` server on every call below."
    else:
        server_note = (
            "Omitting `server` locates each application independently across "
            "all configured servers, so the two runs may live on different "
            'servers; add `server="<name>"` only to force one server or to '
            "disambiguate an id present on more than one."
        )

    context_note = (
        f"\nContext: {context} — use it to focus, but back every observation "
        "with tool data.\n"
        if context
        else ""
    )

    return f"""Investigate why the DPAS-accelerated run (`{dpas_app_id}`) is slow relative to its non-accelerated baseline (`{baseline_app_id}`). This is diagnostic: identify the specific cause(s) and recommend a fix, backing every claim with tool data.

{server_note}
{context_note}
1. Confirm both applications: `list_applications(app_id="{dpas_app_id}"{server_arg})` and `list_applications(app_id="{baseline_app_id}"{server_arg})` — note status, duration, and attempts.
2. Verify acceleration actually engaged before comparing performance — a "DPAS" run where acceleration is disabled or silently falls back is the most common cause of "no speedup". Call `get_environment(app_id="{dpas_app_id}", section="spark_properties"{server_arg})` and check, in order:
   - `spark.gluten.dp.usedpas` / `spark.gluten.dp.usedpas.decision` — the submit-time wrapper records the acceleration decision directly, which is more reliable than inferring engagement indirectly. `decision=dpas` means acceleration was enabled; anything else names *why* it was switched off: `nodpas` (explicitly disabled via `usedpas=false`), `nodpas-config-low` (executor memory/off-heap too small — roughly under 2 GB total or under 1 GB off-heap — so the plugin was dropped), `nodpas-localjob-failed` (a prior local-job failure marker was present), `nodpas-autoBroadcastJoinThreshold-disabled`, or `nodpas-legacy-timeParserPolicy` (`spark.sql.legacy.timeParserPolicy=LEGACY`). Any `nodpas*` value on the DPAS side explains "no speedup" by itself; the fix is the named config, not a plan investigation. Also note whether `spark.gluten.dp.enabled` or a per-operator toggle (`spark.gluten.dp.{{scan,project,filter,aggregate,hashjoin,mergejoin,window,orderby}}.enabled`) was set to `false`, and whether `spark.gluten.dp.filescan.blacklist` excludes the scanned formats — each silently narrows what is offloaded.
   - `spark.plugins` containing `org.apache.gluten.GlutenPlugin`
   - `spark.shuffle.manager` set to `org.apache.spark.shuffle.sort.ColumnarShuffleManager` (or a Celeborn/Uniffle columnar variant)
   - Any other `spark.gluten.*` keys present (columnar batch size, per-operator columnar toggles, memory isolation, shuffle codec)
   Also check `section="classpath_entries"` for a `gluten-*-bundle-*.jar`. Run the same check on `{baseline_app_id}` and confirm these are absent — if they are *not* absent, the "baseline" isn't clean and the comparison is invalid; stop and flag this instead of proceeding. Note: on a managed platform (e.g. Dataproc), the baseline may still show its own unrelated optimizer flags (e.g. `spark.dataproc.*`) on both sides — that's expected and separate from DPAS; only the Gluten/DPAS-specific keys above indicate DPAS itself.
3. Even when acceleration engaged for the application as a whole, a cost-based decision can still fall back individual SQL executions or individual stages to plain row-based execution when it estimates that would be cheaper. If present, `spark.gluten.dp.cost.queryFallbackEnabled` and `spark.gluten.dp.cost.stageFallbackEnabled` (seen in step 2) tell you which granularity is active, and set expectations for the plan/stage evidence below: query-level fallback means a given SQL execution's plan is either fully native or fully vanilla (never a mix); stage-level fallback can produce a mix of native and vanilla stages within one otherwise-native query. Both flags default to off, so if neither key is set, no cost-based fallback is active and any vanilla operators come from per-operator unsupported-feature fallback instead. The query-level decision falls back when the estimated vanilla cost is less than or equal to the native cost (ties go to vanilla), and the cost model assumes only a bounded native speedup, so a workload that is genuinely much faster natively can still be estimated as cheaper on vanilla. Separately, `spark.gluten.dp.history.queryfailure.queryFallbackEnabled` forces a query back to vanilla when its plan matches one previously recorded as failing under DPAS — a fallback that is about history, not about cost.
4. Application-level metrics: `compare_job_performance(app_id1="{dpas_app_id}", app_id2="{baseline_app_id}"{server_arg})` — note deltas in duration, executors, tasks, input, shuffle read/write, disk spill, and GC time.
5. Unit of work — call `list_sql_executions` on each app and match by description. For the largest gap (or the slowest execution overall), call `compare_sql_executions(app_id1="{dpas_app_id}", app_id2="{baseline_app_id}", execution_id1=<id>, execution_id2=<id>, include_plan_diff=true{server_arg})`. In the DPAS plan, look for native/columnar operator markers in node names: `Transformer` (e.g. `WholeStageCodegenTransformer`, `FileSourceScanExecTransformer`, `BatchScanExecTransformer`), `ColumnarToRow` (e.g. `VeloxColumnarToRowExec`), or the generic `GlutenPlan` marker. A plan with *no* such markers at all (100% plain vanilla operator names) is the signature of a whole-query cost-based fallback (see step 3) — treat that as a confirmed cause, not a tool-integration gap, and don't expect a partial diff.
6. Stages: for the slowest or most divergent stages between the two runs, call `get_stage(app_id="{dpas_app_id}", stage_id=<id>, with_summaries=true{server_arg})` (and the equivalent for `{baseline_app_id}`) and compare task-metric quantiles (p25/p50/p75/max) for skew, shuffle spill, and GC time. If stage-level fallback is enabled (step 3), this is also where a partial native/vanilla split within one query shows up — compare stages individually rather than assuming the top-level plan text is uniform. Native engines are memory-layout sensitive: heavy spill or GC specifically on the DPAS side often points to columnar batch size or off-heap/native memory sizing rather than the plan itself.
7. If step 6 points at memory pressure, check `get_environment(app_id="{dpas_app_id}", section="spark_properties"{server_arg})` for batch sizing (`spark.gluten.dp.batchSize`, `spark.gluten.sql.columnar.maxBatchSize`), `spark.gluten.memory.isolation`, and off-heap sizing (`spark.memory.offHeap.*`). The submit-time wrapper may also have rebalanced memory: with dynamic off-heap sizing, Spark's own off-heap is set to `0`/disabled and the heap is enlarged instead, so a small or zero `spark.memory.offHeap.size` is not automatically a misconfiguration — compare total executor memory against the baseline's. If `spark.gluten.auto.adjustStageResource.enabled` is set, fully-fallen-back stages get a vanilla-friendly resource profile (off-heap swapped to on-heap), which would show as differing executor memory on those stages. Then use `get_executor_summary` / `list_executors` to check GC time and failed tasks. Note that a query/stage that fell back (step 5/6) still runs under whatever executor memory layout was sized for native execution (small on-heap, large off-heap) — elevated GC specifically on a fallen-back portion of the DPAS run, absent from the equivalent baseline stage, points at that mismatch rather than at the query itself.
8. Summarize the root cause with specific evidence — the acceleration-decision value from step 2, a fallback confirmed by plan/stage evidence (name the query or stage), or a resource constraint (name the metric) — and recommend a concrete fix (e.g. investigate why the cost-based decision fell back this workload, raise off-heap memory, enable a specific `spark.gluten.sql.columnar.*` flag, or note that this workload's dominant cost sits outside SQL/columnar execution — e.g. an RDD-heavy stage — where DPAS acceleration doesn't apply)."""
