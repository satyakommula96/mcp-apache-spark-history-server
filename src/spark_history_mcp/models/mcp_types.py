"""Curated, LLM-friendly output models for MCP tools.

These intentionally expose a small, summarized subset of the raw Spark History
Server payloads.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SqlExecutionSummary(BaseModel):
    """Header-level summary of a single SQL execution."""

    id: Optional[int] = None
    status: Optional[str] = None
    description: Optional[str] = None
    submission_time: Optional[str] = Field(None, alias="submissionTime")
    duration: Optional[int] = None  # milliseconds, as reported by SHS
    success_job_ids: List[int] = Field(default_factory=list, alias="successJobIds")
    failed_job_ids: List[int] = Field(default_factory=list, alias="failedJobIds")
    running_job_ids: List[int] = Field(default_factory=list, alias="runningJobIds")

    model_config = ConfigDict(populate_by_name=True)


class SqlNodeMetrics(BaseModel):
    """Per-node plan metrics, keyed name -> value."""

    node_id: Optional[int] = Field(None, alias="nodeId")
    node_name: Optional[str] = Field(None, alias="nodeName")
    metrics: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class SqlJobSummary(BaseModel):
    """Curated job row associated with a SQL execution."""

    job_id: Optional[int] = Field(None, alias="jobId")
    status: Optional[str] = None
    description: Optional[str] = None
    duration: Optional[int] = None  # milliseconds
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    stage_ids: List[int] = Field(default_factory=list, alias="stageIds")

    model_config = ConfigDict(populate_by_name=True)


class SqlStageSummary(BaseModel):
    """Curated stage row associated with a SQL execution."""

    stage_id: Optional[int] = Field(None, alias="stageId")
    attempt_id: Optional[int] = Field(None, alias="attemptId")
    status: Optional[str] = None
    description: Optional[str] = None
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    duration: Optional[int] = None  # milliseconds
    input_bytes: Optional[int] = Field(None, alias="inputBytes")
    shuffle_read_bytes: Optional[int] = Field(None, alias="shuffleReadBytes")
    shuffle_write_bytes: Optional[int] = Field(None, alias="shuffleWriteBytes")

    model_config = ConfigDict(populate_by_name=True)


class StageMetricsAggregation(BaseModel):
    """Aggregated stage metrics scoped to a SQL execution."""

    stage_count: int = Field(0, alias="stageCount")
    tasks: int = 0
    duration: int = 0  # milliseconds (sum of per-stage wall durations)
    input_bytes: int = Field(0, alias="inputBytes")
    shuffle_read_bytes: int = Field(0, alias="shuffleReadBytes")
    shuffle_write_bytes: int = Field(0, alias="shuffleWriteBytes")
    disk_bytes_spilled: int = Field(0, alias="diskBytesSpilled")
    jvm_gc_time: int = Field(0, alias="jvmGcTime")

    model_config = ConfigDict(populate_by_name=True)


class SqlExecutionDetail(BaseModel):
    """Detailed view of a SQL execution.

    By default, only ``execution`` (the header) is populated. The optional
    sections are filled in when the corresponding ``include_*`` flag is set on
    ``get_sql_execution``.
    """

    execution: SqlExecutionSummary
    plan_description: Optional[str] = Field(None, alias="planDescription")
    node_metrics: Optional[List[SqlNodeMetrics]] = Field(None, alias="nodeMetrics")
    jobs: Optional[List[SqlJobSummary]] = None
    stage_metrics: Optional[StageMetricsAggregation] = Field(None, alias="stageMetrics")
    stages: Optional[List[SqlStageSummary]] = None

    model_config = ConfigDict(populate_by_name=True)


class SqlCompareSide(BaseModel):
    """One side of a SQL execution metrics comparison."""

    app: str
    sql_id: Optional[int] = Field(None, alias="sqlId")
    description: Optional[str] = None
    status: Optional[str] = None
    duration: Optional[int] = None  # milliseconds
    jobs: int = 0
    stages: int = 0
    tasks: int = 0
    stage_time: int = Field(0, alias="stageTime")  # milliseconds
    input_bytes: int = Field(0, alias="inputBytes")
    shuffle_read_bytes: int = Field(0, alias="shuffleReadBytes")
    shuffle_write_bytes: int = Field(0, alias="shuffleWriteBytes")
    disk_bytes_spilled: int = Field(0, alias="diskBytesSpilled")
    jvm_gc_time: int = Field(0, alias="jvmGcTime")

    model_config = ConfigDict(populate_by_name=True)


class SqlNodeTypeDiff(BaseModel):
    """Count of a single plan node type on each side of a plan comparison."""

    node_type: str = Field(..., alias="nodeType")
    a: int = 0
    b: int = 0

    model_config = ConfigDict(populate_by_name=True)


class SqlPlanComparison(BaseModel):
    """Plan-structure diff between two SQL executions."""

    app_a: str = Field(..., alias="appA")
    app_b: str = Field(..., alias="appB")
    exec_id_a: Optional[int] = Field(None, alias="execIdA")
    exec_id_b: Optional[int] = Field(None, alias="execIdB")
    node_count_a: int = Field(0, alias="nodeCountA")
    node_count_b: int = Field(0, alias="nodeCountB")
    edge_count_a: int = Field(0, alias="edgeCountA")
    edge_count_b: int = Field(0, alias="edgeCountB")
    node_type_diffs: List[SqlNodeTypeDiff] = Field(
        default_factory=list, alias="nodeTypeDiffs"
    )

    model_config = ConfigDict(populate_by_name=True)


class SqlExecutionComparison(BaseModel):
    """Metrics diff between two SQL executions, with an optional plan-structure diff."""

    a: SqlCompareSide
    b: SqlCompareSide
    plan_comparison: Optional[SqlPlanComparison] = Field(None, alias="planComparison")

    model_config = ConfigDict(populate_by_name=True)


class StageTaskQuantiles(BaseModel):
    """Per-task metric distributions for a stage at the requested quantiles.

    Each metric is a list aligned with ``quantiles`` (e.g. ``[p25, p50, p75,
    max]``). Time metrics are milliseconds; byte metrics are bytes.
    """

    quantiles: List[float] = Field(default_factory=list)
    duration: Optional[List[float]] = None
    gc_time: Optional[List[float]] = Field(None, alias="gcTime")
    scheduler_delay: Optional[List[float]] = Field(None, alias="schedulerDelay")
    peak_execution_memory: Optional[List[float]] = Field(
        None, alias="peakExecutionMemory"
    )
    input_bytes: Optional[List[float]] = Field(None, alias="inputBytes")
    output_bytes: Optional[List[float]] = Field(None, alias="outputBytes")
    shuffle_read_bytes: Optional[List[float]] = Field(None, alias="shuffleReadBytes")
    shuffle_write_bytes: Optional[List[float]] = Field(None, alias="shuffleWriteBytes")
    disk_bytes_spilled: Optional[List[float]] = Field(None, alias="diskBytesSpilled")
    memory_bytes_spilled: Optional[List[float]] = Field(
        None, alias="memoryBytesSpilled"
    )

    model_config = ConfigDict(populate_by_name=True)


class StageCompareSide(BaseModel):
    """One side of a stage comparison: stage-level metrics and task quantiles."""

    app: str
    stage_id: Optional[int] = Field(None, alias="stageId")
    attempt_id: Optional[int] = Field(None, alias="attemptId")
    status: Optional[str] = None
    description: Optional[str] = None
    duration: int = 0  # milliseconds (wall time, submission -> completion)
    tasks: int = 0
    failed_tasks: int = Field(0, alias="failedTasks")
    input_bytes: int = Field(0, alias="inputBytes")
    output_bytes: int = Field(0, alias="outputBytes")
    shuffle_read_bytes: int = Field(0, alias="shuffleReadBytes")
    shuffle_write_bytes: int = Field(0, alias="shuffleWriteBytes")
    disk_bytes_spilled: int = Field(0, alias="diskBytesSpilled")
    memory_bytes_spilled: int = Field(0, alias="memoryBytesSpilled")
    jvm_gc_time: int = Field(0, alias="jvmGcTime")  # milliseconds
    task_quantiles: Optional[StageTaskQuantiles] = Field(None, alias="taskQuantiles")

    model_config = ConfigDict(populate_by_name=True)


class StageComparison(BaseModel):
    """Metrics and task-quantile diff between two stages."""

    a: StageCompareSide
    b: StageCompareSide

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineInfo(BaseModel):
    """Whether an application ran on a native SQL engine (e.g. Gluten/Velox).

    Detected from ``spark.plugins`` and ``spark.gluten.*`` entries in the
    application's environment. When ``is_native`` is true, most physical-plan
    operators are replaced with native "*Transformer" nodes whose metrics
    (spill, native memory, native wall time) live only in the SQL plan node
    metrics -- they are not reflected in the standard JVM stage/task fields
    (``memoryBytesSpilled``, ``jvmGcTime``, ...).
    """

    engine: str = "spark"
    backend: Optional[str] = None
    is_native: bool = Field(False, alias="isNative")

    model_config = ConfigDict(populate_by_name=True)


class NativeOperatorMetrics(BaseModel):
    """Per-operator metrics for a native (Gluten/Velox) plan node, scoped to
    one SQL execution."""

    sql_execution_id: Optional[int] = Field(None, alias="sqlExecutionId")
    node_id: Optional[int] = Field(None, alias="nodeId")
    node_name: Optional[str] = Field(None, alias="nodeName")
    metrics: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineSpillHit(BaseModel):
    """A non-zero spill-related metric found on a native operator.

    Surfaced separately from ``NativeOperatorMetrics`` because native (e.g.
    Velox) spill is invisible to the standard stage-level
    ``memoryBytesSpilled``/``diskBytesSpilled`` fields that
    ``get_job_bottlenecks`` otherwise relies on.
    """

    sql_execution_id: Optional[int] = Field(None, alias="sqlExecutionId")
    node_id: Optional[int] = Field(None, alias="nodeId")
    node_name: Optional[str] = Field(None, alias="nodeName")
    metric: str
    value: str

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineMetrics(BaseModel):
    """Curated Gluten/Velox native-engine metrics for an application."""

    application_id: str = Field(..., alias="applicationId")
    engine: NativeEngineInfo
    operators: List[NativeOperatorMetrics] = Field(default_factory=list)
    spill_hits: List[NativeEngineSpillHit] = Field(
        default_factory=list, alias="spillHits"
    )

    model_config = ConfigDict(populate_by_name=True)


class BloomFilterOwnership(BaseModel):
    """Producer/consumer engine ownership of a Spark runtime Bloom filter,
    detected from a SQL execution's physical plan text.

    Spark's runtime Bloom filter join optimization builds the filter with a
    ``bloom_filter_agg`` aggregate (the producer) and evaluates it with
    ``might_contain`` (the consumer); under Gluten each side independently
    runs on Spark or the native engine.
    """

    execution_id: Optional[int] = Field(None, alias="executionId")
    used: bool = False
    producer: Optional[str] = None
    consumer: Optional[str] = None
    hosts: List[str] = Field(default_factory=list)
    note: Optional[str] = Field(
        None,
        description="Set instead of producer/consumer when the plan shape "
        "couldn't be classified unambiguously.",
    )

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineEvent(BaseModel):
    """One raw Gluten/DataPelago custom listener event from the event log."""

    event_type: str = Field(..., alias="eventType")
    fields: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class QueryFallbackSummary(BaseModel):
    """Full Query Fallback (FQF) decision and operator-fallback summary for
    one SQL execution, from GlutenPlanFallbackEvent and the root-scope
    DPQueryCostDecisionEvent in the raw event log."""

    execution_id: Optional[int] = Field(None, alias="executionId")
    fqf_enabled: bool = Field(False, alias="fqfEnabled")
    fell_back: bool = Field(False, alias="fellBack")
    selected_engine: Optional[str] = Field(None, alias="selectedEngine")
    vanilla_plan_cost: Optional[float] = Field(None, alias="vanillaPlanCost")
    gluten_plan_cost: Optional[float] = Field(None, alias="glutenPlanCost")
    cost_decision_fallback_summary: List[Any] = Field(
        default_factory=list, alias="costDecisionFallbackSummary"
    )
    fallback_events: int = Field(0, alias="fallbackEvents")
    fallback_node_reasons: Dict[str, str] = Field(
        default_factory=dict, alias="fallbackNodeReasons"
    )

    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan="strings")


class NativeValidationMemoStats(BaseModel):
    """FQF native-validation memo statistics for one root decision scope,
    from DPDecisionOverheadEvent's ``nativeValidationMemo`` field.

    Confirmed against real event logs: ``misses`` equals ``entry_count``
    (every miss creates exactly one new cache entry), and a query's hit
    rate -- ``hits / (hits + misses)`` -- commonly falls in the 40-95%
    range on real TPC-DS queries, consistent with the memo design doc's
    own representative example (~89%). ``bound_rejections`` counts entries
    that were evicted for exceeding the memo's 4,096-entry/16 MiB cap.
    """

    entry_count: Optional[int] = Field(None, alias="entryCount")
    retained_bytes: Optional[int] = Field(None, alias="retainedBytes")
    hits: Optional[int] = None
    misses: Optional[int] = None
    bound_rejections: Optional[int] = Field(None, alias="boundRejections")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def hit_rate(self) -> Optional[float]:
        """``hits / (hits + misses)``, or None when both are unavailable/zero."""
        if self.hits is None or self.misses is None:
            return None
        total = self.hits + self.misses
        return (self.hits / total) if total else None


class QueryPlanningOverhead(BaseModel):
    """Driver-side native-planning overhead for one FQF decision scope, from
    DPDecisionOverheadEvent in the raw event log.

    ``compare_snapshot_build_ms``/``gluten_build_ms``/etc are the timing
    phases the FQF native-validation memo is designed to shrink; ``memo``
    is the memo's own hit/miss counters for that same scope, when present
    (verified real field -- an earlier version of this tool incorrectly
    said no such counter existed in the event).
    """

    execution_id: Optional[int] = Field(None, alias="executionId")
    scope_type: Optional[str] = Field(None, alias="scopeType")
    scope_id: Optional[str] = Field(None, alias="scopeId")
    compare_snapshot_build_ms: Optional[float] = Field(
        None, alias="compareSnapshotBuildMs"
    )
    evaluation_ms: Optional[float] = Field(None, alias="evaluationMs")
    fallback_build_ms: Optional[float] = Field(None, alias="fallbackBuildMs")
    gluten_build_ms: Optional[float] = Field(None, alias="glutenBuildMs")
    planning_total_ms: Optional[float] = Field(None, alias="planningTotalMs")
    memo: Optional[NativeValidationMemoStats] = None

    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan="strings")


class NativeExecutionSummary(BaseModel):
    """One SQL execution's DPAS offload/fallback profile: which engine was
    selected, how many operators actually ran native vs fell back in the
    EXECUTED plan (not a candidate plan that was later rejected), the
    highest native-operator count Gluten ever proposed for this query
    (its offload *potential*, whether or not it was used), fallback
    reasons bucketed by cause, and this query's native-validation memo.
    """

    execution_id: Optional[int] = Field(None, alias="executionId")
    description: Optional[str] = None
    selected_engine: Optional[str] = Field(None, alias="selectedEngine")
    fell_back: bool = Field(False, alias="fellBack")
    vanilla_plan_cost: Optional[float] = Field(None, alias="vanillaPlanCost")
    gluten_plan_cost: Optional[float] = Field(None, alias="glutenPlanCost")
    native_operator_count: int = Field(
        0,
        alias="nativeOperatorCount",
        description="Native (Gluten/Velox) operator count in the plan that "
        "actually executed.",
    )
    fallback_operator_count: int = Field(
        0,
        alias="fallbackOperatorCount",
        description="Operator count that ran on vanilla Spark in the "
        "executed plan.",
    )
    peak_native_operator_count: int = Field(
        0,
        alias="peakNativeOperatorCount",
        description="The most native operators Gluten proposed in ANY "
        "candidate plan for this query, including ones rejected by FQF -- "
        "the offload ceiling, vs native_operator_count's actual outcome.",
    )
    offload_ratio: Optional[float] = Field(
        None,
        alias="offloadRatio",
        description="native_operator_count / (native_operator_count + "
        "fallback_operator_count) in the executed plan, or None when "
        "neither count is available.",
    )
    fallback_reason_counts: Dict[str, int] = Field(
        default_factory=dict,
        alias="fallbackReasonCounts",
        description="Operator count per fallback-cause bucket in the "
        "executed plan's own fallback reasons: 'cost_fallback' (FQF chose "
        "vanilla on estimated cost), 'unsupported_operator' (Gluten has no "
        "native implementation), 'columnar_cache_disabled' (a cached table "
        "forces a row/columnar conversion), or 'other'.",
    )
    memo: Optional[NativeValidationMemoStats] = None

    model_config = ConfigDict(populate_by_name=True)


class NativeExecutionAppSummary(BaseModel):
    """App-level DPAS/Gluten rollup: whether this run was configured to use
    DPAS at all, how many of its SQL executions actually ran native vs fell
    back, fallback causes aggregated across every execution, and the
    native-validation memo's aggregate effectiveness for the whole app.
    """

    app_id: str = Field(..., alias="appId")
    use_dpas_decision: Optional[str] = Field(None, alias="useDpasDecision")
    cost_fallback_enabled: Optional[bool] = Field(None, alias="costFallbackEnabled")
    executions: List[NativeExecutionSummary] = Field(default_factory=list)
    executions_total: int = Field(0, alias="executionsTotal")
    executions_ran_native: int = Field(0, alias="executionsRanNative")
    executions_fell_back: int = Field(0, alias="executionsFellBack")
    fallback_category_totals: Dict[str, int] = Field(
        default_factory=dict,
        alias="fallbackCategoryTotals",
        description="fallback_reason_counts summed across every execution "
        "in the app.",
    )
    memo_total_entries: int = Field(0, alias="memoTotalEntries")
    memo_total_hits: int = Field(0, alias="memoTotalHits")
    memo_total_misses: int = Field(0, alias="memoTotalMisses")
    memo_total_bound_rejections: int = Field(0, alias="memoTotalBoundRejections")
    memo_overall_hit_rate: Optional[float] = Field(None, alias="memoOverallHitRate")

    model_config = ConfigDict(populate_by_name=True)


class NativeExecutionDiagnosis(BaseModel):
    """One-call preliminary verdict on whether a SQL execution ran natively
    (Gluten/Velox) or fell back to vanilla Spark, and why.

    Combines the DPAS config flags that govern Full Query Fallback (FQF),
    the FQF cost decision and per-operator fallback reasons, the driver-side
    planning overhead FQF itself cost, whether Gluten ever built a native
    candidate plan for this query (even if later rejected), and whether the
    plan that actually executed contains any native operator -- so a
    native-capability flag being enabled (e.g. a "native write" toggle)
    isn't mistaken for proof that native execution happened.
    """

    execution_id: Optional[int] = Field(None, alias="executionId")
    cost_fallback_enabled: Optional[bool] = Field(
        None,
        alias="costFallbackEnabled",
        description="spark.gluten.dp.cost.queryFallbackEnabled, parsed as a bool.",
    )
    use_dpas_decision: Optional[str] = Field(
        None,
        alias="useDpasDecision",
        description="spark.gluten.dp.usedpas.decision ('dpas'/'nodpas'): whether "
        "this run was configured to use the native engine at all.",
    )
    dp_config: Dict[str, str] = Field(
        default_factory=dict,
        alias="dpConfig",
        description="Every spark.gluten.dp.* property seen for this app, "
        "including workload-specific native-capability flags (e.g. a "
        "BigQuery-write native-write toggle) that don't have a dedicated field.",
    )
    fallback_summary: Optional[QueryFallbackSummary] = Field(
        None, alias="fallbackSummary"
    )
    planning_overhead: Optional[QueryPlanningOverhead] = Field(
        None,
        alias="planningOverhead",
        description="Root-scope planning overhead, merged across the "
        "several partial DPDecisionOverheadEvent records a query emits.",
    )
    native_candidate_seen: bool = Field(
        False,
        alias="nativeCandidateSeen",
        description="True if any GlutenPlanFallbackEvent for this execution "
        "shows numGlutenNodes > 0 -- Gluten built a native candidate plan at "
        "some point, whether or not it was ultimately used.",
    )
    executed_plan_has_native_nodes: Optional[bool] = Field(
        None,
        alias="executedPlanHasNativeNodes",
        description="Best-effort scan of the plan that actually executed for "
        "known Gluten/Velox operator-name markers (Transformer/Velox*/"
        "ColumnarExchange/...). None when the executed plan couldn't be "
        "fetched. This is a confirmation cross-check, not the primary "
        "signal -- selected_engine on fallback_summary is authoritative.",
    )
    verdict: str = Field(
        "", description="Human-readable one-paragraph summary of the above."
    )

    model_config = ConfigDict(populate_by_name=True)


class ScanNodeDetail(BaseModel):
    """One scan (leaf/read) operator from a SQL execution's physical plan --
    native (Gluten/Velox) or vanilla, BigQuery or file-based.

    ``metrics`` is a raw passthrough of whatever the engine/connector
    reports under that node (e.g. the BigQuery connector's own
    ``number of BQ bytes read`` on a vanilla scan, or Velox's much larger
    native metric set -- e.g. ``number of raw input bytes``, ``number of
    bytes used for storing bq message in memory`` -- on the same scan once
    Gluten has offloaded it). The two engines use entirely different metric
    vocabularies, verified against real event logs, so this intentionally
    doesn't force them into fixed named fields.

    ``pushed_filters``/``data_filters``/``partition_filters`` come from the
    plan text (present for file-based sources like Parquet/ORC, both native
    and vanilla); the BigQuery connector does not print pushdown info in the
    plan text at all, so these stay empty for BigQuery scans regardless of
    whether pushdown actually happened.

    ``node_id`` is this operator's number in the plan *text* (its own
    self-consistent numbering), not the REST API's JSON ``nodeId`` -- the two
    are independent numbering schemes and don't correspond to the same
    operator. ``metrics`` is attached by matching this node's exact name
    against the JSON nodes, so it's left empty when a table is scanned more
    than once in the same plan (e.g. a self-join) and the name is ambiguous.
    """

    sql_execution_id: Optional[int] = Field(None, alias="sqlExecutionId")
    node_id: Optional[int] = Field(None, alias="nodeId")
    node_name: Optional[str] = Field(None, alias="nodeName")
    is_native: bool = Field(False, alias="isNative")
    source_kind: Optional[str] = Field(
        None,
        alias="sourceKind",
        description="'bigquery', 'file', or None when not recognized",
    )
    format: Optional[str] = None
    table: Optional[str] = None
    location: Optional[str] = None
    read_session_id: Optional[str] = Field(None, alias="readSessionId")
    pushed_filters: List[str] = Field(default_factory=list, alias="pushedFilters")
    data_filters: List[str] = Field(default_factory=list, alias="dataFilters")
    partition_filters: List[str] = Field(
        default_factory=list, alias="partitionFilters"
    )
    metrics: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class RegressionAppMetrics(BaseModel):
    """One app's side of a ``diagnose_regression`` comparison.

    ``duration_ms`` is the matched SQL execution's own headline duration
    (the number a benchmark report would cite). ``stage_time_ms``/``tasks``/
    ``jvm_gc_time_ms`` are aggregated across every job belonging to that
    execution *and* any sibling execution in the same matched group (see
    ``matched_execution_ids``) -- this matters because a wrapper command
    (e.g. a DataSourceV2 ``SaveIntoDataSourceCommand``) can report its own
    duration while all of its actual jobs/stages run under a different,
    sibling SQL execution ID with zero jobs of its own.

    ``unattributed_ms``, when positive, is time inside ``duration_ms`` that
    no stage accounts for at all -- e.g. an external step like a BigQuery
    load job that the connector waits on after Spark's own work finishes.
    Don't attribute 100% of a duration regression to stage/GC metrics
    without checking this first.
    """

    app_id: str = Field(..., alias="appId")
    label: Optional[str] = None
    is_baseline: bool = Field(False, alias="isBaseline")
    matched_execution_id: Optional[int] = Field(None, alias="matchedExecutionId")
    matched_execution_ids: List[int] = Field(
        default_factory=list, alias="matchedExecutionIds"
    )
    description: Optional[str] = None
    duration_ms: Optional[int] = Field(None, alias="durationMs")
    stage_time_ms: Optional[int] = Field(None, alias="stageTimeMs")
    tasks: Optional[int] = None
    jvm_gc_time_ms: Optional[int] = Field(None, alias="jvmGcTimeMs")
    unattributed_ms: Optional[int] = Field(None, alias="unattributedMs")
    primary_stage_id: Optional[int] = Field(None, alias="primaryStageId")
    primary_stage_task_quantiles: Optional[StageTaskQuantiles] = Field(
        None, alias="primaryStageTaskQuantiles"
    )
    executor_memory: Optional[str] = Field(None, alias="executorMemory")
    off_heap_size: Optional[str] = Field(None, alias="offHeapSize")
    native_diagnosis: Optional[NativeExecutionDiagnosis] = Field(
        None, alias="nativeDiagnosis"
    )

    model_config = ConfigDict(populate_by_name=True)


class RegressionDiagnosis(BaseModel):
    """Preliminary multi-app regression diagnosis from ``diagnose_regression``.

    Compares 2+ apps' matching SQL execution against a chosen baseline in
    one call: duration/stage-time/GC deltas, non-infra environment
    differences, and a heuristic ``findings`` narrative -- the same facts
    this project's own regression investigations pull together by hand
    (see skills/dpas-fallback-check and the compare-workflow examples under
    skills/cli/examples/compare), gathered server-side instead.
    """

    baseline_app_id: str = Field(..., alias="baselineAppId")
    metrics: List[RegressionAppMetrics] = Field(default_factory=list)
    duration_delta_pct: Dict[str, float] = Field(
        default_factory=dict, alias="durationDeltaPct"
    )
    stage_time_delta_pct: Dict[str, float] = Field(
        default_factory=dict, alias="stageTimeDeltaPct"
    )
    gc_time_delta_pct: Dict[str, float] = Field(
        default_factory=dict, alias="gcTimeDeltaPct"
    )
    material_env_diffs: Dict[str, Dict[str, Dict[str, Optional[str]]]] = Field(
        default_factory=dict,
        alias="materialEnvDiffs",
        description="Per non-baseline entry (keyed by label, else app_id; #<position> added when a key repeats): {property: {baseline: v, "
        "candidate: v}} for spark properties that differ from the baseline "
        "and aren't recognized infra noise (ports/hosts/ids/timestamps).",
    )
    findings: List[str] = Field(
        default_factory=list,
        description="Heuristic, human-readable findings synthesized from "
        "the above -- a starting narrative, not a substitute for judgment.",
    )

    model_config = ConfigDict(populate_by_name=True)

    model_config = ConfigDict(populate_by_name=True)


class FailedTask(BaseModel):
    """A single failed task with its error message.

    Curated subset of the raw task payload focused on failure investigation;
    the heavy per-task metrics are intentionally omitted.
    """

    task_id: Optional[int] = Field(None, alias="taskId")
    attempt: Optional[int] = None
    executor_id: Optional[str] = Field(None, alias="executorId")
    host: Optional[str] = None
    status: Optional[str] = None
    error_message: Optional[str] = Field(None, alias="errorMessage")

    model_config = ConfigDict(populate_by_name=True)


class GlutenBuildInfo(BaseModel):
    """The "Gluten Build Information" key/value table from the History
    Server's own ``/{app_id}/gluten/`` UI tab -- per-app (not per-query)
    build metadata (Velox/DPAS revision, product name/version, Spark/Scala/
    Java versions). Free-form, since the exact key set varies by platform
    (e.g. "GCP GKE" vs "GCP Dataproc" as the DP Product Name) rather than
    hardcoding fields that may not always be present.
    """

    app_id: str = Field(..., alias="appId")
    fields: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineUiPlanningOverhead(BaseModel):
    """The "== Planning Time Breakdown ==" section of the History Server's
    own ``/dpas/`` UI tab for one SQL execution -- how much of FQF's own
    cost-comparison work (snapshot build, cost evaluation, native plan
    build) added to planning time, distinct from QueryPlanningOverhead
    (which is reconstructed from the raw event log's
    DPDecisionOverheadEvent instead of this rendered page)."""

    total_planning_time_ms: Optional[float] = Field(None, alias="totalPlanningTimeMs")
    smart_fallback_overhead_ms: Optional[float] = Field(
        None, alias="smartFallbackOverheadMs"
    )
    compare_snapshot_build_ms: Optional[float] = Field(
        None, alias="compareSnapshotBuildMs"
    )
    cost_evaluation_ms: Optional[float] = Field(None, alias="costEvaluationMs")
    gluten_plan_build_ms: Optional[float] = Field(None, alias="glutenPlanBuildMs")

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineUiPlanCostNode(BaseModel):
    """One operator's line from the "== DPA-S Plan ==" (or "== Vanilla
    Plan =="/per-subquery) cost tree on the History Server's ``/dpas/`` UI
    tab: ``cost`` is this node's own estimated cost, ``cumulative_cost`` is
    the running total from the plan root down to this node (absent on some
    leaf lines), and ``cost_factor`` (``cf``) is Gluten's per-operator cost
    multiplier."""

    node_id: str = Field(..., alias="nodeId")
    node_name: str = Field(..., alias="nodeName")
    cost: Optional[float] = None
    cumulative_cost: Optional[float] = Field(None, alias="cumulativeCost")
    cost_factor: Optional[float] = Field(None, alias="costFactor")
    stats: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class NativeEngineUiQuerySummary(BaseModel):
    """One SQL execution's Gluten/DPAS cost-and-fallback data, read
    directly from the History Server's own ``/{app_id}/dpas/`` UI tab
    (a server-rendered HTML page, not the JSON REST API -- the REST API's
    ``/sql/{id}`` endpoint has no cost-related fields at all; verified by
    scanning every node's ``metrics`` there for anything cost-related).

    This is a much cheaper alternative to
    ``get_query_fallback_summary``/``diagnose_native_execution`` for this
    subset of the same information: one small HTML page fetch instead of
    downloading and decompressing the (potentially large) raw event log.
    It does NOT replace those tools -- native-validation-memo stats,
    per-operator DPDecisionOverheadEvent-derived overhead, and the
    executed-plan-native-node cross-check they provide aren't rendered on
    this page and still require the raw event log.

    ``has_cost_decision`` is False for executions that ran with Gluten
    active but never triggered a cost comparison (e.g. a trivial
    "CreateTempViewUsing", or a query FQF didn't need to arbitrate) --
    distinct from an app with Gluten not enabled at all, where this whole
    query summary won't be produced/found in the first place.
    """

    execution_id: int = Field(..., alias="executionId")
    description: Optional[str] = None
    num_gluten_nodes: int = Field(0, alias="numGlutenNodes")
    num_fallback_nodes: int = Field(0, alias="numFallbackNodes")
    fallback_summary_text: Optional[str] = Field(None, alias="fallbackSummaryText")
    planning_overhead: Optional[NativeEngineUiPlanningOverhead] = Field(
        None, alias="planningOverhead"
    )
    has_cost_decision: bool = Field(False, alias="hasCostDecision")
    vanilla_cost: Optional[float] = Field(None, alias="vanillaCost")
    dpa_s_cost: Optional[float] = Field(None, alias="dpaSCost")
    dpas_plan_cost_breakdown: List[NativeEngineUiPlanCostNode] = Field(
        default_factory=list, alias="dpasPlanCostBreakdown"
    )
    subquery_cost_text: Optional[str] = Field(
        None,
        alias="subqueryCostText",
        description="Raw '== Subquery Cost Evaluations ==' section text, "
        "when present -- per-correlated-subquery vanilla/dpa-s cost and "
        "plan trees. Kept as raw text rather than fully structured: nesting "
        "is per-subquery, not worth a bespoke model for what's usually a "
        "handful of lines a reader can scan directly.",
    )

    model_config = ConfigDict(populate_by_name=True, ser_json_inf_nan="strings")


class FailedStageDiagnosis(BaseModel):
    """One failed (or partially-failed) stage, with its tasks' real error
    text, as returned by ``diagnose_sql_execution_failures``."""

    stage_id: Optional[int] = Field(None, alias="stageId")
    status: Optional[str] = None
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    failed_tasks: List[FailedTask] = Field(default_factory=list, alias="failedTasks")

    model_config = ConfigDict(populate_by_name=True)


class SqlExecutionDiagnosis(BaseModel):
    """One-call answer to "did this SQL execution fail at the job/stage/task
    level, and why" AND "did it fall back from native execution, and why".

    ``native_engine_available`` is the first-class signal for whether this
    APP has any Gluten/DPAS instrumentation at all (checked via
    ``get_gluten_build_info``, which is per-app, not per-query) -- False for
    a plain-vanilla Spark run where the Gluten plugin was never loaded. This
    is deliberately separate from ``native_engine_summary`` being absent:
    that can also happen for a Gluten-enabled app whose query never
    triggered a cost decision, or an execution_id that plain doesn't exist.
    Always check ``native_engine_available`` before reading anything into a
    missing ``native_engine_summary`` -- absence there is not evidence of
    anything, one way or the other, unless this flag is also False.
    """

    app_id: str = Field(..., alias="appId")
    execution_id: int = Field(..., alias="executionId")
    status: Optional[str] = None
    duration: Optional[int] = None  # milliseconds

    has_stage_failures: bool = Field(False, alias="hasStageFailures")
    failed_stages: List[FailedStageDiagnosis] = Field(
        default_factory=list, alias="failedStages"
    )
    all_stages: Optional[List[SqlStageSummary]] = Field(
        None,
        alias="allStages",
        description="Every stage for this execution, not just failed ones. "
        "Only populated when verbose=True was passed.",
    )

    native_engine_available: bool = Field(False, alias="nativeEngineAvailable")
    native_engine_summary: Optional[NativeEngineUiQuerySummary] = Field(
        None, alias="nativeEngineSummary"
    )

    model_config = ConfigDict(populate_by_name=True)


class GlutenExecutionFallback(BaseModel):
    """Per-SQL-execution Gluten offload outcome: how many plan nodes ran
    natively vs fell back, and why each fallback node fell back."""

    execution_id: Optional[int] = Field(None, alias="executionId")
    description: Optional[str] = None
    num_gluten_nodes: Optional[int] = Field(None, alias="numGlutenNodes")
    num_fallback_nodes: Optional[int] = Field(None, alias="numFallbackNodes")
    fallback_description: Optional[str] = Field(None, alias="fallbackDescription")
    fallback_node_to_reason: Dict[str, str] = Field(
        default_factory=dict, alias="fallbackNodeToReason"
    )

    model_config = ConfigDict(populate_by_name=True)


class GlutenFallbackReasons(BaseModel):
    """Gluten fallback reasons for an app, plus where they came from.

    ``source`` is ``"history_server_plugin"`` when read from the Gluten UI
    plugin's ``/gluten/json/`` endpoint (cheap, no log download) or
    ``"event_log"`` when the History Server lacks the plugin and the raw
    ``GlutenPlanFallbackEvent`` records were read instead.
    """

    app_id: str = Field(..., alias="appId")
    source: str
    build_info: Dict[str, str] = Field(default_factory=dict, alias="buildInfo")
    executions: List[GlutenExecutionFallback] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)
