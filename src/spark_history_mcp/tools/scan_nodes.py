"""Scan (leaf/read) operator introspection: BigQuery connector metrics and
filter/column pushdown, across both native (Gluten/Velox) and vanilla Spark
scan operators.

Field names and plan-text formats here (``PushedFilters:``/``DataFilters:``/
``PartitionFilters:``/``Location:`` for file sources, ``Reading table
[...], Read session Id : ...`` for the BigQuery connector, and the very
different vanilla-vs-native BigQuery metric vocabularies) were verified
against real event logs, not guessed:

* A vanilla (non-Gluten) BigQuery scan reports exactly six connector metrics
  (``number of BQ bytes read``, ``number of BQ rows read``, ``scan time for
  BQ``, ``parsing time for BQ``, ``time spent in spark``,
  ``number of read streams`` -- see spark-bigquery-connector's
  ``SparkBigQueryCustomMetricConstants``).
* Once Gluten offloads that same scan to Velox, those six names disappear
  entirely -- the node instead reports a much larger, differently-named set
  of native metrics (e.g. ``number of raw input bytes``, ``number of bytes
  used for storing bq message in memory``, ``peak memory bytes``, ...).

Because the two vocabularies don't overlap and neither is exhaustive, this
module reports each scan node's metrics as a raw passthrough dict rather
than mapping them to fixed fields.
"""

from mcp.types import ToolAnnotations
from mcp.server.mcpserver import Context
import re
from typing import Dict, List, Optional

from spark_history_mcp.core.app import tool
from spark_history_mcp.models.mcp_types import ScanNodeDetail
from spark_history_mcp.tools.tools import (
    _is_native_node,
    _strip_initial_plans,
    get_client_or_default,
    list_sql_executions,
)

# The JSON "nodeId" returned by the REST API's execution.nodes (assigned by
# SparkPlanGraph, for the metrics-graph UI) and the "(N)" operator numbers in
# physicalPlanDescription's text (assigned separately by
# QueryPlan/ExplainUtils, for verbose EXPLAIN output) are two INDEPENDENT
# numbering schemes -- verified against a real plan where JSON node id 38
# was a ScanTransformer, but "(38)" in that same execution's plan text was an
# unrelated WholeStageCodegenTransformer. So this module walks the text's own
# "(id) name\n<block>" sections (self-consistent) rather than looking up a
# JSON node_id in the text, and only attaches a JSON node's metrics when its
# node_name is unambiguous (not shared by more than one scan in this plan).
_NODE_BLOCK_RE = re.compile(r"^\((\d+)\) ([^\n]+)\n(.*?)(?=\n\(\d+\) |\Z)", re.MULTILINE | re.DOTALL)

_READING_TABLE_RE = re.compile(
    r"Reading table \[([^\]]+)\](?:, Read session Id\s*:\s*(\S+))?"
)
# Vanilla and native file-source scan nodes name themselves
# "Scan <format> <table>" / "ScanTransformer <format> <table>" (e.g. "Scan
# parquet tpch.nation"); DSv2 sources without that convention (BigQuery,
# Iceberg, ...) just say "BatchScan"/"BatchScanTransformer"/
# "BatchScanExecTransformer" and carry their identity in the plan text
# instead (see _READING_TABLE_RE for BigQuery's form).
# Gluten's native file scan is named "FileSourceScanExecTransformer <fmt>
# <table>" (verified on Spark 3.3 and 3.5 apps), not "ScanTransformer".
_FILE_SCAN_NAME_RE = re.compile(
    r"^(?:Scan|ScanTransformer|FileSourceScanExecTransformer|NativeScan)"
    r"\s+(\S+)\s+(\S+?)(?:\s+\[codegen id : \d+\])?$"
)


def _extract_bracket_after(text: str, label: str) -> Optional[str]:
    """Return the ``[...]`` content following ``label: `` in ``text``,
    respecting nested brackets (e.g. an ``In(col, [1, 2, 3])`` filter).

    The bracket doesn't always start immediately after the label -- e.g.
    ``Location: InMemoryFileIndex [path]`` has a class name in between --
    so this finds the label, then the next ``[`` after it, not requiring
    adjacency.
    """
    marker = f"{label}: "
    label_idx = text.find(marker)
    if label_idx == -1:
        return None
    start = text.find("[", label_idx + len(marker))
    if start == -1:
        return None
    start += 1
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
        i += 1
    return text[start : i - 1]


def _split_top_level(text: str) -> List[str]:
    """Split ``text`` on top-level commas, respecting ``()``/``[]`` nesting
    (e.g. ``EqualTo(a,b), In(c, [1,2])`` -> two items, not four)."""
    parts: List[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(text):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _filter_list(text: str, label: str) -> List[str]:
    bracket = _extract_bracket_after(text, label)
    if not bracket:
        return []
    return _split_top_level(bracket)


def _metrics_by_node_name(nodes) -> Dict[str, List[Dict[str, str]]]:
    """Group JSON execution.nodes metrics by node_name, so they can be
    attached to a text-block scan by name (see module docstring on why not
    by id)."""
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for n in nodes or []:
        if not n.node_name or not n.metrics:
            continue
        metrics = {m.name: " ".join((m.value or "").split()) for m in n.metrics}
        grouped.setdefault(n.node_name, []).append(metrics)
    return grouped


def _build_scan_node_detail(
    execution_id: Optional[int],
    text_node_id: int,
    node_name: str,
    block: str,
    metrics_by_name: Dict[str, List[Dict[str, str]]],
) -> Optional[ScanNodeDetail]:
    if "Scan" not in node_name:
        return None

    is_native = _is_native_node(node_name)

    # Only attach metrics when this exact node_name is unambiguous in this
    # plan -- a table scanned more than once (self-join, reused subquery)
    # would otherwise get another instance's metrics silently misattributed.
    candidates = metrics_by_name.get(node_name) or []
    metrics = candidates[0] if len(candidates) == 1 else {}

    source_kind: Optional[str] = None
    fmt: Optional[str] = None
    table: Optional[str] = None
    location: Optional[str] = None
    read_session_id: Optional[str] = None

    reading_table = _READING_TABLE_RE.search(block)
    file_scan = _FILE_SCAN_NAME_RE.match(node_name)
    if reading_table:
        source_kind = "bigquery"
        table = reading_table.group(1)
        read_session_id = reading_table.group(2)
    elif file_scan:
        source_kind = "file"
        fmt, table = file_scan.group(1), file_scan.group(2)
        # Location's own leading "<IndexClassName> [" isn't captured by the
        # generic bracket helper (it anchors on "Location: ["), so recover
        # just the first path entry for a compact hint.
        location_bracket = _extract_bracket_after(block, "Location")
        if location_bracket:
            entries = _split_top_level(location_bracket)
            location = entries[0] if entries else None

    return ScanNodeDetail(
        sql_execution_id=execution_id,
        node_id=text_node_id,
        node_name=node_name,
        is_native=is_native,
        source_kind=source_kind,
        format=fmt,
        table=table,
        location=location,
        read_session_id=read_session_id,
        pushed_filters=_filter_list(block, "PushedFilters"),
        data_filters=_filter_list(block, "DataFilters"),
        partition_filters=_filter_list(block, "PartitionFilters"),
        metrics=metrics,
    )


def collect_scan_nodes(execution_id: Optional[int], nodes, plan_text: str) -> List[ScanNodeDetail]:
    """Build ScanNodeDetail entries for every scan-like operator in a plan.

    Walks physicalPlanDescription's own "(id) name\\n<block>" sections
    (self-consistent numbering) rather than the JSON nodes[].node_id, and
    attaches each JSON node's metrics by exact node_name match when that
    name is unambiguous in this plan.

    The trailing numbered listing is a single, deduplicated appendix shared
    across BOTH the current and (already-stripped) stale "Initial Plan"
    tree, so it can still contain dead entries for operators the retained
    tree no longer references (e.g. the pre-Gluten-offload vanilla "Scan
    parquet ..." for a table now read by native "ScanTransformer parquet
    ..." instead) -- confirmed by comparing a real plan's tree-referenced
    ids against its appendix ids. Only ids that actually appear in the
    retained tree are kept.
    """
    plan_text = _strip_initial_plans(plan_text)
    # Formatted explain puts subquery trees (DPP, scalar subqueries) in a
    # trailing "===== Subqueries =====" section, each "Subquery:K ..." with
    # its own tree followed by its own (N) blocks. Treat the main plan and
    # every subquery as separate tree+blocks groups, or subquery scans are
    # dropped (their ids never appear in the main tree).
    parts = re.split(r"\n=+ Subqueries =+\n", plan_text, maxsplit=1)
    main, subq_text = parts[0], (parts[1] if len(parts) > 1 else "")
    groups = [main] + [g for g in re.split(r"\n(?=Subquery:\d+)", subq_text) if g.strip()]

    referenced_ids: set = set()
    blocks = []
    for group in groups:
        first_detail = _NODE_BLOCK_RE.search(group)
        tree_text = group[: first_detail.start()] if first_detail else group
        referenced_ids |= {int(n) for n in re.findall(r"\((\d+)\)", tree_text)}
        blocks.extend(_NODE_BLOCK_RE.finditer(group))

    metrics_by_name = _metrics_by_node_name(nodes)
    result: List[ScanNodeDetail] = []
    for m in blocks:
        if int(m.group(1)) not in referenced_ids:
            continue
        detail = _build_scan_node_detail(
            execution_id, int(m.group(1)), m.group(2), m.group(3), metrics_by_name
        )
        if detail is not None:
            result.append(detail)
    return result


@tool(annotations=ToolAnnotations(readOnlyHint=True))
def get_scan_node_details(
    app_id: str,
    server: Optional[str] = None,
    app_attempt_id: Optional[str] = None,
    execution_id: Optional[int] = None,
    top_n: int = 10,
    *,
    ctx: Context,
) -> List[ScanNodeDetail]:
    """
    Inspect scan (table read) operators in an application's SQL execution(s):
    BigQuery connector read metrics, and filter/column pushdown for
    file-based sources -- across both native (Gluten/Velox) and vanilla Spark
    scans.

    Each scan's ``metrics`` is a raw passthrough (name -> formatted value)
    rather than a fixed set of fields: a vanilla BigQuery scan reports six
    connector metrics (bytes/rows read, scan/parse time, time in Spark,
    number of read streams), but once Gluten offloads that same scan to
    Velox those disappear and a much larger, differently-named set of native
    metrics appears instead (e.g. "number of raw input bytes", "number of
    bytes used for storing bq message in memory", "peak memory bytes") --
    verified against real event logs, not assumed.

    ``pushed_filters``/``data_filters``/``partition_filters`` are populated
    for file-based scans (Parquet/ORC/etc, native or vanilla) straight from
    the plan text. The BigQuery connector does not print pushdown
    information in the plan text at all, so these stay empty for BigQuery
    scans regardless of whether pushdown actually happened -- that's a gap
    in what the connector exposes, not a detection failure here.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        app_attempt_id: Optional YARN application attempt ID
        execution_id: Optional SQL execution ID to scope to. When omitted,
            the ``top_n`` slowest SQL executions are scanned instead.
        top_n: Number of slowest SQL executions to scan when execution_id is
            not given (default: 10)

    Returns:
        List of ScanNodeDetail, one per scan operator found
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

    results: List[ScanNodeDetail] = []
    for sql_id in sql_ids:
        execution = client.get_sql_execution(
            app_id=app_id,
            execution_id=sql_id,
            app_attempt_id=app_attempt_id,
            details=True,
            plan_description=True,
        )
        results.extend(
            collect_scan_nodes(sql_id, execution.nodes, execution.plan_description or "")
        )
    return results
