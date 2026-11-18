"""Tests for scan-node introspection: BigQuery metrics and filter/column
pushdown, native and vanilla.

Plan-text fixtures below are trimmed, faithful fragments of REAL plan text
captured from two production event logs during development of this module:
a TPC-H Parquet+Gluten run (native/vanilla "Scan(Transformer)? parquet
<table>" with PushedFilters) and a TPC-DS BigQuery+Gluten run ("Reading
table [...], Read session Id : ..."). The tree/appendix split and the
"JSON nodeId != text (id)" numbering mismatch were both confirmed against
real plans, not assumed -- see the module docstring in scan_nodes.py.
"""

import unittest
from unittest.mock import MagicMock, patch

from spark_history_mcp.tools.scan_nodes import (
    _build_scan_node_detail,
    _extract_bracket_after,
    _filter_list,
    _split_top_level,
    collect_scan_nodes,
    get_scan_node_details,
)

# Real fragment: a native Parquet scan with PushedFilters (from the TPC-H
# Gluten run), reachable from a tree that references only id 1.
_NATIVE_PARQUET_PLAN = """== Physical Plan ==
   +- ^ ScanTransformer parquet tpch_1tb_parquet_p_decimal.part (1)

(1) ScanTransformer parquet tpch_1tb_parquet_p_decimal.part
Output [3]: [p_partkey#18L, p_container#23, p_brand#26]
Batched: true
Location: InMemoryFileIndex [gs://dp-tpc-dbgen-dbx/tpch_1tb_parquet_p_decimal/tpch/sf1000_parquet/part]
PushedFilters: [IsNotNull(p_container), EqualTo(p_container,MED BOX), IsNotNull(p_partkey)]
ReadSchema: struct<p_partkey:bigint,p_container:string,p_brand:string>
"""

# Real fragment: a vanilla Parquet scan, same table shape.
_VANILLA_PARQUET_PLAN = """== Physical Plan ==
   +- Scan parquet tpch_1tb_parquet_p_decimal.supplier (1)

(1) Scan parquet tpch_1tb_parquet_p_decimal.supplier
Output [2]: [s_suppkey#7L, s_nationkey#10L]
Batched: true
Location: InMemoryFileIndex [gs://dp-tpc-dbgen-dbx/tpch_1tb_parquet_p_decimal/tpch/sf1000_parquet/supplier]
PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)]
DataFilters: [isnotnull(s_suppkey#7L), isnotnull(s_nationkey#10L)]
ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
"""

# Real fragment: a native BigQuery scan (from the TPC-DS Gluten+BQ run) --
# no PushedFilters/Location at all, identity only in "Reading table [...]".
_BIGQUERY_PLAN = """== Physical Plan ==
   +- ^ BatchScanExecTransformer (1)

(1) BatchScanExecTransformer
Output [8]: [c_customer_sk#202L, c_customer_id#203]
Reading table [rising-woods-374602.tpcds001tb_parquet_p_decimal_v2.customer], Read session Id : projects/rising-woods-374602/locations/us/sessions/CAISDFpYZnlVSFJnV2h3ORoCamMaAm55
"""


def _node(node_id, node_name, metrics):
    n = MagicMock()
    n.node_id = node_id
    n.node_name = node_name
    n.metrics = []
    for name, value in metrics:
        m = MagicMock()
        m.name = name
        m.value = value
        n.metrics.append(m)
    return n


class TestBracketAndFilterHelpers(unittest.TestCase):
    def test_extract_bracket_after_respects_nesting(self):
        text = "PushedFilters: [In(a, [1, 2, 3]), IsNotNull(b)]\nReadSchema: ..."
        self.assertEqual(
            _extract_bracket_after(text, "PushedFilters"),
            "In(a, [1, 2, 3]), IsNotNull(b)",
        )

    def test_extract_bracket_after_missing_label(self):
        self.assertIsNone(_extract_bracket_after("no filters here", "PushedFilters"))

    def test_split_top_level_respects_nested_commas(self):
        self.assertEqual(
            _split_top_level("EqualTo(a,b), In(c, [1,2]), IsNotNull(d)"),
            ["EqualTo(a,b)", "In(c, [1,2])", "IsNotNull(d)"],
        )

    def test_filter_list_empty_when_absent(self):
        self.assertEqual(_filter_list("nothing to see", "PushedFilters"), [])


class TestBuildScanNodeDetail(unittest.TestCase):
    def test_non_scan_node_returns_none(self):
        self.assertIsNone(_build_scan_node_detail(1, 5, "Project", "irrelevant", {}))

    def test_native_parquet_scan_extracts_pushed_filters_and_location(self):
        block = (
            "Output [3]: [p_partkey#18L]\n"
            "Batched: true\n"
            "Location: InMemoryFileIndex [gs://bucket/path/part]\n"
            "PushedFilters: [IsNotNull(p_container), EqualTo(p_container,MED BOX)]\n"
        )
        detail = _build_scan_node_detail(
            7, 1, "ScanTransformer parquet tpch.part", block, {}
        )
        self.assertIsNotNone(detail)
        self.assertTrue(detail.is_native)
        self.assertEqual(detail.source_kind, "file")
        self.assertEqual(detail.format, "parquet")
        self.assertEqual(detail.table, "tpch.part")
        self.assertEqual(detail.location, "gs://bucket/path/part")
        self.assertEqual(
            detail.pushed_filters,
            ["IsNotNull(p_container)", "EqualTo(p_container,MED BOX)"],
        )

    def test_bigquery_scan_extracts_table_and_session_no_filters(self):
        block = (
            "Output [2]: [c_customer_sk#202L]\n"
            "Reading table [proj.ds.customer], "
            "Read session Id : projects/proj/locations/us/sessions/abc123\n"
        )
        detail = _build_scan_node_detail(24, 1, "BatchScanExecTransformer", block, {})
        self.assertIsNotNone(detail)
        self.assertTrue(detail.is_native)
        self.assertEqual(detail.source_kind, "bigquery")
        self.assertEqual(detail.table, "proj.ds.customer")
        self.assertEqual(
            detail.read_session_id,
            "projects/proj/locations/us/sessions/abc123",
        )
        self.assertEqual(detail.pushed_filters, [])

    def test_metrics_attached_only_when_name_unambiguous(self):
        metrics_by_name = {
            "ScanTransformer parquet tpch.part": [{"number of output rows": "5"}]
        }
        detail = _build_scan_node_detail(
            1, 1, "ScanTransformer parquet tpch.part", "Output [1]: [x]\n", metrics_by_name
        )
        self.assertEqual(detail.metrics, {"number of output rows": "5"})

    def test_metrics_empty_when_name_ambiguous(self):
        metrics_by_name = {
            "BatchScanExecTransformer": [
                {"number of output rows": "5"},
                {"number of output rows": "9"},
            ]
        }
        detail = _build_scan_node_detail(
            1, 1, "BatchScanExecTransformer", "Reading table [a.b.c]\n", metrics_by_name
        )
        self.assertEqual(detail.metrics, {})


class TestCollectScanNodes(unittest.TestCase):
    def test_native_parquet_end_to_end(self):
        results = collect_scan_nodes(
            1,
            [_node(99, "ScanTransformer parquet tpch_1tb_parquet_p_decimal.part", [("number of output rows", "5")])],
            _NATIVE_PARQUET_PLAN,
        )
        self.assertEqual(len(results), 1)
        r = results[0]
        # node_id comes from the TEXT's own numbering (1), not the JSON
        # node's id (99) -- they're independent namespaces.
        self.assertEqual(r.node_id, 1)
        self.assertTrue(r.is_native)
        self.assertEqual(r.source_kind, "file")
        self.assertEqual(r.table, "tpch_1tb_parquet_p_decimal.part")
        self.assertIn("IsNotNull(p_container)", r.pushed_filters)
        self.assertEqual(r.metrics, {"number of output rows": "5"})

    def test_vanilla_parquet_end_to_end(self):
        results = collect_scan_nodes(2, [], _VANILLA_PARQUET_PLAN)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertFalse(r.is_native)
        self.assertEqual(r.data_filters, ["isnotnull(s_suppkey#7L)", "isnotnull(s_nationkey#10L)"])

    def test_bigquery_end_to_end(self):
        results = collect_scan_nodes(24, [], _BIGQUERY_PLAN)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r.source_kind, "bigquery")
        self.assertEqual(
            r.table, "rising-woods-374602.tpcds001tb_parquet_p_decimal_v2.customer"
        )
        self.assertTrue(r.read_session_id.startswith("projects/"))

    def test_stale_appendix_entry_not_referenced_by_tree_is_excluded(self):
        """A dead vanilla "Scan parquet ..." entry left in the shared
        appendix by a stripped stale Initial-Plan tree (id 2 here) must not
        be reported -- only id 1, which the retained tree still points at."""
        plan = (
            "== Physical Plan ==\n"
            "   +- ^ ScanTransformer parquet t.nation (1)\n"
            "\n"
            "(1) ScanTransformer parquet t.nation\n"
            "Output [1]: [x]\n"
            "PushedFilters: [IsNotNull(x)]\n"
            "\n"
            "(2) Scan parquet t.nation\n"
            "Output [1]: [x]\n"
            "PushedFilters: [IsNotNull(x)]\n"
        )
        results = collect_scan_nodes(1, [], plan)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].node_id, 1)
        self.assertTrue(results[0].is_native)


class TestGetScanNodeDetailsTool(unittest.TestCase):
    @patch("spark_history_mcp.tools.scan_nodes.get_client_or_default")
    def test_scoped_to_one_execution(self, mock_get_client):
        mock_client = MagicMock()
        execution = MagicMock()
        execution.nodes = []
        execution.plan_description = _NATIVE_PARQUET_PLAN
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_scan_node_details("app-1", execution_id=7, ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].sql_execution_id, 7)
        mock_client.get_sql_list.assert_not_called()
        mock_client.get_sql_execution.assert_called_once_with(
            app_id="app-1",
            execution_id=7,
            app_attempt_id=None,
            details=True,
            plan_description=True,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.scan_nodes.get_client_or_default")
    def test_scans_slowest_executions_when_no_execution_id(
        self, mock_get_client, mock_get_client_tools
    ):
        mock_client = MagicMock()
        sql = MagicMock()
        sql.id = 3
        sql.duration = 500
        sql.status = "COMPLETED"
        sql.description = "q"
        sql.submission_time = "2025-08-05T00:23:38.607GMT"
        sql.success_job_ids = []
        sql.failed_job_ids = []
        sql.running_job_ids = []
        mock_client.get_sql_list.return_value = [sql]

        execution = MagicMock()
        execution.nodes = []
        execution.plan_description = _BIGQUERY_PLAN
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client
        mock_get_client_tools.return_value = mock_client

        result = get_scan_node_details("app-1", top_n=5, ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].source_kind, "bigquery")


if __name__ == "__main__":
    unittest.main()
