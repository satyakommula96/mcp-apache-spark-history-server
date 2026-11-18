"""Tests for Gluten/Velox native-engine event-log parsing and Bloom filter
ownership detection.

The event-log fixtures below (zip layout, event field names) and the
"legacy" plan-text fixture were verified against a real Gluten/DPAS event
log (application_1785742137670_0001, a TPC-DS query with Full Query
Fallback and Bloom filters enabled) -- not hand-guessed.
"""

import gzip
import io
import unittest
import zipfile
from unittest.mock import MagicMock, patch

from spark_history_mcp.tools.native_engine import (
    _build_fallback_summary,
    _nanos_to_ms,
    _plan_cost,
    detect_bloom_filter_ownership,
    get_bloom_filter_ownership,
    get_native_engine_events,
    get_native_planning_overhead,
    get_query_fallback_summary,
    iter_event_log_records,
)


def _zip_bytes(files: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class TestIterEventLogRecords(unittest.TestCase):
    def test_flat_file_skips_blank_and_malformed_lines(self):
        content = (
            b'{"Event":"SparkListenerLogStart","Spark Version":"3.5.2"}\n'
            b"\n"
            b"not json\n"
            b'{"Event":"SparkListenerJobStart","Job Id":1}\n'
        )
        zip_bytes = _zip_bytes({"application_123": content})

        records = list(iter_event_log_records(zip_bytes))

        self.assertEqual([r["Event"] for r in records], [
            "SparkListenerLogStart",
            "SparkListenerJobStart",
        ])

    def test_rolling_v2_reads_numbered_parts_in_order_and_skips_appstatus(self):
        zip_bytes = _zip_bytes(
            {
                "eventlog_v2_app1/": "",
                "eventlog_v2_app1/events_2_app1": b'{"Event":"Second"}\n',
                "eventlog_v2_app1/events_1_app1": b'{"Event":"First"}\n',
                "eventlog_v2_app1/appstatus_app1": b"",
            }
        )

        records = list(iter_event_log_records(zip_bytes))

        self.assertEqual([r["Event"] for r in records], ["First", "Second"])

    def test_gzip_compressed_file_is_decompressed(self):
        raw = b'{"Event":"SparkListenerLogStart"}\n'
        zip_bytes = _zip_bytes({"application_123": gzip.compress(raw)})

        records = list(iter_event_log_records(zip_bytes))

        self.assertEqual(records, [{"Event": "SparkListenerLogStart"}])


class TestPlanCostHelpers(unittest.TestCase):
    def test_plan_cost_parses_and_defaults_to_none(self):
        self.assertEqual(_plan_cost("123.5"), 123.5)
        self.assertIsNone(_plan_cost(None))
        self.assertIsNone(_plan_cost("not-a-number"))

    def test_nanos_to_ms(self):
        self.assertEqual(_nanos_to_ms(1_000_000), 1.0)
        self.assertIsNone(_nanos_to_ms(None))


class TestBuildFallbackSummary(unittest.TestCase):
    def test_no_events_means_fqf_disabled(self):
        summary = _build_fallback_summary(0, [], [])
        self.assertFalse(summary.fqf_enabled)
        self.assertFalse(summary.fell_back)
        self.assertIsNone(summary.selected_engine)

    def test_root_cost_event_selects_gluten_when_cheaper_and_no_fallback(self):
        cost_events = [
            {
                "scopeType": {"type": "root"},
                "vanillaPlanCost": 111911054.37,
                "glutenPlanCost": 73600612.13,
                "fallbackSummary": [],
            }
        ]
        summary = _build_fallback_summary(24, [], cost_events)
        self.assertTrue(summary.fqf_enabled)
        self.assertEqual(summary.selected_engine, "gluten")
        self.assertEqual(summary.vanilla_plan_cost, 111911054.37)
        self.assertEqual(summary.gluten_plan_cost, 73600612.13)

    def test_fallback_summary_forces_vanilla_even_if_gluten_cheaper(self):
        cost_events = [
            {
                "scopeType": {"type": "root"},
                "vanillaPlanCost": 100.0,
                "glutenPlanCost": 10.0,
                "fallbackSummary": ["some unsupported expression"],
            }
        ]
        summary = _build_fallback_summary(1, [], cost_events)
        self.assertEqual(summary.selected_engine, "vanilla")
        self.assertTrue(summary.fell_back)

    def test_non_root_scope_events_are_ignored_for_selection(self):
        cost_events = [
            {
                "scopeType": {"type": "subquery", "id": "subquery#1"},
                "vanillaPlanCost": 1.0,
                "glutenPlanCost": 2.0,
                "fallbackSummary": [],
            }
        ]
        summary = _build_fallback_summary(1, [], cost_events)
        self.assertFalse(summary.fqf_enabled)
        self.assertIsNone(summary.selected_engine)

    def test_fallback_events_merge_node_reasons_and_flag_fallback(self):
        fallback_events = [
            {"numFallbackNodes": 0, "fallbackNodeToReason": {}},
            {
                "numFallbackNodes": 2,
                "fallbackNodeToReason": {"001 Filter": "Gluten does not support it"},
            },
        ]
        summary = _build_fallback_summary(5, fallback_events, [])
        self.assertTrue(summary.fell_back)
        self.assertEqual(summary.fallback_events, 2)
        self.assertEqual(
            summary.fallback_node_reasons,
            {"001 Filter": "Gluten does not support it"},
        )


# Minimal, self-contained fragment of a REAL Gluten/DPAS event log's
# physicalPlanDescription (from application_1785742137670_0001, a TPC-DS
# query with Bloom filters), trimmed down to just the hosting Filter operator
# and its bloom_filter_agg-producing subquery. Both sides are plain Spark
# operators (no "Transformer" suffix) -> spark/spark ownership.
_REAL_LEGACY_SPARK_SPARK_PLAN = """== Physical Plan ==
Filter (2)

(2) Filter
Input [12]: [ss_item_sk#561L, ss_customer_sk#562L]
Condition : (isnotnull(ss_item_sk#561L) AND might_contain(Subquery subquery#1855, [id=#821], cast(ss_item_sk#561L as bigint)))

===== Subqueries =====

Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery subquery#1855, [id=#821]
AdaptiveSparkPlan (251)
+- ObjectHashAggregate (250)
   +- Exchange (249)
      +- ObjectHashAggregate (248)
         +- Project (247)
            +- Filter (246)
               +- BatchScan item (245)


(245) BatchScan item
Output [4]: [i_item_sk#362L]

(246) Filter
Input [4]: [i_item_sk#362L]
Condition : isnotnull(i_item_sk#362L)

(247) Project
Output [1]: [i_item_sk#362L]

(248) ObjectHashAggregate
Functions [1]: [partial_bloom_filter_agg(i_item_sk#362L, 1000000, 8388608, 0, 0)]

(249) Exchange
Arguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=749]

(250) ObjectHashAggregate
Functions [1]: [bloom_filter_agg(i_item_sk#362L, 1000000, 8388608, 0, 0)]
"""


class TestDetectBloomFilterOwnership(unittest.TestCase):
    def test_not_used_when_no_bloom_expressions(self):
        result = detect_bloom_filter_ownership("== Physical Plan ==\nProject (1)\n")
        self.assertEqual(result, {"used": False})

    def test_real_legacy_plan_fragment_classifies_spark_spark(self):
        result = detect_bloom_filter_ownership(_REAL_LEGACY_SPARK_SPARK_PLAN)
        self.assertTrue(result["used"])
        self.assertEqual(result["producer"], "spark")
        self.assertEqual(result["consumer"], "spark")
        self.assertIn("2:Filter", result["hosts"])

    def test_real_legacy_plan_fragment_velox_when_transformer_suffix(self):
        plan = _REAL_LEGACY_SPARK_SPARK_PLAN.replace(
            "(2) Filter", "(2) FilterExecTransformer"
        ).replace("Filter (2)", "FilterExecTransformer (2)")
        result = detect_bloom_filter_ownership(plan)
        self.assertTrue(result["used"])
        self.assertEqual(result["consumer"], "velox")
        # Producer side untouched -> still spark.
        self.assertEqual(result["producer"], "spark")

    def test_bloom_expressions_without_subquery_marker_reports_note(self):
        plan = (
            "== Physical Plan ==\n"
            "Filter (1)\n\n"
            "(1) Filter\n"
            "Condition: might_contain(x) AND bloom_filter_agg(y)\n"
        )
        result = detect_bloom_filter_ownership(plan)
        self.assertTrue(result["used"])
        self.assertIsNotNone(result["note"])
        self.assertNotIn("producer", result)

    def test_missing_hosting_operator_reports_note_not_raise(self):
        plan = _REAL_LEGACY_SPARK_SPARK_PLAN.replace(
            "Hosting operator id = 2", "Hosting operator id = 999"
        )
        result = detect_bloom_filter_ownership(plan)
        self.assertTrue(result["used"])
        self.assertIsNotNone(result["note"])


class TestNativeEngineTools(unittest.TestCase):
    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_native_engine_events_filters_prefix_type_and_execution(
        self, mock_get_client
    ):
        mock_client = MagicMock()
        content = (
            b'{"Event":"SparkListenerJobStart","Job Id":1}\n'
            b'{"Event":"org.apache.gluten.events.GlutenBuildInfoEvent","info":{"a":1}}\n'
            b'{"Event":"org.apache.gluten.events.DPQueryCostDecisionEvent","executionId":1,"vanillaPlanCost":1.0}\n'
            b'{"Event":"org.apache.gluten.events.DPQueryCostDecisionEvent","executionId":2,"vanillaPlanCost":2.0}\n'
        )
        mock_client.download_event_log.return_value = _zip_bytes(
            {"app-1": content}
        )
        mock_get_client.return_value = mock_client

        result = get_native_engine_events(
            "app-1", event_type="DPQueryCostDecisionEvent", execution_id=1, ctx=MagicMock()
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].event_type, "DPQueryCostDecisionEvent")
        self.assertEqual(result[0].fields["executionId"], 1)
        mock_client.download_event_log.assert_called_once_with(
            "app-1", app_attempt_id=None
        )

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_query_fallback_summary_groups_by_execution(self, mock_get_client):
        mock_client = MagicMock()
        content = (
            b'{"Event":"org.apache.gluten.events.GlutenPlanFallbackEvent",'
            b'"executionId":7,"numGlutenNodes":1,"numFallbackNodes":0,'
            b'"fallbackNodeToReason":{}}\n'
            b'{"Event":"org.apache.gluten.events.DPQueryCostDecisionEvent",'
            b'"executionId":7,"scopeType":{"type":"root"},'
            b'"vanillaPlanCost":10.0,"glutenPlanCost":5.0,"fallbackSummary":[]}\n'
        )
        mock_client.download_event_log.return_value = _zip_bytes(
            {"app-1": content}
        )
        mock_get_client.return_value = mock_client

        result = get_query_fallback_summary("app-1", ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].execution_id, 7)
        self.assertTrue(result[0].fqf_enabled)
        self.assertEqual(result[0].selected_engine, "gluten")

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_native_planning_overhead_converts_nanos_to_ms(
        self, mock_get_client
    ):
        mock_client = MagicMock()
        content = (
            b'{"Event":"org.apache.gluten.events.DPDecisionOverheadEvent",'
            b'"executionId":3,"scopeType":{"type":"root"},'
            b'"compareSnapshotBuildNanos":7888273505,"evaluationNanos":null,'
            b'"fallbackBuildNanos":null,"glutenBuildNanos":29769814,'
            b'"planningTotalNanos":null}\n'
        )
        mock_client.download_event_log.return_value = _zip_bytes(
            {"app-1": content}
        )
        mock_get_client.return_value = mock_client

        result = get_native_planning_overhead("app-1", ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].execution_id, 3)
        self.assertEqual(result[0].scope_type, "root")
        self.assertAlmostEqual(result[0].compare_snapshot_build_ms, 7888.273505)
        self.assertAlmostEqual(result[0].gluten_build_ms, 29.769814)
        self.assertIsNone(result[0].evaluation_ms)
        self.assertIsNone(result[0].memo)

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_native_planning_overhead_parses_memo_stats(self, mock_get_client):
        """Real payload confirmed against a production TPC-DS/Gluten event
        log (application_1788905038595_0001, query q11): misses ==
        entryCount, hit rate ~87%."""
        mock_client = MagicMock()
        content = (
            b'{"Event":"org.apache.gluten.events.DPDecisionOverheadEvent",'
            b'"executionId":24,"scopeType":{"type":"root"},'
            b'"compareSnapshotBuildNanos":null,"evaluationNanos":null,'
            b'"fallbackBuildNanos":null,"glutenBuildNanos":null,'
            b'"planningTotalNanos":null,'
            b'"nativeValidationMemo":{"entryCount":48,"retainedBytes":13091,'
            b'"hits":317,"misses":48,"boundRejections":0}}\n'
        )
        mock_client.download_event_log.return_value = _zip_bytes({"app-1": content})
        mock_get_client.return_value = mock_client

        result = get_native_planning_overhead("app-1", ctx=MagicMock())

        self.assertEqual(len(result), 1)
        memo = result[0].memo
        self.assertIsNotNone(memo)
        self.assertEqual(memo.entry_count, 48)
        self.assertEqual(memo.retained_bytes, 13091)
        self.assertEqual(memo.hits, 317)
        self.assertEqual(memo.misses, 48)
        self.assertEqual(memo.bound_rejections, 0)
        self.assertAlmostEqual(memo.hit_rate, 317 / 365, places=6)

    def test_native_validation_memo_hit_rate_none_when_both_zero(self):
        from spark_history_mcp.models.mcp_types import NativeValidationMemoStats

        memo = NativeValidationMemoStats(hits=0, misses=0)
        self.assertIsNone(memo.hit_rate)

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_bloom_filter_ownership_scoped_to_one_execution(
        self, mock_get_client
    ):
        mock_client = MagicMock()
        execution = MagicMock()
        execution.plan_description = _REAL_LEGACY_SPARK_SPARK_PLAN
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_bloom_filter_ownership("app-1", execution_id=42, ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].execution_id, 42)
        self.assertTrue(result[0].used)
        self.assertEqual(result[0].producer, "spark")
        self.assertEqual(result[0].consumer, "spark")
        # execution_id given -> the slowest-executions scan (get_sql_list) is skipped.
        mock_client.get_sql_list.assert_not_called()
        mock_client.get_sql_execution.assert_called_once_with(
            app_id="app-1",
            execution_id=42,
            app_attempt_id=None,
            details=True,
            plan_description=True,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_get_bloom_filter_ownership_scans_slowest_executions(
        self, mock_get_client, mock_get_client_tools
    ):
        # get_bloom_filter_ownership calls tools.list_sql_executions() to find
        # the slowest executions, which resolves its OWN client via
        # tools.get_client_or_default -- both need patching to the same mock.
        mock_client = MagicMock()
        sql = MagicMock()
        sql.id = 9
        sql.duration = 1000
        sql.status = "COMPLETED"
        sql.description = "q"
        sql.submission_time = "2025-08-05T00:23:38.607GMT"
        sql.success_job_ids = []
        sql.failed_job_ids = []
        sql.running_job_ids = []
        mock_client.get_sql_list.return_value = [sql]

        execution = MagicMock()
        execution.plan_description = "== Physical Plan ==\nProject (1)\n"
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client
        mock_get_client_tools.return_value = mock_client

        result = get_bloom_filter_ownership("app-1", top_n=3, ctx=MagicMock())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].execution_id, 9)
        self.assertFalse(result[0].used)


if __name__ == "__main__":
    unittest.main()


def _fallback_log(*events) -> bytes:
    """Event-log zip containing the given GlutenPlanFallbackEvent payloads."""
    import json as _json

    lines = [
        _json.dumps({"Event": "org.apache.gluten.events.GlutenPlanFallbackEvent", **e})
        for e in events
    ]
    return _zip_bytes({"app-1": ("\n".join(lines) + "\n").encode()})


class TestEventLogCache(unittest.TestCase):
    def setUp(self):
        from spark_history_mcp.tools import native_engine as ne

        self.ne = ne
        ne.clear_event_log_cache()
        self.client = MagicMock()
        self.client.config.url = "http://shs"
        self.client.download_event_log.return_value = b"zipbytes"

    def _attempts(self, completed):
        attempt = MagicMock(attempt_id=None, completed=completed)
        self.client.get_application.return_value = MagicMock(attempts=[attempt])

    def test_completed_app_downloaded_once(self):
        self._attempts(True)
        for _ in range(3):
            self.assertEqual(
                self.ne.download_event_log_cached(self.client, "app-1"), b"zipbytes"
            )
        self.assertEqual(self.client.download_event_log.call_count, 1)

    def test_in_progress_app_never_cached(self):
        self._attempts(False)
        self.ne.download_event_log_cached(self.client, "app-1")
        self.ne.download_event_log_cached(self.client, "app-1")
        self.assertEqual(self.client.download_event_log.call_count, 2)

    def test_cache_keyed_by_server(self):
        self._attempts(True)
        other = MagicMock()
        other.config.url = "http://other"
        other.download_event_log.return_value = b"other"
        other.get_application.return_value = self.client.get_application.return_value
        self.ne.download_event_log_cached(self.client, "app-1")
        self.assertEqual(self.ne.download_event_log_cached(other, "app-1"), b"other")

    @patch.dict("os.environ", {"SPARK_MCP_EVENT_LOG_CACHE_MB": "0"})
    def test_zero_budget_disables_cache(self):
        self._attempts(True)
        self.ne.download_event_log_cached(self.client, "app-1")
        self.ne.download_event_log_cached(self.client, "app-1")
        self.assertEqual(self.client.download_event_log.call_count, 2)


class TestGetGlutenFallbackReasons(unittest.TestCase):
    def setUp(self):
        from spark_history_mcp.tools import native_engine as ne

        self.ne = ne
        ne.clear_event_log_cache()

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_uses_history_server_plugin_json(self, mock_get_client):
        client = MagicMock()
        client.get_gluten_ui_summary.return_value = {
            "buildInfo": {"Velox Revision": "abc"},
            "queries": {
                "count": 2,
                "executions": [
                    {"executionId": 0, "numGlutenNodes": 2, "numFallbackNodes": 1,
                     "fallbackNodeToReason": {"005 Exchange": "empty schema"}},
                    {"executionId": 1, "numGlutenNodes": 4, "numFallbackNodes": 0,
                     "fallbackNodeToReason": {}},
                ],
            },
        }
        mock_get_client.return_value = client
        r = self.ne.get_gluten_fallback_reasons("app-1", execution_id=0, ctx=MagicMock())
        self.assertEqual(r.source, "history_server_plugin")
        self.assertEqual(r.build_info, {"Velox Revision": "abc"})
        self.assertEqual(len(r.executions), 1)
        self.assertEqual(r.executions[0].fallback_node_to_reason, {"005 Exchange": "empty schema"})
        client.download_event_log.assert_not_called()

    @patch("spark_history_mcp.tools.native_engine.get_client_or_default")
    def test_falls_back_to_event_log_last_event_wins(self, mock_get_client):
        client = MagicMock()
        client.config.url = "http://shs"
        client.get_gluten_ui_summary.return_value = None
        client.get_application.return_value = MagicMock(attempts=[])
        client.download_event_log.return_value = _fallback_log(
            {"executionId": 0, "numGlutenNodes": 0, "numFallbackNodes": 2,
             "fallbackNodeToReason": {"001 Scan": "untouched", "002 Agg": "untouched"}},
            {"executionId": 0, "numGlutenNodes": 1, "numFallbackNodes": 1,
             "fallbackNodeToReason": {"002 Agg": "validation failed"}},
        )
        mock_get_client.return_value = client
        r = self.ne.get_gluten_fallback_reasons("app-1", ctx=MagicMock())
        self.assertEqual(r.source, "event_log")
        self.assertEqual(len(r.executions), 1)
        e = r.executions[0]
        self.assertEqual((e.num_gluten_nodes, e.num_fallback_nodes), (1, 1))
        self.assertEqual(e.fallback_node_to_reason, {"002 Agg": "validation failed"})
