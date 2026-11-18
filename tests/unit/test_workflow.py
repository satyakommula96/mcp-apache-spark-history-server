import unittest
from types import SimpleNamespace as NS
from unittest.mock import MagicMock

from spark_history_mcp.tools.workflow import compare_benchmark_runs, find_applications


def _ctx(clients, default=None):
    ctx = MagicMock()
    lc = ctx.request_context.lifespan_context
    lc.clients = clients
    lc.default_client = default or next(iter(clients.values()))
    lc.app_discovery = None
    return ctx


def _app(app_id, name, dur=1000):
    att = NS(start_time="2026-11-08", duration=dur, completed=True, app_spark_version="3.5.2")
    return NS(id=app_id, name=name, attempts=[att])


def _exec(i, desc, ms, status="COMPLETED"):
    return NS(id=i, description=desc, duration=ms, status=status)


class TestFindApplications(unittest.TestCase):
    def test_searches_all_servers_and_filters_by_name(self):
        a, b = MagicMock(), MagicMock()
        a.list_applications.return_value = [_app("app-1", "TPCDS q1"), _app("app-2", "other")]
        b.list_applications.return_value = [_app("app-3", "tpcds q2")]
        rows = find_applications(name_contains="TPCDS", ctx=_ctx({"deb11": a, "deb12": b}))
        self.assertEqual([(r["server"], r["id"]) for r in rows], [("deb11", "app-1"), ("deb12", "app-3")])
        self.assertEqual(rows[0]["duration_s"], 1.0)

    def test_unreachable_server_reported_not_raised(self):
        a, b = MagicMock(), MagicMock()
        a.list_applications.side_effect = ConnectionError("down")
        b.list_applications.return_value = [_app("app-3", "x")]
        rows = find_applications(ctx=_ctx({"deb11": a, "deb12": b}))
        self.assertIn("error", rows[0])
        self.assertEqual(rows[1]["id"], "app-3")


class TestCompareBenchmarkRuns(unittest.TestCase):
    def _clients(self, e1, e2):
        c1, c2 = MagicMock(), MagicMock()
        c1.get_sql_list.side_effect = lambda **kw: e1 if kw.get("offset", 0) == 0 else []
        c2.get_sql_list.side_effect = lambda **kw: e2 if kw.get("offset", 0) == 0 else []
        c1.get_gluten_ui_summary.return_value = None
        c2.get_gluten_ui_summary.return_value = {
            "queries": {"executions": [{"executionId": 1, "numGlutenNodes": 5, "numFallbackNodes": 0}]}
        }
        return c1, c2

    def test_pairs_by_description_and_flags_movers(self):
        e1 = [_exec(0, "q1", 1000), _exec(1, "q2", 1000), _exec(2, "q3", 1000), _exec(3, "only-a", 900)]
        e2 = [_exec(0, "q1 ", 1050), _exec(1, "q2", 3000), _exec(2, "q3", 400)]
        c1, c2 = self._clients(e1, e2)
        ctx = _ctx({"a": c1, "b": c2})
        r = compare_benchmark_runs("app-a", "app-b", server1="a", server2="b", ctx=ctx)
        self.assertEqual(r["executions"]["paired"], 3)
        self.assertEqual(r["executions"]["only_in_baseline"], 1)
        self.assertEqual((r["regressions"], r["improvements"]), (1, 1))
        self.assertEqual(r["top_regressions"][0]["description"], "q2")
        self.assertEqual(r["top_regressions"][0]["native"][1], {"gluten_nodes": 5, "fallback_nodes": 0})
        self.assertEqual(r["total_ms"], [3000, 4450])

    def test_repeated_descriptions_pair_in_order_and_fast_queries_skipped(self):
        e1 = [_exec(0, "insert", 1000), _exec(1, "insert", 2000), _exec(2, "tiny", 10)]
        e2 = [_exec(0, "insert", 1000), _exec(1, "insert", 2000), _exec(2, "tiny", 90)]
        c1, c2 = self._clients(e1, e2)
        r = compare_benchmark_runs("a", "b", server1="a", server2="b", ctx=_ctx({"a": c1, "b": c2}))
        self.assertEqual(r["executions"]["skipped_under_min_duration"], 1)
        self.assertEqual(r["regressions"], 0)
        self.assertEqual(r["geomean_ratio"], 1.0)


class TestNativeDiff(unittest.TestCase):
    def test_reports_new_fallbacks_and_reason_kinds(self):
        def gl(fb, reasons):
            return {"queries": {"executions": [
                {"executionId": 1, "numGlutenNodes": 9, "numFallbackNodes": fb,
                 "fallbackNodeToReason": reasons}]}}
        c1, c2 = MagicMock(), MagicMock()
        for c in (c1, c2):
            c.list_sql_executions.side_effect = None
        c1.get_gluten_ui_summary.return_value = gl(0, {})
        c2.get_gluten_ui_summary.return_value = gl(
            2, {"1: Sort": "Query DP cost fallback: spark=2.9E7, gluten=Infinity"})
        import spark_history_mcp.tools.workflow as wf
        orig = wf._fetch_all_sql_executions
        wf._fetch_all_sql_executions = lambda c, a: [_exec(1, "q1", 1000 if c is c1 else 2000)]
        try:
            r = compare_benchmark_runs("a1", "a2", "s1", "s2", ctx=_ctx({"s1": c1, "s2": c2}))
        finally:
            wf._fetch_all_sql_executions = orig
        nd = r["native_diff"]
        self.assertEqual(nd["queries_more_fallback"], 1)
        self.assertEqual(nd["top_more_fallback"][0]["new_reasons"],
                         ["Query DP cost fallback: spark=#, gluten=#"])
        self.assertIn("Query DP cost fallback: spark=#, gluten=#", nd["reason_kinds_only_in_candidate"])
        self.assertNotIn("_reasons", r["top_regressions"][0]["native"][1])

    def test_name_filter_scans_more_than_limit(self):
        a = MagicMock()
        a.list_applications.return_value = [_app(f"app-{i}", "x") for i in range(30)] + [_app("old", "TPCDS")]
        rows = find_applications(name_contains="tpcds", limit_per_server=5, ctx=_ctx({"s": a}))
        self.assertIsNone(a.list_applications.call_args.kwargs["limit"])
        self.assertEqual([r["id"] for r in rows], ["old"])


class TestNewWorkflowTools(unittest.TestCase):
    def _client(self):
        c = MagicMock()
        c.get_gluten_ui_summary.return_value = {"queries": {"executions": [
            {"executionId": 1, "numGlutenNodes": 0, "numFallbackNodes": 0},
            {"executionId": 2, "numGlutenNodes": 0, "numFallbackNodes": 4},
            {"executionId": 3, "numGlutenNodes": 5, "numFallbackNodes": 1},
            {"executionId": 4, "numGlutenNodes": 7, "numFallbackNodes": 0}]}}
        return c

    def test_query_suite_report(self):
        import spark_history_mcp.tools.workflow as wf
        c = self._client()
        execs = [_exec(1, "SET x", 1), _exec(2, "q2", 900, "FAILED"), _exec(3, "q3 | x", 50), _exec(4, "q4", 70)]
        orig = wf._fetch_all_sql_executions
        wf._fetch_all_sql_executions = lambda cl, a: execs
        try:
            r = wf.query_suite_report("a", server="s", ctx=_ctx({"s": c}))
            self.assertEqual(r["totals"]["by_engine"], {"no_plan": 1, "vanilla": 1, "partial": 1, "native": 1})
            self.assertEqual([row[3] for row in r["rows"]], ["no_plan", "vanilla", "partial", "native"])
            r = wf.query_suite_report("a", server="s", only="not_native", ctx=_ctx({"s": c}))
            self.assertEqual([row[0] for row in r["rows"]], [2, 3])
            r = wf.query_suite_report("a", server="s", only="failed", markdown=True, ctx=_ctx({"s": c}))
            self.assertIn("| 2 | FAILED | 900 | vanilla | q2 |", r["table"])
            r = wf.query_suite_report("a", server="s", markdown=True, ctx=_ctx({"s": c}))
            self.assertIn("q3 \\| x", r["table"])
        finally:
            wf._fetch_all_sql_executions = orig

    def test_config_changes_across_runs(self):
        from spark_history_mcp.tools.workflow import config_changes_across_runs
        envs = {
            "a1": [("spark.gluten.x", "1"), ("spark.executor.memory", "8g"), ("spark.yarn.tags", "t1"),
                   ("spark.executor.extraJavaOptions", "-Da " * 40)],
            "a2": [("spark.gluten.x", "2"), ("spark.executor.memory", "8g"), ("spark.yarn.tags", "t2"),
                   ("spark.executor.extraJavaOptions", "-Da " * 40 + "-Dnew")],
        }
        c = MagicMock()
        c.get_environment.side_effect = lambda app_id: NS(spark_properties=envs[app_id])
        r = config_changes_across_runs(["a1", "a2"], servers=["s", "s"], ctx=_ctx({"s": c}))
        self.assertEqual(r["changed"]["spark.gluten.x"], ["1", "2"])
        self.assertNotIn("spark.executor.memory", r["changed"])
        self.assertNotIn("spark.yarn.tags", r["changed"])
        self.assertEqual(r["changed"]["spark.executor.extraJavaOptions"]["diff_vs_run_0"][0]["added"], ["-Dnew"])

    def test_check_executor_memory_gc_flags_native_sizing(self):
        from spark_history_mcp.tools.workflow import check_executor_memory_gc
        c = self._client()
        c.get_environment.return_value = NS(spark_properties=[
            ("spark.executor.memory", "8192m"),
            ("spark.gluten.memory.offHeap.size.in.bytes", str(5 * 2**30)),
            ("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "true")])
        pm = NS(jvm_heap_memory=6 * 2**30, off_heap_execution_memory=2**30)
        c.list_all_executors.return_value = [
            NS(id="driver", total_duration=1, total_gc_time=0, peak_memory_metrics=None),
            NS(id="1", total_duration=10000, total_gc_time=3000, peak_memory_metrics=pm)]
        c.list_stages.return_value = [NS(stage_id=1, status="COMPLETE", executor_run_time=5000,
                                         jvm_gc_time=2500, name="s1")]
        r = check_executor_memory_gc("a", server="s", ctx=_ctx({"s": c}))
        self.assertEqual(r["gc"]["gc_ratio"], 0.3)
        self.assertEqual(r["memory_config"]["native_memory_mb"], 5120.0)
        self.assertIn("Likely cause", r["verdict"])
        self.assertEqual(r["top_gc_stages"][0]["gc_ratio"], 0.5)


class TestFindApplicationsDedup(unittest.TestCase):
    def test_same_url_searched_once_under_named_server(self):
        a, b = MagicMock(), MagicMock()
        a.config.url = "http://h:18080"
        b.config.url = "http://h:18080/"
        for c in (a, b):
            c.list_applications.return_value = [_app("app-1", "x")]
        rows = find_applications(ctx=_ctx({"local": a, "deb11": b}))
        self.assertEqual([(r["server"], r["id"]) for r in rows], [("deb11", "app-1")])


class TestRetentionWarning(unittest.TestCase):
    def test_warns_when_low_ids_evicted(self):
        from spark_history_mcp.tools.workflow import _retention_warning
        self.assertIsNone(_retention_warning("a", [_exec(0, "x", 1), _exec(1, "y", 1)]))
        w = _retention_warning("a", [_exec(1142, "x", 1), _exec(1143, "y", 1)])
        self.assertIn("0..1141 were evicted", w)
