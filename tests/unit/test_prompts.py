import unittest

from spark_history_mcp.core.app import mcp
from spark_history_mcp.prompts.prompts import (
    compare_applications,
    investigate_failure,
    investigate_slowness,
)


class TestInvestigateFailurePrompt(unittest.TestCase):
    def test_renders_app_id_and_investigation_tools(self):
        """The rendered prompt embeds the app id and names the real tools."""
        result = investigate_failure(app_id="app-123")

        self.assertIn("app-123", result)
        # References the actual tools an agent should call, in order.
        for tool_name in (
            "list_applications",
            "list_jobs",
            "list_stages",
            "get_stage",
            "list_stage_task_failures",
        ):
            self.assertIn(tool_name, result)

    def test_omits_server_arg_when_not_provided(self):
        """Without a server, no concrete server name is injected and the
        automatic cross-server discovery behavior is explained."""
        result = investigate_failure(app_id="app-123")

        # No concrete server name should be threaded into the tool calls.
        self.assertNotIn('server="', result.replace('server="<name>"', ""))
        # The discovery behavior should be spelled out for the agent.
        self.assertIn("search every configured", result)

    def test_includes_server_arg_when_provided(self):
        """A provided server name is threaded into the rendered tool calls."""
        result = investigate_failure(app_id="app-123", server="production")

        self.assertIn('server="production"', result)

    def test_status_filter_uses_failed(self):
        """The job/stage steps filter on the FAILED status."""
        result = investigate_failure(app_id="app-123")

        self.assertIn('status=["FAILED"]', result)

    def test_prompt_is_registered_with_mcp(self):
        """The prompt is discoverable via the shared MCPServer instance."""
        # Importing the module registers the prompt; the manager lists it.
        registered = {p.name for p in mcp._prompt_manager.list_prompts()}

        self.assertIn("investigate_failure", registered)


class TestCompareApplicationsPrompt(unittest.TestCase):
    def test_renders_both_app_ids_and_comparison_tools(self):
        """The rendered prompt embeds both app ids and names the compare tools."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertIn("app-a", result)
        self.assertIn("app-b", result)
        for tool_name in (
            "compare_job_environments",
            "compare_job_performance",
            "list_sql_executions",
            "compare_sql_executions",
            "get_stage",
        ):
            self.assertIn(tool_name, result)

    def test_is_analysis_only(self):
        """The prompt explicitly avoids root-cause diagnosis and fixes."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertIn("do not diagnose", result)
        self.assertIn("do not recommend", result)

    def test_omits_server_arg_when_not_provided(self):
        """Without a server, no concrete name is injected and the cross-server
        (cross-environment) discovery behavior is explained."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertNotIn('server="', result.replace('server="<name>"', ""))
        self.assertIn("may live on different", result)

    def test_includes_server_arg_when_provided(self):
        """A provided server name is threaded into the rendered tool calls."""
        result = compare_applications(app_a="app-a", app_b="app-b", server="prod")

        self.assertIn('server="prod"', result)

    def test_includes_context_when_provided(self):
        """Free-text context is embedded when supplied."""
        result = compare_applications(
            app_a="app-a",
            app_b="app-b",
            context="A is the baseline, B doubled shuffle partitions",
        )

        self.assertIn("A is the baseline, B doubled shuffle partitions", result)

    def test_omits_context_section_when_absent(self):
        """No context line leaks in when none is provided."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertNotIn("Context for this comparison", result)

    def test_prompt_is_registered_with_mcp(self):
        """The prompt is discoverable via the shared MCPServer instance."""
        registered = {p.name for p in mcp._prompt_manager.list_prompts()}

        self.assertIn("compare_applications", registered)


class TestInvestigateSlownessPrompt(unittest.TestCase):
    def test_renders_both_app_ids_and_diagnostic_tools(self):
        """The rendered prompt embeds both app ids and names the diagnostic tools."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertIn("app-dpas", result)
        self.assertIn("app-base", result)
        for tool_name in (
            "list_applications",
            "get_environment",
            "compare_job_performance",
            "list_sql_executions",
            "compare_sql_executions",
            "get_stage",
        ):
            self.assertIn(tool_name, result)

    def test_checks_gluten_acceleration_markers(self):
        """The prompt names the concrete config/plan markers that confirm
        whether native (Gluten-based) acceleration actually engaged."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertIn("spark.gluten.dp.usedpas", result)
        self.assertIn("org.apache.gluten.GlutenPlugin", result)
        self.assertIn("ColumnarShuffleManager", result)
        self.assertIn("spark.gluten.*", result)
        self.assertIn("Transformer", result)
        self.assertIn("ColumnarToRow", result)

    def test_explains_query_vs_stage_fallback_granularity(self):
        """The prompt distinguishes the two cost-based fallback granularities
        so the agent knows what plan/stage pattern to expect from each."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertIn("spark.gluten.dp.cost.queryFallbackEnabled", result)
        self.assertIn("spark.gluten.dp.cost.stageFallbackEnabled", result)
        self.assertIn("fully native or fully vanilla", result)

    def test_lists_acceleration_decision_values(self):
        """The wrapper-recorded usedpas.decision values explain why DPAS was
        off; the prompt should name them so the model can interpret them."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        for decision in (
            "nodpas-config-low",
            "nodpas-localjob-failed",
            "nodpas-autoBroadcastJoinThreshold-disabled",
            "nodpas-legacy-timeParserPolicy",
        ):
            self.assertIn(decision, result)

    def test_mentions_history_based_fallback_and_default_off(self):
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertIn("spark.gluten.dp.history.queryfailure.queryFallbackEnabled", result)
        self.assertIn("default to off", result)

    def test_does_not_leak_internal_implementation_names(self):
        """This is a public repo: the prompt must describe the cost-based
        fallback mechanism generically and never name DataPelago's internal,
        closed-source implementation classes."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        for internal_name in (
            "QueryCostDecisionPolicy",
            "StageCostFallbackPolicy",
            "DPCostModel",
            "QueryHashFallbackRegistry",
            "ConservativeCostOverrideConfig",
            "minFactor",
        ):
            self.assertNotIn(internal_name, result)

    def test_is_diagnostic_not_descriptive_only(self):
        """Unlike compare_applications, this prompt asks for a root cause and
        a concrete fix."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertIn("root cause", result)
        self.assertIn("recommend", result)

    def test_omits_server_arg_when_not_provided(self):
        """Without a server, no concrete name is injected and cross-server
        discovery is explained."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base"
        )

        self.assertNotIn('server="', result.replace('server="<name>"', ""))
        self.assertIn("may live on different", result)

    def test_includes_server_arg_when_provided(self):
        """A provided server name is threaded into the rendered tool calls."""
        result = investigate_slowness(
            dpas_app_id="app-dpas", baseline_app_id="app-base", server="production"
        )

        self.assertIn('server="production"', result)

    def test_includes_context_when_provided(self):
        """Free-text context is embedded when supplied."""
        result = investigate_slowness(
            dpas_app_id="app-dpas",
            baseline_app_id="app-base",
            context="same TPC-DS query, DPAS run is 40% slower",
        )

        self.assertIn("same TPC-DS query, DPAS run is 40% slower", result)

    def test_prompt_is_registered_with_mcp(self):
        """The prompt is discoverable via the shared FastMCP instance."""
        registered = {p.name for p in mcp._prompt_manager.list_prompts()}

        self.assertIn("investigate_slowness", registered)


if __name__ == "__main__":
    unittest.main()
