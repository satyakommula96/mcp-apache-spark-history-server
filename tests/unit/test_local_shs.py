import unittest
from unittest.mock import MagicMock, patch

import spark_history_mcp.core.app  # noqa: F401  (register tools first)
from spark_history_mcp.tools import local_shs as shs


def _ctx():
    ctx = MagicMock()
    ctx.request_context.lifespan_context.clients = {}
    return ctx


class TestLocalShs(unittest.TestCase):
    def test_default_name_is_readable_and_stable(self):
        p = "gs://b/x/05-11-2026/ab03e42684f061d1/dataproc_cluster/spark-logs"
        n = shs._default_name(p)
        self.assertTrue(n.startswith("shs-ab03e42684f061d1-"))
        self.assertEqual(n, shs._default_name(p))
        self.assertRegex(n, shs._NAME_RE)

    def test_rejects_conf_injection_and_bad_input(self):
        for bad in ("gs://b/p\nspark.foo bar", "gs://b/p x", "/local/path", "http://x"):
            with self.assertRaises(ValueError):
                shs.start_history_server(bad, ctx=_ctx())
        with patch.object(shs, "_load_registry", return_value={}):
            with self.assertRaises(ValueError):
                shs.start_history_server("gs://b/p", name="Bad Name", ctx=_ctx())

    def test_reuses_running_server_for_same_log_dir(self):
        reg = {"shs-a": {"log_dir": "gs://b/p", "url": "http://localhost:18090", "port": 18090}}
        ctx = _ctx()
        with patch.object(shs, "_load_registry", return_value=reg), patch.object(shs, "_alive", return_value=True):
            r = shs.start_history_server("gs://b/p/", ctx=ctx)
        self.assertEqual((r["status"], r["name"]), ("reused", "shs-a"))
        self.assertIn("shs-a", ctx.request_context.lifespan_context.clients)

    def test_stop_only_registry_servers(self):
        with patch.object(shs, "_load_registry", return_value={}):
            with self.assertRaises(ValueError):
                shs.stop_history_server("deb11", ctx=_ctx())
