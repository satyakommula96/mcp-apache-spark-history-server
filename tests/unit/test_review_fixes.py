"""Regression tests for review findings (spill parsing, native detection, scan names, subqueries)."""
import gzip
import io
import json
import zipfile

import lz4.block

from spark_history_mcp.models.mcp_types import QueryFallbackSummary
from spark_history_mcp.tools import native_engine as ne
from spark_history_mcp.tools import scan_nodes as sn
from spark_history_mcp.tools.regression import _ENV_NOISE_KEY_RE
from spark_history_mcp.tools.tools import _NATIVE_NODE_PATTERN, _is_nonzero_metric_value


def test_multi_task_zero_metric_is_zero():
    v = "total (min, med, max (stageId: taskId))\n0.0 B (0.0 B, 0.0 B, 0.0 B (stage 6.0: task 3771))"
    assert not _is_nonzero_metric_value(v)
    v2 = "total (min, med, max (stageId: taskId))\n12.0 MiB (0.0 B, 1.0 MiB, 4.0 MiB (stage 6.0: task 3771))"
    assert _is_nonzero_metric_value(v2)
    assert not _is_nonzero_metric_value("0")
    assert _is_nonzero_metric_value("3")


def test_native_pattern():
    assert not _NATIVE_NODE_PATTERN.search("ColumnarToRow")
    assert not _NATIVE_NODE_PATTERN.search("RowToColumnar")
    for n in ("VeloxColumnarToRow", "RowToVeloxColumnar", "ProjectExecTransformer", "CHColumnarToRow"):
        assert _NATIVE_NODE_PATTERN.search(n), n


def test_scan_name_regex():
    for name in (
        "Scan parquet db.t",
        "ScanTransformer parquet db.t",
        "FileSourceScanExecTransformer parquet spark_catalog.tpcds.store_sales",
        "Scan parquet db.t [codegen id : 3]",
    ):
        m = sn._FILE_SCAN_NAME_RE.match(name)
        assert m and m.group(1) == "parquet", name
    assert sn._FILE_SCAN_NAME_RE.match("Scan parquet db.t [codegen id : 3]").group(2) == "db.t"


def test_noise_keys():
    for k in ("spark.yarn.tags", "spark.eventLog.dir", "spark.metrics.namespace",
              "spark.dataproc.metrics.listener.metrics.collector.hostname"):
        assert _ENV_NOISE_KEY_RE.search(k)
    assert not _ENV_NOISE_KEY_RE.search("spark.executor.memory")


# ---- native_engine.py review fixes


def _lz4block(payload: bytes) -> bytes:
    comp = lz4.block.compress(payload, store_size=False)

    def hdr(tok, c, d):
        return b"LZ4Block" + bytes([tok]) + c.to_bytes(4, "little") + d.to_bytes(4, "little") + b"\0\0\0\0"

    return hdr(0x20, len(comp), len(payload)) + comp + hdr(0x10, 0, 0)


def _zip(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for n, b in members.items():
            z.writestr(n, b)
    return buf.getvalue()


def test_lz4block_event_log():
    body = b'{"Event":"A"}\n{"Event":"B"}\n'
    recs = list(ne.iter_event_log_records(_zip({"app_1.lz4": _lz4block(body)})))
    assert [r["Event"] for r in recs] == ["A", "B"]


def test_truncated_gzip_keeps_prefix():
    body = b"".join(b'{"Event":"E%d"}\n' % i for i in range(2000))
    data = gzip.compress(body)
    recs = list(ne.iter_event_log_records(_zip({"app_1": data[: len(data) // 2]})))
    assert recs and recs[0]["Event"] == "E0"


def test_latest_attempt_only():
    names = ["eventlog_v2_app_1/events_1_app_1", "eventlog_v2_app_2/events_1_app_2",
             "eventlog_v2_app_1/events_2_app_1", "eventlog_v2_app_2/appstatus_app_2"]
    assert ne._event_log_member_names(names) == ["eventlog_v2_app_2/events_1_app_2"]
    assert ne._event_log_member_names(["app_1", "app_2.lz4"]) == ["app_2.lz4"]


def test_infinite_cost_serializes():
    s = QueryFallbackSummary(execution_id=1, gluten_plan_cost=float("inf"), vanilla_plan_cost=2.0)
    assert json.loads(s.model_dump_json())["gluten_plan_cost"] == "Infinity"


def test_cost_regexes():
    m = ne._TOTAL_COST_RE.search("Total Cost: vanilla=2.9173110191993106E7, dpa-s=Infinity")
    assert m and ne._to_float(m.group(1)) == 2.9173110191993106e7
