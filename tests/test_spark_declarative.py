"""
KAGE x Spark Declarative Pipelines tests.

Uses a stubbed `dlt` module so the integration runs outside Databricks.
"""
import importlib
import json
import shutil
import sys
import tempfile
import time
import types
from pathlib import Path

import pytest


# ----- stub `dlt` module installed before kage.integrations imports it -----

class _DltStub(types.ModuleType):
    """Records decorator invocations; passes the wrapped function through."""

    def __init__(self):
        super().__init__("dlt")
        self.calls = []

    def _record(self, kind, *args, **kwargs):
        def decorator(func):
            self.calls.append((kind, args, kwargs, func.__name__))
            return func
        return decorator

    def table(self, *args, **kwargs):
        return self._record("table", *args, **kwargs)

    def view(self, *args, **kwargs):
        return self._record("view", *args, **kwargs)

    def expect(self, *args, **kwargs):
        return self._record("expect", *args, **kwargs)

    def expect_or_drop(self, *args, **kwargs):
        return self._record("expect_or_drop", *args, **kwargs)

    def expect_or_fail(self, *args, **kwargs):
        return self._record("expect_or_fail", *args, **kwargs)

    def expect_all(self, *args, **kwargs):
        return self._record("expect_all", *args, **kwargs)

    def expect_all_or_drop(self, *args, **kwargs):
        return self._record("expect_all_or_drop", *args, **kwargs)

    def expect_all_or_fail(self, *args, **kwargs):
        return self._record("expect_all_or_fail", *args, **kwargs)


@pytest.fixture(scope="module")
def dlt_stub():
    stub = _DltStub()
    sys.modules["dlt"] = stub
    # Force re-import so the module picks up the stub
    import kage.integrations.spark_declarative as sd
    importlib.reload(sd)
    yield stub, sd
    sys.modules.pop("dlt", None)
    importlib.reload(sd)


@pytest.fixture
def tmp_logger(dlt_stub):
    from kage import KageLogger, set_default_logger
    base = tempfile.mkdtemp(prefix="kage-dlt-")
    lg = KageLogger(base_path=base, pipeline_name="dlt_pipeline")
    set_default_logger(lg)
    yield lg, base
    shutil.rmtree(base, ignore_errors=True)


def _read_events(base, event_type):
    files = list(Path(base).glob(f"**/event_type={event_type}/**/part-*.jsonl"))
    out = []
    for f in files:
        for line in f.read_text().splitlines():
            if line.strip():
                out.append(json.loads(line))
    return out


def _wait_for(base, event_type, timeout=2.0):
    start = time.time()
    pattern = f"**/event_type={event_type}/**/part-*.jsonl"
    while time.time() - start < timeout:
        if list(Path(base).glob(pattern)):
            return True
        time.sleep(0.05)
    return False


# --------------------------------------------------------------------------- #


def test_kage_dlt_table_positive(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(
        layer="bronze",
        upstream_datasets=["landing.orders"],
        comment="bronze orders",
        skip_count=False,
    )
    def bronze_orders():
        return [1, 2, 3, 4, 5]

    result = bronze_orders()
    assert result == [1, 2, 3, 4, 5]

    assert _wait_for(base, "dataset_event")
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "bronze_orders"]
    assert target
    e = target[0]
    assert e["event_action"] == "WRITE"
    assert e["layer"] == "bronze"
    assert e["record_count"] == 5
    assert e["upstream_datasets"] == ["landing.orders"]
    assert e["custom_fields"]["status"] == "SUCCESS"


def test_kage_dlt_table_skip_count_default(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(layer="silver", upstream_datasets=["bronze_orders"])
    def silver_orders():
        return [1] * 1_000_000  # would be expensive to count

    silver_orders()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "silver_orders"]
    assert target
    assert target[0]["record_count"] == 0  # skipped by default


def test_kage_dlt_table_negative_exception(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(layer="bronze", upstream_datasets=["landing.missing"])
    def broken_bronze():
        raise FileNotFoundError("table not found")

    with pytest.raises(FileNotFoundError, match="table not found"):
        broken_bronze()

    events = _read_events(base, "dataset_event")
    failed = [e for e in events if e["dataset_name"] == "broken_bronze"]
    assert failed
    cf = failed[0]["custom_fields"]
    assert cf["status"] == "FAILED"
    assert cf["error_type"] == "FileNotFoundError"
    assert "table not found" in cf["error_message"]
    assert "stack_trace" in cf


def test_kage_dlt_table_streaming_dataframe(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    class StreamingDF:
        isStreaming = True
        def count(self):
            raise RuntimeError("count() on streaming DF would block")

    @sd.kage_dlt_table(
        layer="bronze",
        upstream_datasets=["landing.events"],
        skip_count=False,
    )
    def stream_bronze():
        return StreamingDF()

    stream_bronze()  # must not raise
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "stream_bronze"]
    assert target and target[0]["record_count"] == 0


def test_kage_dlt_table_empty_dataframe(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(layer="bronze", upstream_datasets=["x"], skip_count=False)
    def empty_bronze():
        return []

    empty_bronze()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "empty_bronze"]
    assert target and target[0]["record_count"] == 0


def test_kage_dlt_table_custom_record_count_fn(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(
        layer="gold",
        upstream_datasets=["silver"],
        record_count_fn=lambda r: r["row_count"],
    )
    def gold_summary():
        return {"row_count": 4242, "extra": "ignored"}

    gold_summary()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "gold_summary"]
    assert target and target[0]["record_count"] == 4242


def test_kage_dlt_view_action_read(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_view(layer="gold", upstream_datasets=["gold_table"])
    def gold_recent_view():
        return [1, 2, 3]

    gold_recent_view()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "gold_recent_view"]
    assert target and target[0]["event_action"] == "READ"


def test_kage_dlt_expectations_records_constraints(dlt_stub):
    stub, sd = dlt_stub
    stub.calls.clear()

    @sd.kage_dlt_table(layer="silver", upstream_datasets=["bronze"])
    @sd.kage_dlt_expectations(
        ("expect_or_drop", "valid_amount", "amount > 0"),
        ("expect_or_fail", "non_null_id", "order_id IS NOT NULL"),
    )
    def silver_orders():
        return []

    kinds = [c[0] for c in stub.calls]
    assert "expect_or_drop" in kinds
    assert "expect_or_fail" in kinds
    assert "table" in kinds

    expect_calls = [c for c in stub.calls if c[0] == "expect_or_drop"]
    assert expect_calls[0][1] == ("valid_amount", "amount > 0")


def test_kage_dlt_expectations_invalid_action(dlt_stub):
    _, sd = dlt_stub
    with pytest.raises(ValueError, match="Invalid expectation action"):
        sd.kage_dlt_expectations(("expect_or_nope", "n", "c"))


def test_is_dlt_available(dlt_stub):
    _, sd = dlt_stub
    assert sd.is_dlt_available() is True


def test_kage_dlt_table_extra_fields(tmp_logger, dlt_stub):
    _, sd = dlt_stub
    _, base = tmp_logger

    @sd.kage_dlt_table(
        layer="silver",
        upstream_datasets=["bronze"],
        extra_fields={"owner": "data-platform", "sla_minutes": 30},
    )
    def silver_with_meta():
        return [1, 2]

    silver_with_meta()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "silver_with_meta"]
    assert target
    cf = target[0]["custom_fields"]
    assert cf["owner"] == "data-platform"
    assert cf["sla_minutes"] == 30
