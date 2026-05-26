"""
KAGE Decorator Tests - declarative @ API
KAGE-PROPRIETARY-2026-v1.1
"""
import json
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from kage import KageLogger, configure, dataset, pipeline, set_default_logger, task


def _read_events(base_path: str, event_type: str):
    files = list(Path(base_path).glob(f"**/event_type={event_type}/**/part-*.jsonl"))
    events = []
    for f in files:
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def _wait_for(base_path, event_type, timeout=2.0):
    start = time.time()
    pattern = f"**/event_type={event_type}/**/part-*.jsonl"
    while time.time() - start < timeout:
        if list(Path(base_path).glob(pattern)):
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def tmp_logger():
    base = tempfile.mkdtemp(prefix="kage-decor-")
    lg = KageLogger(base_path=base, pipeline_name="decor_pipeline")
    set_default_logger(lg)
    yield lg, base
    shutil.rmtree(base, ignore_errors=True)


def test_pipeline_decorator_success(tmp_logger):
    lg, base = tmp_logger

    @pipeline("daily_etl")
    def run():
        return 42

    assert run() == 42
    assert _wait_for(base, "job_run")

    events = _read_events(base, "job_run")
    statuses = [e["status"] for e in events]
    assert "RUNNING" in statuses
    assert "SUCCESS" in statuses


def test_pipeline_decorator_captures_error(tmp_logger):
    lg, base = tmp_logger

    @pipeline("breaking_etl")
    def run():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        run()

    events = _read_events(base, "job_run")
    failed = [e for e in events if e["status"] == "FAILED"]
    assert failed, "expected a FAILED job_run event"
    cf = failed[0]["custom_fields"]
    assert cf["error_type"] == "ValueError"
    assert "boom" in cf["error_message"]
    assert "stack_trace" in cf


def test_pipeline_no_parens(tmp_logger):
    lg, base = tmp_logger

    @pipeline
    def named_job():
        return "ok"

    assert named_job() == "ok"
    events = _read_events(base, "job_run")
    job_names = [e.get("job_name") for e in events if e.get("job_name")]
    assert "named_job" in job_names


def test_task_decorator(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("parent_job")

    @task(layer="bronze", task_name="clean")
    def clean(x):
        return x * 2

    assert clean(5) == 10
    events = _read_events(base, "task_run")
    assert any(e.get("layer") == "bronze" and e.get("status") == "RUNNING" for e in events)
    assert any(e.get("status") == "SUCCESS" for e in events)


def test_task_failure(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("parent_job")

    @task(layer="silver")
    def failing_task():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        failing_task()

    events = _read_events(base, "task_run")
    failed = [e for e in events if e.get("status") == "FAILED"]
    assert failed
    assert failed[0]["custom_fields"]["error_type"] == "RuntimeError"


def test_dataset_decorator_write_with_int_return(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("ds_job")

    @dataset(layer="gold", dataset_name="summary")
    def build_summary():
        return 1234

    build_summary()
    events = _read_events(base, "dataset_event")
    writes = [e for e in events if e["event_action"] == "WRITE"
              and e["dataset_name"] == "summary"]
    assert writes
    assert writes[0]["record_count"] == 1234
    assert writes[0]["layer"] == "gold"


def test_dataset_decorator_uses_len(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("ds_job")

    @dataset(layer="bronze", dataset_name="rows")
    def make_rows():
        return [1, 2, 3, 4]

    make_rows()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "rows"]
    assert target and target[0]["record_count"] == 4


def test_dataset_read_action(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("ds_job")

    @dataset(layer="landing", dataset_name="src", action="READ")
    def read_src():
        return 99

    read_src()
    events = _read_events(base, "dataset_event")
    reads = [e for e in events if e["event_action"] == "READ"
             and e["dataset_name"] == "src"]
    assert reads and reads[0]["record_count"] == 99


def test_dataset_custom_record_count_fn(tmp_logger):
    lg, base = tmp_logger
    lg.job_start("ds_job")

    @dataset(layer="gold", dataset_name="custom",
             record_count_fn=lambda r: r["n"])
    def build():
        return {"n": 777, "other": "ignored"}

    build()
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "custom"]
    assert target and target[0]["record_count"] == 777


def test_configure_returns_logger(tmp_logger):
    base = tempfile.mkdtemp(prefix="kage-conf-")
    try:
        lg = configure(base_path=base, pipeline_name="from_configure")
        assert isinstance(lg, KageLogger)
        assert lg.context["pipeline_name"] == "from_configure"
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_full_decorator_pipeline(tmp_logger):
    """End-to-end: pipeline + task + dataset, including lineage."""
    lg, base = tmp_logger

    @dataset(layer="bronze", dataset_name="bronze_orders")
    def clean(_raw):
        return 128394

    @task(layer="bronze", task_name="bronze_step")
    def bronze_step(raw):
        return clean(raw)

    @pipeline("full_demo")
    def run():
        return bronze_step(150000)

    run()

    job_events = _read_events(base, "job_run")
    task_events = _read_events(base, "task_run")
    ds_events = _read_events(base, "dataset_event")

    assert any(e["status"] == "SUCCESS" for e in job_events)
    assert any(e["status"] == "SUCCESS" for e in task_events)
    assert any(e["dataset_name"] == "bronze_orders"
               and e["event_action"] == "WRITE"
               and e["record_count"] == 128394
               for e in ds_events)
