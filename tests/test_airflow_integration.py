"""
KAGE x Airflow integration tests.

Airflow's callback API passes a `context` dict. We build a minimal
synthetic context (TaskInstance + DagRun + Dag stand-ins) so the tests
run without installing Airflow.
"""
import json
import shutil
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from kage import KageLogger, set_default_logger
from kage.integrations.airflow import (
    kage_dag_callbacks,
    kage_task_callbacks,
    log_dataset_from_context,
)


def _read_events(base: Path, event_type: str):
    out = []
    for f in base.glob(f"**/event_type={event_type}/**/part-*.jsonl"):
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _wait_for(base: Path, event_type: str, timeout=2.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if list(base.glob(f"**/event_type={event_type}/**/part-*.jsonl")):
            return True
        time.sleep(0.05)
    return False


def _make_context(*, dag_id="orders_etl", run_id="run_2026_05_26",
                  task_id="extract", try_number=1, duration=0.5,
                  exception=None):
    ti = SimpleNamespace(
        task_id=task_id, dag_id=dag_id,
        try_number=try_number, duration=duration,
    )
    dag = SimpleNamespace(dag_id=dag_id)
    dag_run = SimpleNamespace(run_id=run_id)
    return {
        "task_instance": ti, "ti": ti,
        "dag": dag, "dag_run": dag_run,
        "run_id": run_id,
        "exception": exception,
    }


@pytest.fixture
def tmp_logger():
    base = tempfile.mkdtemp(prefix="kage-airflow-")
    lg = KageLogger(base_path=base, pipeline_name="airflow_pipeline",
                    platform="airflow")
    set_default_logger(lg)
    yield lg, Path(base)
    shutil.rmtree(base, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Positive cases

def test_task_callbacks_emit_start_and_success(tmp_logger):
    lg, base = tmp_logger
    cbs = kage_task_callbacks(layer="bronze", logger=lg)
    ctx = _make_context(task_id="extract")

    cbs["on_execute_callback"](ctx)
    cbs["on_success_callback"](ctx)

    assert _wait_for(base, "task_run")
    events = _read_events(base, "task_run")
    states = [e.get("status") for e in events]
    assert "RUNNING" in states
    assert "SUCCESS" in states
    # All task_run events should reference airflow run_id as job_run_id
    job_ids = {e["job_run_id"] for e in events if "job_run_id" in e}
    assert ctx["run_id"] in job_ids


def test_task_callback_failure_captures_exception(tmp_logger):
    lg, base = tmp_logger
    cbs = kage_task_callbacks(layer="silver", logger=lg)

    try:
        raise ValueError("downstream timeout")
    except ValueError as exc:
        ctx = _make_context(task_id="transform", exception=exc)
        cbs["on_execute_callback"](ctx)
        cbs["on_failure_callback"](ctx)

    events = _read_events(base, "task_run")
    failed = [e for e in events if e.get("status") == "FAILED"]
    assert failed, "expected FAILED task_run"
    cf = failed[0]["custom_fields"]
    assert cf["error_type"] == "ValueError"
    assert "downstream timeout" in cf["error_message"]
    assert "stack_trace" in cf and cf["stack_trace"]


def test_per_task_layer_override(tmp_logger):
    lg, base = tmp_logger
    bronze_cbs = kage_task_callbacks(layer="bronze", logger=lg)
    gold_cbs = kage_task_callbacks(layer="gold", logger=lg)

    bronze_cbs["on_execute_callback"](_make_context(task_id="ingest"))
    gold_cbs["on_execute_callback"](_make_context(task_id="aggregate",
                                                  run_id="run_2026_05_26",
                                                  try_number=1))

    events = _read_events(base, "task_run")
    layers = {e["task_name"]: e["layer"]
              for e in events if e.get("status") == "RUNNING"}
    assert layers.get("ingest") == "bronze"
    assert layers.get("aggregate") == "gold"


def test_dag_callbacks_emit_job_end(tmp_logger):
    lg, base = tmp_logger
    dag_cbs = kage_dag_callbacks(logger=lg)
    ctx = _make_context(run_id="run_dag_success")
    dag_cbs["on_success_callback"](ctx)

    events = _read_events(base, "job_run")
    ok = [e for e in events if e.get("status") == "SUCCESS"]
    assert ok and ok[0]["job_run_id"] == "run_dag_success"


def test_dag_failure_callback_carries_error(tmp_logger):
    lg, base = tmp_logger
    dag_cbs = kage_dag_callbacks(logger=lg)
    try:
        raise RuntimeError("dag-level failure")
    except RuntimeError as exc:
        ctx = _make_context(run_id="run_dag_fail", exception=exc)
        dag_cbs["on_failure_callback"](ctx)

    events = _read_events(base, "job_run")
    failed = [e for e in events if e.get("status") == "FAILED"]
    assert failed
    assert failed[0]["custom_fields"]["error_type"] == "RuntimeError"


# --------------------------------------------------------------------------- #
# log_dataset_from_context helper

def test_log_dataset_from_context_writes_event(tmp_logger):
    lg, base = tmp_logger
    ctx = _make_context(task_id="aggregate")
    log_dataset_from_context(
        ctx, layer="gold", dataset_name="customer_summary",
        record_count=4242, upstream_datasets=["silver_orders"],
        logger=lg,
    )
    events = _read_events(base, "dataset_event")
    target = [e for e in events if e["dataset_name"] == "customer_summary"]
    assert target
    assert target[0]["layer"] == "gold"
    assert target[0]["record_count"] == 4242
    assert "silver_orders" in target[0]["upstream_datasets"]
    cf = target[0]["custom_fields"]
    assert cf["airflow_dag_id"] == "orders_etl"


# --------------------------------------------------------------------------- #
# Extreme / minimal contexts

def test_missing_task_instance_uses_fallback_task_name(tmp_logger):
    lg, base = tmp_logger
    cbs = kage_task_callbacks(layer="bronze", logger=lg)
    # Bare context — no task_instance, no dag, just a run_id
    ctx = {"run_id": "bare_run"}
    cbs["on_execute_callback"](ctx)
    cbs["on_success_callback"](ctx)

    events = _read_events(base, "task_run")
    assert events
    names = {e.get("task_name") for e in events if "task_name" in e}
    assert "unnamed_task" in names


def test_callbacks_share_job_run_id_across_tasks(tmp_logger):
    """All tasks of one DAG run should share the same job_run_id."""
    lg, base = tmp_logger
    cbs = kage_task_callbacks(layer="bronze", logger=lg)
    run_id = "shared_run_id_xyz"
    for t in ("extract", "transform", "load"):
        ctx = _make_context(task_id=t, run_id=run_id)
        cbs["on_execute_callback"](ctx)
        cbs["on_success_callback"](ctx)

    events = _read_events(base, "task_run")
    job_ids = {e["job_run_id"] for e in events if "job_run_id" in e}
    assert job_ids == {run_id}
