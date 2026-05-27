"""
KAGE x dbt integration tests.

Build synthetic target/run_results.json + target/manifest.json artifacts that
match the real dbt schema, then verify KAGE event emission.
"""
import json
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from kage import KageLogger
from kage.integrations.dbt import emit_dbt_run_results


# --------------------------------------------------------------------------- #
# Helpers

def _write_dbt_artifacts(target_dir: Path, *, results, nodes):
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "run_results.json").write_text(json.dumps({
        "metadata": {"invocation_id": "abc-123"},
        "args": {"target": "prod"},
        "elapsed_time": 12.34,
        "results": results,
    }))
    (target_dir / "manifest.json").write_text(json.dumps({"nodes": nodes}))


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


@pytest.fixture
def kage_dir():
    d = tempfile.mkdtemp(prefix="kage-dbt-")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def dbt_target(tmp_path):
    return tmp_path / "target"


# --------------------------------------------------------------------------- #
# Positive cases

def test_emit_dbt_run_results_happy_path(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[{
            "unique_id": "model.demo.bronze_orders",
            "status": "success",
            "execution_time": 1.2,
            "adapter_response": {"rows_affected": 1500},
            "message": None,
        }],
        nodes={
            "model.demo.bronze_orders": {
                "name": "bronze_orders",
                "resource_type": "model",
                "tags": ["bronze"],
                "schema": "analytics",
                "depends_on": {"nodes": ["source.demo.raw_orders"]},
            },
            "source.demo.raw_orders": {
                "name": "raw_orders", "resource_type": "source",
            },
        },
    )

    logger = KageLogger(base_path=str(kage_dir), pipeline_name="demo_dbt", platform="dbt")
    job_id = emit_dbt_run_results(dbt_target, pipeline_name="demo_dbt",
                                  base_path=str(kage_dir), logger=logger)

    assert job_id
    assert _wait_for(kage_dir, "dataset_event")
    ds = _read_events(kage_dir, "dataset_event")
    bronze = [e for e in ds if e["dataset_name"] == "bronze_orders"]
    assert bronze, "expected a dataset_event for bronze_orders"
    e = bronze[0]
    assert e["layer"] == "bronze"
    assert e["event_action"] == "WRITE"
    assert e["record_count"] == 1500
    assert "raw_orders" in e["upstream_datasets"]


def test_layer_inferred_from_tag(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[
            {"unique_id": "model.demo.silver_clean", "status": "success",
             "execution_time": 0.5, "adapter_response": {"rows_affected": 800}},
            {"unique_id": "model.demo.gold_summary", "status": "success",
             "execution_time": 0.3, "adapter_response": {"rows_affected": 42}},
        ],
        nodes={
            "model.demo.silver_clean": {
                "name": "silver_clean", "resource_type": "model",
                "tags": ["silver"], "schema": "analytics",
            },
            "model.demo.gold_summary": {
                "name": "gold_summary", "resource_type": "model",
                "config": {"tags": ["gold"]}, "schema": "analytics",
            },
        },
    )

    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir), logger=logger)

    ds = _read_events(kage_dir, "dataset_event")
    layers = {e["dataset_name"]: e["layer"] for e in ds}
    assert layers.get("silver_clean") == "silver"
    assert layers.get("gold_summary") == "gold"


def test_layer_inferred_from_schema_when_no_tag(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[{"unique_id": "model.demo.thing", "status": "success",
                  "execution_time": 0.1, "adapter_response": {"rows_affected": 1}}],
        nodes={"model.demo.thing": {
            "name": "thing", "resource_type": "model",
            "schema": "dbt_bronze",  # bronze in the schema
        }},
    )
    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir), logger=logger)
    ds = _read_events(kage_dir, "dataset_event")
    assert ds and ds[0]["layer"] == "bronze"


def test_default_layer_when_nothing_matches(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[{"unique_id": "model.demo.foo", "status": "success",
                  "execution_time": 0.1, "adapter_response": {"rows_affected": 1}}],
        nodes={"model.demo.foo": {"name": "foo", "resource_type": "model",
                                  "schema": "public"}},
    )
    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir),
                         default_layer="gold", logger=logger)
    ds = _read_events(kage_dir, "dataset_event")
    assert ds and ds[0]["layer"] == "gold"


# --------------------------------------------------------------------------- #
# Negative / extreme cases

def test_failed_model_emits_failed_task_and_no_dataset_event(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[{
            "unique_id": "model.demo.broken",
            "status": "error",
            "execution_time": 0.05,
            "adapter_response": {},
            "message": "relation \"src.missing\" does not exist",
        }],
        nodes={"model.demo.broken": {"name": "broken", "resource_type": "model",
                                     "tags": ["bronze"]}},
    )
    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir), logger=logger)

    tasks = _read_events(kage_dir, "task_run")
    failed = [t for t in tasks if t.get("status") == "FAILED"]
    assert failed, "expected a FAILED task_run"
    assert "missing" in failed[0]["custom_fields"]["error_message"]

    ds = _read_events(kage_dir, "dataset_event")
    assert not [e for e in ds if e["dataset_name"] == "broken"]

    jobs = _read_events(kage_dir, "job_run")
    assert any(j.get("status") == "FAILED" for j in jobs)


def test_missing_manifest_still_works(dbt_target, kage_dir):
    # Only run_results.json, no manifest
    dbt_target.mkdir(parents=True, exist_ok=True)
    (dbt_target / "run_results.json").write_text(json.dumps({
        "args": {"target": "dev"}, "elapsed_time": 1.0,
        "results": [{
            "unique_id": "model.demo.standalone", "status": "success",
            "execution_time": 0.1, "adapter_response": {"rows_affected": 100},
        }],
    }))

    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir),
                         default_layer="silver", logger=logger)
    ds = _read_events(kage_dir, "dataset_event")
    assert ds and ds[0]["dataset_name"] == "standalone"
    assert ds[0]["layer"] == "silver"   # default applied


def test_no_results_emits_empty_job(dbt_target, kage_dir):
    _write_dbt_artifacts(dbt_target, results=[], nodes={})
    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir), logger=logger)
    jobs = _read_events(kage_dir, "job_run")
    assert any(j.get("status") == "SUCCESS" for j in jobs)
    assert not _read_events(kage_dir, "task_run")
    assert not _read_events(kage_dir, "dataset_event")


def test_missing_run_results_raises(dbt_target, kage_dir):
    dbt_target.mkdir(parents=True, exist_ok=True)
    with pytest.raises(FileNotFoundError):
        emit_dbt_run_results(dbt_target, base_path=str(kage_dir))


def test_snapshot_and_seed_emit_dataset_events(dbt_target, kage_dir):
    _write_dbt_artifacts(
        dbt_target,
        results=[
            {"unique_id": "snapshot.demo.snap_a", "status": "success",
             "execution_time": 0.2, "adapter_response": {"rows_affected": 10}},
            {"unique_id": "seed.demo.lookup", "status": "success",
             "execution_time": 0.1, "adapter_response": {"rows_affected": 50}},
        ],
        nodes={
            "snapshot.demo.snap_a": {"name": "snap_a", "resource_type": "snapshot",
                                     "tags": ["bronze"]},
            "seed.demo.lookup": {"name": "lookup", "resource_type": "seed",
                                 "tags": ["landing"]},
        },
    )
    logger = KageLogger(base_path=str(kage_dir), platform="dbt")
    emit_dbt_run_results(dbt_target, base_path=str(kage_dir), logger=logger)
    names = {e["dataset_name"] for e in _read_events(kage_dir, "dataset_event")}
    assert {"snap_a", "lookup"}.issubset(names)
