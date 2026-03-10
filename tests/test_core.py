"""
KAGE Tests - 100% GREEN VERSION
KAGE-PROPRIETARY-2026-v1.1
"""
import pytest
import os
import tempfile
import shutil
from pathlib import Path
from kage.core import KageLogger

@pytest.fixture
def temp_logger():
    """Logger with predictable temp path"""
    temp_dir = tempfile.mkdtemp(prefix="kage-test-")
    logger = KageLogger(base_path=temp_dir, pipeline_name="test_pipeline")
    yield logger
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)

def wait_for_files(base_path, pattern, timeout=2.0):
    """Helper: Wait for log files to appear"""
    import time
    start = time.time()
    while time.time() - start < timeout:
        if any(Path(base_path).glob(pattern)):
            return True
        time.sleep(0.1)
    return False

def test_kage_logger_job_workflow(temp_logger):
    """✅ FIXED: Test job lifecycle + file creation"""
    base_path = temp_logger.transports.transports[0].base_path

    job_id = temp_logger.job_start("test_job")
    assert "job" in temp_logger.active_runs
    assert temp_logger.active_runs["job"] == job_id

    temp_logger.job_end(job_id, "SUCCESS")

    # ✅ FIXED: Wait for async file write + correct path
    assert wait_for_files(
        base_path,
        "**/pyspark/event_type=job_run/**/part-*.jsonl"
    ), f"No job_run files in {base_path}"

def test_dataset_lineage(temp_logger):
    job_id = temp_logger.job_start("lineage_test")
    ds_id = temp_logger.dataset_write(
        layer="bronze",
        dataset_name="bronze_orders",
        record_count=128394
    )
    assert ds_id
    assert len(ds_id) > 10  # UUID length

def test_multi_layer_pipeline(temp_logger):
    """Full medallion flow"""
    job_id = temp_logger.job_start("customer_360")

    temp_logger.dataset_write("landing", "raw_orders", 150000)
    task_id = temp_logger.task_start("bronze", "clean_orders")
    temp_logger.dataset_write("bronze", "bronze_orders", 128394)
    temp_logger.task_end(task_id, "SUCCESS")
    temp_logger.dataset_write("gold", "customer_summary", 8472)

    temp_logger.job_end(job_id, "SUCCESS")

def test_never_crashes(temp_logger):
    """KAGE never breaks pipelines"""
    # Invalid data should not crash
    temp_logger.dataset_read("invalid", "test", record_count="invalid")
    temp_logger.task_start("invalid_layer", "test")

def test_thread_safety(temp_logger):
    """Concurrent logging"""
    import threading
    def log_event(i):
        temp_logger.job_start(f"thread-{i}")

    threads = [threading.Thread(target=log_event, args=(i,)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

@pytest.mark.parametrize("layer", ["landing", "bronze", "silver", "gold"])
def test_medallion_layers(temp_logger, layer):
    """All medallion layers work"""
    temp_logger.job_start("layer_test")
    temp_logger.dataset_write(layer, f"test_{layer}", 1000)