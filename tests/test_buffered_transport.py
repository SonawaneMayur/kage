"""
BufferedFileTransport tests — batching, concurrency, rotation, shutdown.
"""
import json
import shutil
import tempfile
import threading
import time
from pathlib import Path

import pytest

from kage import KageLogger
from kage.transports import BufferedFileTransport


def _list_part_files(base: Path, event_type: str):
    return sorted(base.glob(f"**/event_type={event_type}/**/part-*.jsonl"))


def _read_events(base: Path, event_type: str):
    out = []
    for f in _list_part_files(base, event_type):
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


@pytest.fixture
def tmp_base():
    d = tempfile.mkdtemp(prefix="kage-buf-")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Direct transport API


def test_batching_produces_one_file_per_batch(tmp_base):
    """Sanity: write 50 events with batch_size=50 -> exactly 1 file."""
    t = BufferedFileTransport(
        str(tmp_base), batch_size=50, flush_interval_sec=10, background=False,
    )
    for i in range(50):
        t.write([{"event_type": "job_run", "platform": "test", "i": i}])
    # batch_size hit -> auto flush
    files = _list_part_files(tmp_base, "job_run")
    assert len(files) == 1, f"expected 1 file, got {len(files)}"
    events = _read_events(tmp_base, "job_run")
    assert len(events) == 50
    t.close()


def test_flush_finalises_partial_batches(tmp_base):
    t = BufferedFileTransport(
        str(tmp_base), batch_size=1000, flush_interval_sec=10, background=False,
    )
    for i in range(7):
        t.write([{"event_type": "job_run", "platform": "test", "i": i}])
    # Buffer holds 7 events, no auto-flush triggered yet
    assert _list_part_files(tmp_base, "job_run") == []
    t.flush()
    events = _read_events(tmp_base, "job_run")
    assert len(events) == 7
    t.close()


def test_file_rotation_when_size_exceeded(tmp_base):
    # max_file_size_mb=0 forces rotation on every flush (any size > 0 exceeds)
    t = BufferedFileTransport(
        str(tmp_base), batch_size=2, flush_interval_sec=10,
        max_file_size_mb=0, background=False,
    )
    # 6 events / batch 2 = 3 flushes = 3 files
    for i in range(6):
        t.write([{"event_type": "job_run", "platform": "test", "i": i}])
    files = _list_part_files(tmp_base, "job_run")
    assert len(files) == 3
    t.close()


def test_partitioning_by_event_type_and_date(tmp_base):
    t = BufferedFileTransport(
        str(tmp_base), batch_size=100, flush_interval_sec=10, background=False,
    )
    t.write([
        {"event_type": "job_run", "platform": "test", "event_timestamp": "2026-01-01T00:00:00"},
        {"event_type": "task_run", "platform": "test", "event_timestamp": "2026-01-01T00:00:00"},
        {"event_type": "job_run", "platform": "test", "event_timestamp": "2026-01-02T00:00:00"},
    ])
    t.flush()
    assert len(_list_part_files(tmp_base, "job_run")) == 2     # two different dates
    assert len(_list_part_files(tmp_base, "task_run")) == 1
    t.close()


def test_concurrent_writers_are_thread_safe(tmp_base):
    t = BufferedFileTransport(
        str(tmp_base), batch_size=200, flush_interval_sec=10, background=False,
    )
    n_threads, per_thread = 8, 250
    barrier = threading.Barrier(n_threads)

    def writer(tid):
        barrier.wait()
        for i in range(per_thread):
            t.write([{"event_type": "task_run", "platform": "test",
                      "tid": tid, "i": i}])

    threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
    for th in threads: th.start()
    for th in threads: th.join()
    t.flush()

    events = _read_events(tmp_base, "task_run")
    assert len(events) == n_threads * per_thread
    # Each (tid, i) pair must appear exactly once
    seen = {(e["tid"], e["i"]) for e in events}
    assert len(seen) == n_threads * per_thread
    t.close()


def test_background_flush_thread_writes_within_interval(tmp_base):
    t = BufferedFileTransport(
        str(tmp_base), batch_size=10_000, flush_interval_sec=0.15,
        background=True,
    )
    try:
        t.write([{"event_type": "job_run", "platform": "test", "k": 1}])
        # Wait long enough for the background flush thread to wake up
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if _list_part_files(tmp_base, "job_run"):
                break
            time.sleep(0.05)
        assert _list_part_files(tmp_base, "job_run"), \
            "background thread should have flushed the buffer"
    finally:
        t.close()


def test_close_is_idempotent_and_flushes(tmp_base):
    t = BufferedFileTransport(
        str(tmp_base), batch_size=1000, flush_interval_sec=10, background=False,
    )
    t.write([{"event_type": "job_run", "platform": "test", "k": 1}])
    t.close()
    t.close()  # second call must not raise
    assert _read_events(tmp_base, "job_run") == [
        e for e in _read_events(tmp_base, "job_run")  # just verify it loaded
    ]
    # writes after close are silently dropped
    t.write([{"event_type": "job_run", "platform": "test", "k": 2}])
    assert len(_read_events(tmp_base, "job_run")) == 1


# --------------------------------------------------------------------------- #
# Through the KageLogger facade


def test_kage_logger_buffered_flag(tmp_base):
    lg = KageLogger(
        base_path=str(tmp_base), pipeline_name="buf_pipe", platform="pyspark",
        buffered=True, batch_size=50, flush_interval_sec=10,
    )
    try:
        for i in range(50):
            lg.job_start(f"job_{i}")
        # batch_size hit -> exactly 1 file should exist for job_run
        files = _list_part_files(tmp_base, "job_run")
        assert len(files) == 1, f"expected 1 batched file, got {len(files)}"
    finally:
        lg.close()


def test_kage_logger_flush_method(tmp_base):
    lg = KageLogger(
        base_path=str(tmp_base), pipeline_name="buf_pipe", platform="pyspark",
        buffered=True, batch_size=10_000, flush_interval_sec=10,
    )
    try:
        lg.job_start("job_a")
        assert _list_part_files(tmp_base, "job_run") == []
        lg.flush()
        assert _list_part_files(tmp_base, "job_run"), "flush() should write the event"
    finally:
        lg.close()


def test_kage_logger_non_buffered_default_unchanged(tmp_base):
    """Ensure existing default behaviour (per-event files) is preserved."""
    lg = KageLogger(base_path=str(tmp_base), pipeline_name="legacy",
                    platform="pyspark")
    for i in range(3):
        lg.job_start(f"job_{i}")
    files = _list_part_files(tmp_base, "job_run")
    assert len(files) == 3, "default FileTransport still writes one file per event"
