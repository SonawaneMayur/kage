"""
KAGE Transports - PRODUCTION READY v1.3 (Buffered + Rotating)
KAGE-PROPRIETARY-2026-v1.3
"""
import atexit
import json
import os
import threading
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

try:
    import dbutils
    DATABRICKS = True
except ImportError:
    DATABRICKS = False

class Transport(ABC):
    @abstractmethod
    def write(self, events: List[Dict[str, Any]]) -> None:
        pass

class FileTransport(Transport):
    def __init__(self, base_path: str, max_file_size_mb: int = 128):
        self.base_path = base_path.rstrip('/')
        self.max_file_size_mb = max_file_size_mb * 1024 * 1024

    def write(self, events: List[Dict[str, Any]]) -> None:
        """✅ PRODUCTION JSONL - NEVER FAILS"""
        if not events:
            return

        try:
            # ✅ CRITICAL FIX: ENSURE event_timestamp EXISTS
            for event in events:
                if "event_timestamp" not in event:
                    event["event_timestamp"] = datetime.utcnow().isoformat()

                # ✅ PRESERVE custom_fields (already fixed in core.py)
                if "custom_fields" not in event:
                    event["custom_fields"] = {}

            # ✅ USE FIRST EVENT FOR PARTITIONING (safe now)
            first_event = events[0]
            platform = first_event.get("platform", "pyspark")
            event_type = first_event.get("event_type", "unknown")
            dt = first_event["event_timestamp"][:10]  # YYYY-MM-DD

            # ✅ CORRECT PARTITION PATH
            dir_path = f"{self.base_path}/{platform}/event_type={event_type}/dt={dt}"

            print(f"📁 Writing to: {dir_path}")  # Debug

            if DATABRICKS:
                dbutils.fs.mkdirs(dir_path)
            else:
                os.makedirs(dir_path, exist_ok=True)

            self._write_batch(dir_path, events)
            print(f"✅ Wrote {len(events)} events")  # Success confirmation

        except Exception as e:
            print(f"❌ TRANSPORT ERROR: {str(e)}")
            self._emergency_fallback(events, str(e))

    def _write_batch(self, dir_path: str, events: List[Dict[str, Any]]) -> None:
        """Atomic JSONL writes - rotated files"""
        batch_id = uuid.uuid4().hex[:8]

        for i, event in enumerate(events):
            file_id = f"{batch_id}-{i:03d}"
            file_path = f"{dir_path}/part-{file_id}.jsonl"

            content = json.dumps(event, default=str) + "\n"

            if DATABRICKS:
                dbutils.fs.put(file_path, content, overwrite=True)
            else:
                with open(file_path, "w") as f:
                    f.write(content)

    def _emergency_fallback(self, events: List[Dict[str, Any]], error: str) -> None:
        print(f"KAGE EMERGENCY FALLBACK [{error}]:")
        for event in events:
            print(json.dumps(event, default=str))

class StdoutTransport(Transport):
    def write(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            print(json.dumps(event, default=str))

class MultiTransport(Transport):
    def __init__(self, transports: List[Transport]):
        self.transports = transports

    def write(self, events: List[Dict[str, Any]]) -> None:
        for transport in self.transports:
            try:
                transport.write(events)
            except Exception:
                pass  # Fire-and-forget

    def flush(self) -> None:
        for transport in self.transports:
            flush = getattr(transport, "flush", None)
            if callable(flush):
                try:
                    flush()
                except Exception:
                    pass

    def close(self) -> None:
        for transport in self.transports:
            close = getattr(transport, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass


# --------------------------------------------------------------------------- #
# High-throughput buffered transport
# --------------------------------------------------------------------------- #

class BufferedFileTransport(Transport):
    """High-throughput JSONL transport with batching + file rotation.

    Why this exists:
      The original FileTransport writes ONE file per event. At any non-trivial
      throughput that produces millions of tiny files, which kills S3 / DBFS /
      Spark scans. This transport batches events per partition (platform x
      event_type x date), appends to one rotating file per partition, and
      flushes from a background thread so producers never block on I/O.

    Behaviour:
      - Events accumulate in an in-memory buffer keyed by (platform,event_type,dt)
      - A partition flushes when ANY of:
          * its buffer reaches `batch_size` events
          * `flush_interval_sec` seconds elapse (background thread)
          * `flush()` / `close()` is called explicitly
      - Per partition, one "current" file is appended to until it exceeds
        `max_file_size_mb`, after which it rotates to a fresh `part-<uuid>.jsonl`.
      - On Databricks (where dbutils.fs.put requires full-file writes), every
        flush writes a fresh part file — still vastly fewer files than the
        per-event transport.
      - `atexit` registers `close()` so events buffered at interpreter shutdown
        are flushed (best-effort; not guaranteed under SIGKILL).
    """

    def __init__(
        self,
        base_path: str,
        *,
        batch_size: int = 500,
        flush_interval_sec: float = 2.0,
        max_file_size_mb: int = 128,
        background: bool = True,
    ):
        self.base_path = base_path.rstrip("/")
        self.batch_size = max(1, batch_size)
        self.flush_interval_sec = max(0.05, flush_interval_sec)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

        self._lock = threading.Lock()
        # (platform, event_type, dt) -> list[event_dict]
        self._buffers: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        # (platform, event_type, dt) -> {"path": str, "size": int}
        self._files: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._closed = False
        self._stop = threading.Event() if background else None
        self._thread: Optional[threading.Thread] = None

        if background:
            self._thread = threading.Thread(
                target=self._periodic_flush, name="kage-flush", daemon=True
            )
            self._thread.start()
            atexit.register(self.close)

    # ----- public ---------------------------------------------------------

    def write(self, events: List[Dict[str, Any]]) -> None:
        if self._closed or not events:
            return
        with self._lock:
            for event in events:
                self._enqueue_locked(event)
            self._maybe_flush_locked()

    def flush(self) -> None:
        """Flush all partitions immediately."""
        with self._lock:
            for key in list(self._buffers.keys()):
                self._flush_partition_locked(key)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.flush()

    # ----- internals ------------------------------------------------------

    def _enqueue_locked(self, event: Dict[str, Any]) -> None:
        if "event_timestamp" not in event:
            event["event_timestamp"] = datetime.utcnow().isoformat()
        if "custom_fields" not in event:
            event["custom_fields"] = {}
        platform = event.get("platform", "pyspark")
        event_type = event.get("event_type", "unknown")
        dt = event["event_timestamp"][:10]
        key = (platform, event_type, dt)
        self._buffers.setdefault(key, []).append(event)

    def _maybe_flush_locked(self) -> None:
        for key in list(self._buffers.keys()):
            if len(self._buffers[key]) >= self.batch_size:
                self._flush_partition_locked(key)

    def _flush_partition_locked(self, key: Tuple[str, str, str]) -> None:
        events = self._buffers.pop(key, [])
        if not events:
            return
        platform, event_type, dt = key
        dir_path = f"{self.base_path}/{platform}/event_type={event_type}/dt={dt}"
        content = "".join(json.dumps(e, default=str) + "\n" for e in events)
        encoded = content.encode("utf-8")

        try:
            if DATABRICKS:
                # No native append; write a fresh file per flush.
                dbutils.fs.mkdirs(dir_path)
                file_path = f"{dir_path}/part-{uuid.uuid4().hex[:12]}.jsonl"
                dbutils.fs.put(file_path, content, overwrite=True)
                # No file-state tracking needed on Databricks
                return

            os.makedirs(dir_path, exist_ok=True)
            state = self._files.get(key)
            if state is None or state["size"] + len(encoded) > self.max_file_size_bytes:
                file_path = f"{dir_path}/part-{uuid.uuid4().hex[:12]}.jsonl"
                state = {"path": file_path, "size": 0}
                self._files[key] = state
            with open(state["path"], "ab") as f:
                f.write(encoded)
            state["size"] += len(encoded)
        except Exception as e:
            # Never lose data to a write error — re-queue and surface
            print(f"KAGE BufferedFileTransport flush error: {e}")
            self._buffers.setdefault(key, []).extend(events)

    def _periodic_flush(self) -> None:
        while self._stop is not None and not self._stop.is_set():
            self._stop.wait(self.flush_interval_sec)
            if self._stop.is_set():
                break
            try:
                self.flush()
            except Exception as e:
                print(f"KAGE periodic flush error: {e}")
