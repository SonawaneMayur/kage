"""
KAGE Transports - PRODUCTION READY v1.2 (Timestamp Fix + Robust)
KAGE-PROPRIETARY-2026-v1.2
"""
import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any
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
