#!/usr/bin/env python3
"""
KAGE Quickstart - All 4 integration patterns
KAGE-PROPRIETARY-2026-v1.1
"""
from kage import KageLogger

print("🚀 KAGE Universal Pipeline Observability")

# PATTERN 1: Manual (most control)
print("\n1. MANUAL INTEGRATION (4 lines)")
logger = KageLogger(base_path="./kage-logs", pipeline_name="manual_pipeline")
job_id = logger.job_start("daily_etl")
logger.dataset_write("bronze", "orders", record_count=128394)
logger.job_end(job_id, "SUCCESS")
print("✅ Manual logs written")

# PATTERN 2: Full medallion pipeline
print("\n2. FULL MEDALLION PIPELINE (landing→bronze→gold)")
logger2 = KageLogger(base_path="./kage-logs", pipeline_name="customer_360")
job_id2 = logger2.job_start("customer_360_daily")

logger2.dataset_write("landing", "raw_orders_api", 150000)
task_id = logger2.task_start("bronze", "clean_orders")
logger2.dataset_write("bronze", "bronze_orders", 128394, upstream_datasets=["raw_orders_api"])
logger2.task_end(task_id, "SUCCESS", input_count=150000, output_count=128394)

logger2.dataset_write("gold", "customer_summary", 8472, upstream_datasets=["bronze_orders"])
logger2.job_end(job_id2, "SUCCESS")
print("✅ Medallion lineage captured")

print("\n📊 View logs:")
print("ls -la kage-logs/pyspark/event_type=dataset_event/")
print("head -n 5 kage-logs/pyspark/event_type=dataset_event/dt=*/part-*.jsonl")
