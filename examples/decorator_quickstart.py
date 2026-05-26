#!/usr/bin/env python3
"""
KAGE Declarative API - @ decorator quickstart
KAGE-PROPRIETARY-2026-v1.1

Run:
    python examples/decorator_quickstart.py
"""
from kage import configure, dataset, pipeline, task

# 1. Configure the default logger once. All decorators below use it.
configure(base_path="./kage-logs", pipeline_name="customer_360")


@dataset(layer="landing", dataset_name="raw_orders_api", action="READ")
def fetch_orders():
    print("📥 fetching orders...")
    return 150_000  # record count


@dataset(layer="bronze", dataset_name="bronze_orders",
         upstream_datasets=["raw_orders_api"])
def clean_orders(_raw_count):
    print("🧼 cleaning orders...")
    return 128_394


@task(layer="bronze", task_name="bronze_clean")
def bronze_stage():
    raw = fetch_orders()
    return clean_orders(raw)


@dataset(layer="gold", dataset_name="customer_summary",
         upstream_datasets=["bronze_orders"])
def aggregate(bronze_count):
    print("📊 aggregating...")
    return bronze_count // 15


@pipeline("daily_customer_360")
def run_pipeline():
    bronze_count = bronze_stage()
    aggregate(bronze_count)
    print("✅ pipeline done")


if __name__ == "__main__":
    run_pipeline()
    print("\nLogs written to ./kage-logs/")
    print("Inspect: ls kage-logs/pyspark/event_type=*/dt=*/part-*.jsonl")
