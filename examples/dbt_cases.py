#!/usr/bin/env python3
"""
KAGE x dbt - Positive / negative / extreme case handling

This script is self-contained: it fabricates dbt-shaped artifacts in a
temp directory so you can see KAGE emit the right events for each case
without actually running dbt.

Sections:
    A. Positive   - happy bronze + silver + gold run with lineage
    B. Negative   - one model errors; KAGE logs FAILED and skips dataset_event
    C. Extreme    - empty run / missing manifest / unknown schema
"""
import json
import shutil
import tempfile
from pathlib import Path

from kage import KageLogger
from kage.integrations.dbt import emit_dbt_run_results


def write_artifacts(target_dir: Path, results, nodes):
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "run_results.json").write_text(json.dumps({
        "metadata": {"invocation_id": "demo-1"},
        "args": {"target": "prod"},
        "elapsed_time": 4.2,
        "results": results,
    }))
    (target_dir / "manifest.json").write_text(json.dumps({"nodes": nodes}))


def run_case(label, *, results, nodes, default_layer="silver"):
    tmp = Path(tempfile.mkdtemp(prefix="kage-dbt-case-"))
    target = tmp / "target"
    write_artifacts(target, results, nodes)
    kage_logs = tmp / "kage-logs"
    logger = KageLogger(base_path=str(kage_logs), pipeline_name="demo_dbt",
                        platform="dbt")
    emit_dbt_run_results(target, default_layer=default_layer, logger=logger)
    print(f"\n=== {label} ===")
    for ev_type in ("job_run", "task_run", "dataset_event"):
        files = list(kage_logs.glob(f"**/event_type={ev_type}/**/part-*.jsonl"))
        print(f"  {ev_type}: {len(files)} files")
    shutil.rmtree(tmp, ignore_errors=True)


# A. Positive ------------------------------------------------------------
positive_results = [
    {"unique_id": "model.demo.bronze_orders", "status": "success",
     "execution_time": 1.1, "adapter_response": {"rows_affected": 150000}},
    {"unique_id": "model.demo.silver_orders", "status": "success",
     "execution_time": 2.3, "adapter_response": {"rows_affected": 128394}},
    {"unique_id": "model.demo.gold_revenue", "status": "success",
     "execution_time": 0.4, "adapter_response": {"rows_affected": 84}},
]
positive_nodes = {
    "model.demo.bronze_orders": {"name": "bronze_orders", "resource_type": "model",
                                 "tags": ["bronze"], "schema": "analytics",
                                 "depends_on": {"nodes": ["source.demo.raw_orders"]}},
    "model.demo.silver_orders": {"name": "silver_orders", "resource_type": "model",
                                 "tags": ["silver"], "schema": "analytics",
                                 "depends_on": {"nodes": ["model.demo.bronze_orders"]}},
    "model.demo.gold_revenue":  {"name": "gold_revenue", "resource_type": "model",
                                 "tags": ["gold"], "schema": "analytics",
                                 "depends_on": {"nodes": ["model.demo.silver_orders"]}},
    "source.demo.raw_orders": {"name": "raw_orders", "resource_type": "source"},
}

# B. Negative ------------------------------------------------------------
negative_results = [
    {"unique_id": "model.demo.bronze_orders", "status": "success",
     "execution_time": 1.0, "adapter_response": {"rows_affected": 150000}},
    {"unique_id": "model.demo.silver_broken", "status": "error",
     "execution_time": 0.05, "adapter_response": {},
     "message": 'column "amount_usd" does not exist'},
]
negative_nodes = {
    "model.demo.bronze_orders": {"name": "bronze_orders", "resource_type": "model",
                                 "tags": ["bronze"]},
    "model.demo.silver_broken": {"name": "silver_broken", "resource_type": "model",
                                 "tags": ["silver"]},
}

# C. Extreme -------------------------------------------------------------
empty_results = []
empty_nodes = {}

untagged_results = [
    {"unique_id": "model.demo.foo", "status": "success",
     "execution_time": 0.1, "adapter_response": {"rows_affected": 99}},
]
untagged_nodes = {
    # no tag, no schema match -> falls back to default_layer
    "model.demo.foo": {"name": "foo", "resource_type": "model", "schema": "public"},
}


if __name__ == "__main__":
    run_case("A. positive (bronze -> silver -> gold)",
             results=positive_results, nodes=positive_nodes)
    run_case("B. negative (one failed model)",
             results=negative_results, nodes=negative_nodes)
    run_case("C1. extreme: empty run",
             results=empty_results, nodes=empty_nodes)
    run_case("C2. extreme: untagged model -> default_layer=gold",
             results=untagged_results, nodes=untagged_nodes,
             default_layer="gold")
