#!/usr/bin/env python3
"""
KAGE x dbt - Quickstart

Workflow:
    1. Tag your dbt models with the medallion layer they belong to:
         # models/bronze/bronze_orders.sql
         {{ config(materialized='table', tags=['bronze']) }}
         select ...

    2. Run dbt as usual:
         dbt run --target prod

    3. Ingest the artifacts (this file does it programmatically; equivalent CLI
       is shown below):
         python -m kage.integrations.dbt target/ \\
             --pipeline-name orders_dbt --base-path ./kage-logs

This script can also be wired as an on-run-end hook in dbt_project.yml:

    # dbt_project.yml
    on-run-end:
      - "{{ run_query('select 1') }}"  # placeholder; better: a post-hook macro
      - "{{ exceptions.warn('Run: python -m kage.integrations.dbt target/') }}"

For most teams, a CI step that calls `python -m kage.integrations.dbt` after
`dbt run` is the cleanest pattern.
"""
from kage.integrations.dbt import emit_dbt_run_results


def main():
    # Assumes you've just run `dbt run` and your CWD is the dbt project root.
    job_id = emit_dbt_run_results(
        target_dir="target/",
        pipeline_name="orders_dbt",
        base_path="./kage-logs",
        default_layer="silver",
        environment="prod",
        job_name="orders_dbt_daily",
    )
    print(f"[OK] Emitted KAGE events for dbt run; job_run_id = {job_id}")
    print("Inspect:")
    print("  ls kage-logs/dbt/event_type=*/dt=*/part-*.jsonl")
    print()
    print("Query in Databricks SQL / DuckDB:")
    print("  SELECT pipeline_name, layer, dataset_name, record_count,")
    print("         custom_fields.dbt_resource_type")
    print("  FROM read_json_auto('kage-logs/dbt/event_type=dataset_event/**')")
    print("  ORDER BY event_timestamp DESC;")


if __name__ == "__main__":
    main()
