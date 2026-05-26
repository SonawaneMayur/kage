# KAGE × Databricks — What KAGE Adds
<!-- LAYOUT: comparison -->

## Extends Databricks observability; does not replace it.

| Capability | Databricks today | What KAGE adds |
|---|---|---|
| Cross-platform schema | Unity Catalog & system tables are Databricks-only | One JSONL contract across Spark, Glue, Synapse, Snowflake, dbt, Airflow, plain Python |
| Medallion as first-class | Lineage knows tables, not layers | Every event carries `layer` — direct "gold survival %" queries, no joins |
| You own the storage | System tables retained per Databricks policy | Plain JSONL in your Volume / S3 / ADLS — your retention, your export |
| Business custom fields | Cluster tags don't reach the event stream | `custom_fields={owner, sla_minutes, cost_center}` on every event |
| Lineage outside Databricks | UC lineage requires Databricks reads/writes | `upstream_datasets=[...]` works from pandas, dbt, Airflow operators |
| AI-suggested remediation | Not in built-in observability today | `ai_query()` column on every analytics SQL — LLM fix per error / bottleneck |
| Cost attribution by layer | Billing is cluster-level | Synthetic cost per pipeline × layer × records — chargeback ready |
| DLT overlay without rewrites | DLT metrics are what DLT exposes | `@kage_dlt_table` adds your business context at materialization time |
| Deployment | Tied to Databricks runtime | `pip install kage` — <10 ms / event, library-only, no agent |

> "Same lens across every platform you run — medallion-aware, AI-augmented."
