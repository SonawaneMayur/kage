# Slide 7 — Backup: KAGE × Databricks (skip unless asked)

## When to show this slide
Only when someone asks "how is this different from what Databricks already gives me?"
Do NOT run through this in the 5-minute slot — it eats your buffer.

## Framing (open with this)
> "KAGE doesn't replace Databricks observability. It gives you the **same
> lens across every platform you run** — Databricks, Snowflake, on-prem
> Spark, dbt, Airflow — with medallion semantics, business custom fields,
> and AI-suggested fixes built in."

## Talking points (pick 2–3 based on the questioner)
- **Cross-platform schema** — same JSONL contract everywhere; one SQL spans all clouds.
- **Medallion semantics** — `layer` is a first-class column; gold survival % is a direct query, not a derivation.
- **You own the data** — JSONL in your Volume / S3 / ADLS; export and retention are yours, not Databricks'.
- **Custom business fields** — owner, SLA, cost center attach to every event.
- **Lineage outside Databricks** — Python/dbt/Airflow code emits `upstream_datasets`; Unity Catalog can't see those today.
- **AI suggestions** — every analytics query takes `model_name=…` and tacks on an `ai_query()` column with remediation per row.

## Be honest about overlap (volunteer this)
- Column-level lineage *inside* Databricks → Unity Catalog wins, KAGE doesn't replace this.
- Lakehouse Monitoring covers profile / quality drift; KAGE's schema-drift query overlaps.
- Jobs UI shows job-level success/failure already — KAGE's value is making it cross-platform queryable.

## Closing line (after they're convinced)
> "Think of KAGE as the **glue** that makes Databricks observability portable
> to the rest of your stack — and as the place to put your *business* context
> next to the Databricks platform context."
