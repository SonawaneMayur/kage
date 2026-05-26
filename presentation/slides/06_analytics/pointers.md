# Slide 6 — Analytics + Close (target: 45s)

## Core message
Logs aren't the destination — insights are. KAGE ships 10+ pre-built queries
plus opt-in AI suggestions.

## Talking points
1. The analytics module (`kage.utils.databricks_analytics`) ships SQL templates
   for the metrics every data team needs: SLA, health, root cause, cost, lineage.
2. Each query accepts an optional `model_name=` → it tacks on an
   `ai_query(...) AS resolution_suggestion` column. Plain text recommendations,
   inline with the metrics.
3. Output is Lakehouse-native — no extra service, no ingest pipeline.

## Closing line
> "Four lines to instrument. Zero infra. Every pipeline becomes a queryable,
> AI-suggestible asset."

## Call to action
- `pip install kage`
- Architecture: `docs/architecture.html`
- Examples: `examples/decorator_quickstart.py`, `examples/spark_declarative_cases.py`
- Questions?

## Backup answers (likely Q&A)
- **Q: Performance?** A: <10ms/event, thread-safe, atomic JSONL writes.
- **Q: Storage cost?** A: JSONL compresses ~10x with Delta; one event ≈ 1KB.
- **Q: How do I migrate existing logs?** A: Replay your job-log table into
  `dataset_write` / `job_start` calls — KAGE writes additive events.
