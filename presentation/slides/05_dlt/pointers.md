# Slide 5 — DLT / Lakeflow Integration (target: 60s)

## Core message
KAGE plays nicely with declarative pipelines — no architectural change.

## Talking points
1. `@kage_dlt_table` = `@dlt.table` + KAGE event in one decorator.
2. **Default skips `.count()`** — DLT already materializes the data; we don't want to double the compute on a 50B-row table.
3. **Streaming-safe** — `isStreaming` is auto-detected; no blocking.
4. **Quality rules** — `@kage_dlt_expectations` bundles multiple `@dlt.expect_or_drop` / `expect_or_fail`.
5. On exception: log `status=FAILED` + stack trace → re-raise → DLT marks the table failed. Single source of truth.

## Cases I've stress-tested (have ready to mention)
- **Positive** — happy bronze→silver→gold path with lineage.
- **Negative** — function raises → KAGE logs + re-raises; expectation fires → DLT aborts, KAGE captures via dataset_event.
- **Extreme** — empty DF (0 rows logged, no crash), streaming DF (no count), 50B-row batch (skip_count), exact count via `DataFrame.observe()`.

## Reference
Full example notebook at `examples/spark_declarative_cases.py`.

## Transition
*"Once events land, the lakehouse does the rest."* → slide 6.
