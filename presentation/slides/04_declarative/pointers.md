# Slide 4 — Declarative `@` API (target: 60s)

## Core message
Three decorators replace ~30 lines of try/except + logging boilerplate.

## Talking points (walk top→bottom)
1. **`configure(...)`** — set once per process; all decorators share it.
2. **`@dataset`** — auto record_count via `DataFrame.count()`, `len()`, or `int` return; pass `upstream_datasets=[...]` and lineage is wired.
3. **`@task(layer=...)`** — medallion-aware, emits `task_run` start/end.
4. **`@pipeline`** — wraps a job; on exception logs FAILED with `error_type` + `error_message` + `stack_trace`, then **re-raises** (KAGE never swallows errors).
5. **`@pipeline` works without parens** — `@pipeline` alone uses the function name.

## What to say if someone asks "what about streaming?"
"Streaming DataFrames are auto-detected via `isStreaming` — no `.count()` call, no blocking. Same decorator, no special case in your code."

## Transition
*"And if your platform is Databricks DLT or Lakeflow — one more decorator."*
→ slide 5.
