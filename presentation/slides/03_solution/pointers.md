# Slide 3 — How KAGE Works (target: 60s)

## Core message
Tiny surface area, lakehouse-shaped output. Two paradigms — ETL **and** agentic — sharing one event stream.

## Talking points
1. **3 event types** — `job_run`, `task_run`, `dataset_event`. The entire data model.
2. **Two API flavors** —
   - ETL: `@pipeline` / `@task(layer=…)` / `@dataset(layer=…)` for medallion pipelines.
   - Agentic: `@agent` / `@step` / `@tool` / `@llm_call` for spans (no medallion required).
3. **Four adapters** — DLT, dbt, Airflow, LangChain. Same JSONL output.
4. **Core engine** — thread-safe lock for ETL parallelism; `contextvars` span stack for agentic parallelism (asyncio.gather + ThreadPool with copy_context).
5. **JSONL on disk** — no daemon, no agent, no separate database. SQL it directly.

## If asked about architecture
Point to `docs/architecture.html` — six-layer stack:
User Code → KAGE API → Adapters → Core Engine → Storage → Analytics.

## Transition
*"Let's see the declarative version — this is where it gets fun."* → slide 4.
