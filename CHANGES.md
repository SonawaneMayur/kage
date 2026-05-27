# KAGE Change Log

Running summary of changes made in each Claude Code session prompt.
Newest entries on top. Each prompt = one section.

---

## Prompt 8 — Refresh architecture diagrams (HTML + PPT)
**Date:** 2026-05-27
**Request:** Update the architecture diagram to reflect agentic + dbt + Airflow + LangChain additions.

### Modified files
- `docs/architecture.html`:
  - **Subtitle reframed** — "Universal observability for pipelines *and* agents. Two paradigms (medallion-aware ETL + span-based agentic) feed one event stream."
  - **Layer 1 (User Code)** — added `Agentic frameworks` (LangChain / LangGraph / custom), broke `Airflow / dbt` into separate cards, kept Spark / DLT / Python.
  - **Layer 2 (KAGE API Surface)** — split into 7 cards: imperative, declarative ETL, **agentic decorators** (`@agent` / `@step` / `@tool` / `@llm_call`), DLT, dbt, Airflow, LangChain.
  - **Layer 3 (Core Engine)** — added `contextvars span stack` (parallel/async/nested parent linkage) and `Auto error capture`. Updated schema card to note `layer` is now optional + `kind` and `parent_span_id` are first-class.
  - **Event-type pills** — added `kind = etl | agent | step | tool | llm_call | chain | retrieval`.
  - **Analytics layer** — added `Agentic trace queries` card (call-tree rebuild via `parent_span_id`, token/cost sums per agent).
  - **Trace section reorganised into two paradigms side-by-side** — ETL trace (`@kage_dlt_table`) and Agentic trace (`@agent + @step + @tool`). Each gets a sibling "safety note" panel: streaming-safe for ETL, concurrency-safe for agentic.
  - **Component → File Map** — added rows for `kage/agentic.py`, `kage/integrations/dbt.py`, `kage/integrations/airflow.py`, `kage/integrations/langchain.py` with their test files.
  - **Legend** — adds an "11+ entry points · one event stream" callout.
- `presentation/generate_pptx.py`:
  - Architecture slide layer cards updated. New 6-layer stack: User Code → KAGE API → **Adapters** (renamed from Transports) → Core Engine → Storage → Analytics.
  - Layer body text reflects agentic + LangChain + dbt + Airflow: `@kage_dlt_table · emit_dbt_run_results · kage_task_callbacks · KageLangChainCallback`.
  - Core Engine card now mentions `contextvars span stack` alongside the thread-safe lock.
- `presentation/slides/03_solution/slide.md` — subtitle updated to "3 event types · ETL + Agentic APIs · 4 adapters · 1 JSONL stream".
- `presentation/slides/03_solution/pointers.md` — talking points rewritten to cover both paradigms and four adapters.
- `presentation/kage_5min.pptx` — regenerated.

### Test status
**70 / 70 passing** (no production code touched — diagram + slide updates only).

---

## Prompt 7 — Agentic observability (any sequential / parallel flow)
**Date:** 2026-05-27
**Request:** Make KAGE work for agentic frameworks too — capture business + system logs at any scale across sequential or parallel "layers" that don't fit the medallion model.

### New files
- `kage/agentic.py`:
  - `@agent(name=None, **fields)` — wraps an agent invocation, emits a `job_run` (kind=agent) + a root span; sync and async both supported.
  - `@step` / `@tool` / `@llm_call` — generic span decorators (`kind` set automatically); each detects sync vs async via `inspect.iscoroutinefunction` and dispatches accordingly.
  - `kage_span(name, kind=..., **fields)` — context manager (works as `with` and `async with`); has `.add(**fields)` to attach fields mid-span.
  - `log_llm_usage(prompt_tokens=, completion_tokens=, total_tokens=, cost_usd=, model=)` — attach metrics to the active span (no-op outside one).
  - `log_metric(**fields)` — attach arbitrary fields to the active span.
  - `current_span()` — return the active span object.
  - Parent/child linkage uses `contextvars` (`_active_span_stack`, `_active_job_id`) so `asyncio.gather` siblings, nested calls, and `ThreadPoolExecutor` with `contextvars.copy_context().run` all see correct `parent_span_id`. Parallel agents do not clobber each other's `job_run_id`.
  - `latency_ms` automatically recorded on every end event via `time.perf_counter()`.
- `kage/integrations/langchain.py`:
  - `KageLangChainCallback(BaseCallbackHandler)` translates LangChain / LangGraph callbacks (`on_chain_*`, `on_llm_*`, `on_chat_model_*`, `on_tool_*`, `on_retriever_*`) into KAGE span events.
  - LangChain `run_id` -> KAGE `task_run_id`; `parent_run_id` -> KAGE `parent_span_id`.
  - Token usage extracted from both legacy `llm_output.token_usage` and newer `usage_metadata` paths.
  - Lazy import: subclasses a stub when `langchain_core` isn't installed; instantiation raises a helpful `ImportError`.
- `tests/test_agentic.py` — 15 tests: sync/async decorators, 3-level nesting + parent linkage, tool/llm_call kinds, `log_llm_usage` populating prompt/completion/total tokens + cost + model, exception → FAILED + stack_trace + re-raise, no-parens form, `latency_ms` recording, `kage_span` context manager, `current_span()`, `asyncio.gather` parallel children sharing a parent, `ThreadPoolExecutor` with explicit context propagation.
- `tests/test_langchain_integration.py` — 7 tests using a stubbed `langchain_core.callbacks.BaseCallbackHandler`: chain start/end, parent_run_id → parent_span_id mapping, token usage capture, tool error → FAILED, retriever doc_count, unknown end-event safety, helpful ImportError when LangChain isn't installed.
- `examples/agentic_quickstart.py` — sequential research-agent demo (plan → gather → synthesize) with `@agent` / `@step` / `@tool` / `@llm_call` and `log_llm_usage` / `log_metric`.
- `examples/agentic_async_parallel.py` — both concurrency styles: `asyncio.gather` for async tools and `ThreadPoolExecutor + copy_context().run` for sync tools. Demonstrates parent linkage works for both.
- `examples/agentic_langchain.py` — drop-in `KageLangChainCallback` setup for any LangChain / LangGraph runnable.

### Modified files
- `kage/schemas.py` — `TaskRunEvent.layer` is now `Optional[str]` (was `Literal["landing","bronze","silver","gold"]`); new fields `kind: Optional[str]` and `parent_span_id: Optional[str]`. Same loosening on `DatasetEvent.layer` (with optional `kind`). Backward compatible — existing medallion calls still validate.
- `kage/__init__.py` — exports the agentic API (`agent`, `step`, `tool`, `llm_call`, `kage_span`, `current_span`, `log_llm_usage`, `log_metric`) and `KageLangChainCallback`.
- `README.md` — new top-line callout for agentic observability, full "Agentic Observability (Spans for Agents, Tools, LLMs)" section with sequential/parallel/async patterns and the LangChain adapter, extended API Reference.

### Design decisions
- **Reuse `event_type=task_run`** rather than introducing a new event type — analytics queries and partitioned-JSONL storage keep working unchanged; `kind` discriminates agentic vs ETL spans.
- **Layer is optional everywhere** — schemas no longer enforce medallion vocabulary, but `@task(layer=...)` continues to work for ETL.
- **`contextvars` over thread-local state** — the only way parallel async children get correct `parent_span_id` for free. ThreadPoolExecutor is documented to require `copy_context().run` (and the test exercises it).
- **`@agent` doesn't mutate `KageLogger.active_runs["job"]`** — instead it sets `_active_job_id` via contextvar, so two agents running in parallel keep their own job ids.
- **`KageLangChainCallback` is opt-in** — module imports cleanly without LangChain; only instantiation raises if `langchain_core` is missing.

### Test status
**70 / 70 passing** (5 core + 11 decorators + 11 DLT + 4 parametrized + 8 dbt + 8 airflow + 15 agentic + 7 langchain + 1 medallion).

---

## Prompt 6 — dbt + Airflow adapters, tests, examples, README
**Date:** 2026-05-27
**Request:** Develop code, adapters, test cases for dbt and Airflow; provide examples; update README.

### New files
- `kage/integrations/dbt.py` — parses `target/run_results.json` + `target/manifest.json`, emits one `job_run` + one `task_run` + one `dataset_event` per model/snapshot/seed. Layer inference: model tags → schema name substring → `default_layer` fallback. Lineage from `depends_on.nodes`. Has a `python -m kage.integrations.dbt` CLI.
- `kage/integrations/airflow.py`:
  - `kage_task_callbacks(layer, logger=None)` returns `on_execute_callback` + `on_success_callback` + `on_failure_callback` for `default_args` or per-operator.
  - `kage_dag_callbacks(logger=None)` returns DAG-level `on_success_callback` + `on_failure_callback`.
  - `log_dataset_from_context(context, layer, dataset_name, …)` for inline dataset events.
  - All-task KAGE `job_run_id` is bound to Airflow's `run_id` (mutates `logger.active_runs["job"]` in the execute callback) — every task of a DAG run shares one job.
- `tests/test_dbt_integration.py` — 8 tests: happy path, layer-by-tag, layer-by-schema, default-layer fallback, FAILED model + no dataset_event, missing manifest, empty run, snapshots/seeds.
- `tests/test_airflow_integration.py` — 8 tests: start/success, failure with stack_trace, per-task layer override, DAG-level success + failure, `log_dataset_from_context`, minimal context fallback, shared `job_run_id` across tasks. Mocks Airflow context with `types.SimpleNamespace` — no Airflow install needed.
- `examples/dbt_quickstart.py` — programmatic + CLI ingestion of `target/`.
- `examples/dbt_cases.py` — positive (bronze→silver→gold), negative (failed model), extreme (empty run, untagged → default_layer).
- `examples/airflow_quickstart.py` — full DAG with mixed-layer callbacks + `log_dataset_from_context` inside task bodies.
- `examples/airflow_cases.py` — positive/negative/DAG-failure with a self-contained mini-runner that fabricates Airflow contexts.

### Modified files
- `kage/__init__.py` — exports `emit_dbt_run_results`, `kage_task_callbacks`, `kage_dag_callbacks`, `log_dataset_from_context`.
- `README.md` — new dbt section (tag + ingest + CLI), new Airflow section (callbacks + `log_dataset_from_context`), updated platform tagline, extended API Reference.

### Design decisions
- **dbt artifacts-based, not adapter-injected** — KAGE doesn't need a dbt plugin; it post-processes `target/` after the run. Works with any dbt version and any adapter (Snowflake, BigQuery, Databricks, DuckDB).
- **Airflow callbacks, not Operator subclasses** — keeps the integration zero-imports-on-Airflow-side. Users add `**kage_task_callbacks(...)` to existing operators, no rewrites.
- **Process-local task_id stash on `ti.kage_task_id`** — Airflow runs `on_execute_callback` and `on_success/failure_callback` for the same try in the same worker process, so the stash is safe. Falls back to a synthetic id if missing.
- **No DAG-level `job_start` event** — Airflow has no DAG-level `on_execute_callback`; we emit only `job_end`. Start time can be derived from earliest `task_run` event.

### Test status
**48 / 48 passing** (5 core + 11 decorators + 11 DLT + 4 parametrized + 8 dbt + 8 airflow + 1 medallion).

---

## Prompt 5 — Backup comparison slide (KAGE × Databricks)
**Date:** 2026-05-26
**Request:** After answering "what does KAGE support that Databricks doesn't?", user asked to add the comparison as a backup slide.

### New files
- `presentation/slides/07_backup_databricks/slide.md` — comparison slide marked `<!-- LAYOUT: comparison -->`. 9-row markdown table covering cross-platform schema, medallion-first-class, ownership, custom fields, lineage outside Databricks, AI suggestions, cost attribution by layer, DLT overlay, library-only deployment.
- `presentation/slides/07_backup_databricks/pointers.md` — speaker notes: framing line, talking points to pick from based on questioner, honest overlap callouts (UC column lineage, Lakehouse Monitoring, Jobs UI), closing line.

### Modified files
- `presentation/generate_pptx.py`:
  - Parser now recognises GitHub-flavoured markdown tables (`|...|...|` rows + `|---|---|` separator).
  - New `build_comparison_slide()` emits a **native PowerPoint table** (`add_table`) — every cell editable, header row filled with the accent colour, zebra-striped body, named `kage_databricks_comparison` for fast selection in PPT.
  - `add_page_number()` is now dynamic (`{idx} / {total}`) so adding/removing slide folders updates the footer automatically.

### Design decisions
- **Backup, not main** — the slide is positioned as #7 and lives outside the 5-minute timing. The `pointers.md` opens with "skip unless asked." Page numbers now read `N / 7` across the deck.
- **Honest framing** — slide subtitle is *"Extends Databricks observability; does not replace it."* Notes explicitly call out the three areas where Databricks already wins (UC column lineage, Lakehouse Monitoring, Jobs UI) to avoid overclaiming during Q&A.
- **Native table over image** — same principle as the architecture slide: every cell stays editable in PowerPoint.

### Test status
**31 / 31 passing** (no production code touched).

---

## Prompt 4 — 5-minute presentation deck
**Date:** 2026-05-26
**Request:** Prepare a 5-min presentation with talking points in separate folders and a modifiable PPT. Follow-up: use white background and a high-level **editable** architecture diagram.

### New files
- `presentation/` (new top-level folder)
  - `generate_pptx.py` — builder script (`python -m pptx`-based). Parses each `slides/<NN>_<name>/slide.md` and emits one slide. White/light palette with blue accent.
  - `kage_5min.pptx` — the generated, modifiable deck (6 slides, 16:9, speaker notes attached).
  - `slides/01_title/` through `slides/06_analytics/` — one folder per slide, each containing:
    - `slide.md` — slide content (title, subtitle, bullets, code blocks, quote)
    - `pointers.md` — speaker notes / talking points / per-slide timing target
- Memory: `.claude/projects/.../memory/presentation_preferences.md` — preserves the white-theme + native-editable-shapes preference for future sessions.

### Design decisions
- `slide.md` supports `<!-- LAYOUT: architecture -->`. When set, `build_architecture_slide()` emits 6 rounded-rectangle layer cards + 5 connector arrows as **native PowerPoint shapes** (each `layer__*` and `arrow_down` is selectable, movable, recolourable). No images baked in.
- 6-slide outline targets 5:00 total: Title (30s), Problem (45s), How KAGE Works / arch diagram (60s), Declarative `@` API (60s), DLT integration (60s), Analytics + close (45s).
- `pointers.md` is attached verbatim as PowerPoint speaker notes — the user reads them in presenter view.

### Test status
**31 / 31 passing** (no production code touched).

---

## Prompt 3 — HTML architecture diagram
**Date:** 2026-05-26
**Request:** Add an architecture diagram in HTML.

### New files
- `docs/architecture.html` — self-contained, single-file diagram. No external CSS / JS / fonts; renders in any browser offline. Shows the 6-layer stack (User Code → API Surface → Core Engine → Transports → Storage → Analytics), medallion layer strip, event-type pills, a step-by-step trace of `@kage_dlt_table` firing, and a Component→File map.

### Design decisions
- Pure HTML/CSS (no SVG library, no Mermaid CDN) so the file works without internet — important for Databricks Workspace where outbound network is often blocked.
- Dark theme matching GitHub's palette so it reads well alongside the repo.

### Test status
**31 / 31 passing** (no code changes; existing tests still green).

---

## Prompt 2 — Spark Declarative Pipelines (DLT / Lakeflow) integration
**Date:** 2026-05-26
**Request:** Compatibility with Spark Declarative Pipelines + decorator
quickstart + positive/negative/extreme case examples.

### New files
- `kage/integrations/__init__.py` — integrations subpackage.
- `kage/integrations/spark_declarative.py` — DLT bridge:
  - `@kage_dlt_table(layer, upstream_datasets, skip_count=True, record_count_fn, extra_fields, **dlt_kwargs)` — stacks `@dlt.table` + KAGE `dataset_event`.
  - `@kage_dlt_view(layer, upstream_datasets, ...)` — same for `@dlt.view`, logs `action=READ`.
  - `@kage_dlt_expectations((action, name, expr), ...)` — bundles multiple `@dlt.expect_*`.
  - `is_dlt_available()` helper.
  - Lazy `dlt` import — `import kage` works without DLT; only calling decorators outside Databricks raises a clear `ImportError`.
- `examples/spark_declarative_quickstart.py` — landing → bronze → silver → gold notebook with streaming, expectations, view.
- `examples/spark_declarative_cases.py` — 8 sections (A–H) covering positive (simple table, lineage + expectations), negative (raise, expectation fires), extreme (empty, streaming, huge batch, observed-count via `DataFrame.observe()`).
- `tests/test_spark_declarative.py` — 11 tests using a stubbed `dlt` module installed into `sys.modules` so the integration runs outside Databricks.

### Modified files
- `kage/decorators.py` — `_infer_record_count` now respects `isStreaming` (never blocks on streaming DataFrames).
- `kage/__init__.py` — exports `kage_dlt_table`, `kage_dlt_view`, `kage_dlt_expectations`, `is_dlt_available`.
- `README.md` — new "Spark Declarative Pipelines" Quickstart section, 6-step implementation guide, case-handling cheat sheet, and API Reference block.

### Design decisions
- `skip_count=True` is the **default** — DLT already materializes the DataFrame, so an extra `.count()` would double the compute on large batch tables.
- Streaming DataFrames auto-skip count even when `skip_count=False` (the `isStreaming` guard in `_infer_record_count`).
- On exception: KAGE logs `dataset_event` with `status=FAILED`, `error_type`, `error_message`, `stack_trace`, then re-raises so DLT still marks the table failed.
- `extra_fields={...}` merges into `custom_fields` for per-table metadata (owner, SLA).

### Test status
**31 / 31 passing** (5 core + 11 decorators + 11 DLT + 4 parametrized).

---

## Prompt 1 — Audit + declarative `@` decorator API
**Date:** 2026-05-26
**Request:** Review project, fix anything missing, add a declarative `@`-style logging API.

### Bugs fixed
- `kage/core.py:144` — `dataset_write` was delegating to `dataset_read`, which hardcoded `event_action: "READ"`; writes appeared as reads. Refactored to a shared `_dataset_event(action, ...)` helper.
- `kage/transports.py:54` — Local filesystem partition directory was never `mkdir`'d (only Databricks path was); writes silently failed locally. Added `os.makedirs(..., exist_ok=True)`.
- `kage/utils/databricks_analytics.py:22` — Top-level `from pyspark.sql import ...` broke `import kage` whenever pyspark wasn't installed. Made the import lazy.

### New files
- `kage/decorators.py` — declarative API:
  - `@pipeline(name=None, **fields)` — wraps a job; emits `job_run` start/end, captures exceptions as FAILED with `error_type` / `error_message` / `stack_trace`, re-raises.
  - `@task(layer, task_name=None, **fields)` — wraps a medallion-layer step; emits `task_run` start/end.
  - `@dataset(layer, dataset_name=None, action="WRITE", upstream_datasets, record_count_fn)` — emits `dataset_event` with auto record-count inference (PySpark `DataFrame.count()` → `int` return → `len()`).
  - `configure(**kwargs)` / `set_default_logger(logger)` / `get_default_logger()` for the shared default logger backing the decorators.
  - Both short (`pipeline`, `task`, `dataset`) and namespaced (`kage_pipeline`, …) names exported.
- `tests/test_decorators.py` — 11 tests covering success, failure + re-raise + error fields, no-parens form, action=READ/WRITE, custom `record_count_fn`, len-inference, full pipeline+task+dataset composition.
- `examples/decorator_quickstart.py` — end-to-end runnable demo.

### Modified files
- `kage/__init__.py` — exports decorators + `configure` helpers.
- `README.md` — replaced the broken decorator-pattern stub with a real declarative-API section, clarified that `install_spark_listener` is a preview stub (was incorrectly claimed to "Auto-log reads, writes, AND errors").

### Test status
**20 / 20 passing** (5 core + 11 decorators + 4 parametrized).

---

## How to read / update this file

- Newest prompt section goes on top, beneath the title.
- Each section follows the template: `New files` → `Modified files` → `Design decisions` (optional) → `Test status`.
- Mention exact file paths with `:line` when fixing a specific bug.
- Cite test counts so future sessions can verify nothing regressed.
