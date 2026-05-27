"""
KAGE Agentic Observability tests.

Covers: sync/async decorators, nested spans, parent_span_id linkage,
sequential and parallel execution (asyncio.gather + ThreadPoolExecutor with
context propagation), exception capture, LLM usage tracking, and the
kage_span context manager.
"""
import asyncio
import contextvars
import json
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from kage import (
    KageLogger,
    agent,
    current_span,
    kage_span,
    llm_call,
    log_llm_usage,
    log_metric,
    set_default_logger,
    step,
    tool,
)


# --------------------------------------------------------------------------- #
# Helpers

def _read_events(base: Path, event_type: str):
    out = []
    for f in base.glob(f"**/event_type={event_type}/**/part-*.jsonl"):
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _wait_for(base: Path, event_type: str, timeout=2.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if list(base.glob(f"**/event_type={event_type}/**/part-*.jsonl")):
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def tmp_logger():
    base = tempfile.mkdtemp(prefix="kage-agentic-")
    lg = KageLogger(base_path=base, pipeline_name="agentic_pipeline",
                    platform="agent")
    set_default_logger(lg)
    yield lg, Path(base)
    shutil.rmtree(base, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Sync decorators

def test_agent_emits_job_run_and_root_span(tmp_logger):
    _, base = tmp_logger

    @agent("research_agent")
    def run(q):
        return f"answer to {q}"

    assert run("life?") == "answer to life?"
    assert _wait_for(base, "job_run")

    jobs = _read_events(base, "job_run")
    assert any(e["status"] == "RUNNING" for e in jobs)
    assert any(e["status"] == "SUCCESS" for e in jobs)
    assert all(e.get("kind") == "agent" for e in jobs)

    spans = _read_events(base, "task_run")
    root = [e for e in spans if e.get("kind") == "agent" and e["task_name"] == "research_agent"]
    assert root, "expected a root span with kind=agent"
    assert root[0]["parent_span_id"] is None


def test_step_decorator_links_to_parent(tmp_logger):
    _, base = tmp_logger

    @agent("orchestrator")
    def orchestrator():
        return inner_step()

    @step("inner_step")
    def inner_step():
        return 42

    orchestrator()

    spans = _read_events(base, "task_run")
    agent_span = next(s for s in spans
                      if s.get("kind") == "agent" and s["status"] == "RUNNING")
    step_span = next(s for s in spans
                     if s.get("kind") == "step" and s["status"] == "RUNNING")
    # inner_step's parent_span_id == agent_span's task_run_id
    assert step_span["parent_span_id"] == agent_span["task_run_id"]


def test_nested_spans_three_deep(tmp_logger):
    _, base = tmp_logger

    @agent
    def a():
        return b()

    @step
    def b():
        return c()

    @tool
    def c():
        return "ok"

    a()
    spans = _read_events(base, "task_run")
    # Build a parent map from RUNNING events only
    running = {s["task_name"]: s for s in spans if s["status"] == "RUNNING"}
    assert running["c"]["kind"] == "tool"
    assert running["c"]["parent_span_id"] == running["b"]["task_run_id"]
    assert running["b"]["parent_span_id"] == running["a"]["task_run_id"]
    assert running["a"]["parent_span_id"] is None


def test_tool_decorator_sets_kind_tool(tmp_logger):
    _, base = tmp_logger

    @tool("web_search")
    def search(q):
        return ["result1", "result2"]

    with kage_span("manual_root", kind="agent"):
        search("python")

    spans = _read_events(base, "task_run")
    tool_runs = [s for s in spans if s["task_name"] == "web_search"]
    assert tool_runs and all(s["kind"] == "tool" for s in tool_runs)


def test_llm_call_with_log_usage(tmp_logger):
    _, base = tmp_logger

    @llm_call(model="claude-opus-4-7", temperature=0.0)
    def call_claude(prompt):
        log_llm_usage(prompt_tokens=100, completion_tokens=250,
                      cost_usd=0.0042)
        return "response"

    with kage_span("agent_root", kind="agent"):
        call_claude("hello")

    spans = _read_events(base, "task_run")
    llm = [s for s in spans
           if s.get("kind") == "llm_call" and s["status"] == "SUCCESS"]
    assert llm, "expected SUCCESS llm_call event"
    cf = llm[0]["custom_fields"]
    assert cf["model"] == "claude-opus-4-7"
    assert cf["prompt_tokens"] == 100
    assert cf["completion_tokens"] == 250
    assert cf["total_tokens"] == 350
    assert cf["cost_usd"] == 0.0042


def test_exception_emits_failed_and_re_raises(tmp_logger):
    _, base = tmp_logger

    @step("broken")
    def broken():
        raise ValueError("nope")

    with kage_span("agent_root", kind="agent"):
        with pytest.raises(ValueError):
            broken()

    spans = _read_events(base, "task_run")
    failed = [s for s in spans if s["status"] == "FAILED"]
    assert failed
    cf = failed[0]["custom_fields"]
    assert cf["error_type"] == "ValueError"
    assert "nope" in cf["error_message"]
    assert cf["stack_trace"]


def test_log_metric_attaches_to_active_span(tmp_logger):
    _, base = tmp_logger

    @step("retrieve")
    def retrieve():
        log_metric(chunks_returned=42, retriever="faiss")
        return list(range(42))

    with kage_span("root", kind="agent"):
        retrieve()

    spans = _read_events(base, "task_run")
    success = [s for s in spans
               if s["task_name"] == "retrieve" and s["status"] == "SUCCESS"]
    assert success
    cf = success[0]["custom_fields"]
    assert cf["chunks_returned"] == 42
    assert cf["retriever"] == "faiss"


def test_no_parens_decorator(tmp_logger):
    _, base = tmp_logger

    @agent
    def named_agent():
        return "ok"

    assert named_agent() == "ok"
    jobs = _read_events(base, "job_run")
    names = {j.get("job_name") for j in jobs}
    assert "named_agent" in names


def test_latency_ms_is_recorded(tmp_logger):
    _, base = tmp_logger

    @step("slow")
    def slow():
        time.sleep(0.02)

    with kage_span("root", kind="agent"):
        slow()

    spans = _read_events(base, "task_run")
    success = [s for s in spans
               if s["task_name"] == "slow" and s["status"] == "SUCCESS"]
    assert success
    assert success[0].get("latency_ms", 0) >= 15


# --------------------------------------------------------------------------- #
# Async


def test_async_step(tmp_logger):
    _, base = tmp_logger

    @step("async_step")
    async def async_step():
        await asyncio.sleep(0.01)
        return 99

    async def runner():
        async with kage_span("root", kind="agent"):
            return await async_step()

    assert asyncio.run(runner()) == 99
    spans = _read_events(base, "task_run")
    assert any(s["task_name"] == "async_step" and s["status"] == "SUCCESS"
               for s in spans)


def test_async_parallel_children_share_parent(tmp_logger):
    """asyncio.gather() siblings must each see the enclosing parent span."""
    _, base = tmp_logger

    @tool("worker")
    async def worker(i):
        await asyncio.sleep(0.005)
        return i * 2

    @agent("parallel_agent")
    async def run():
        results = await asyncio.gather(worker(1), worker(2), worker(3))
        return sum(results)

    assert asyncio.run(run()) == 12

    spans = _read_events(base, "task_run")
    workers = [s for s in spans
               if s["task_name"] == "worker" and s["status"] == "RUNNING"]
    assert len(workers) == 3
    # All three siblings should share the same parent (the agent's root span)
    parents = {w["parent_span_id"] for w in workers}
    assert len(parents) == 1, f"expected 1 shared parent, got {parents}"


# --------------------------------------------------------------------------- #
# Thread-pool parallel with explicit context propagation


def test_threadpool_parallel_with_context_propagation(tmp_logger):
    _, base = tmp_logger

    @tool("thread_worker")
    def worker(i):
        return i * 2

    @agent("threaded_agent")
    def run():
        with ThreadPoolExecutor(max_workers=3) as ex:
            # One fresh context copy per submission - a Context can only be
            # entered once at a time.
            futures = [
                ex.submit(contextvars.copy_context().run, worker, i)
                for i in (1, 2, 3)
            ]
            return [f.result() for f in futures]

    assert run() == [2, 4, 6]

    spans = _read_events(base, "task_run")
    workers = [s for s in spans
               if s["task_name"] == "thread_worker" and s["status"] == "RUNNING"]
    assert len(workers) == 3
    parents = {w["parent_span_id"] for w in workers}
    assert len(parents) == 1


# --------------------------------------------------------------------------- #
# kage_span context manager


def test_kage_span_context_manager(tmp_logger):
    _, base = tmp_logger

    with kage_span("retrieve_chunks", kind="retrieval", k=10) as span:
        span.add(chunks_returned=8)

    spans = _read_events(base, "task_run")
    success = [s for s in spans
               if s["task_name"] == "retrieve_chunks" and s["status"] == "SUCCESS"]
    assert success
    cf = success[0]["custom_fields"]
    assert cf["k"] == 10
    assert cf["chunks_returned"] == 8
    assert success[0]["kind"] == "retrieval"


def test_current_span_returns_none_outside(tmp_logger):
    assert current_span() is None


def test_current_span_inside_with_block(tmp_logger):
    with kage_span("test_span", kind="step"):
        span = current_span()
        assert span is not None
        assert span.name == "test_span"
