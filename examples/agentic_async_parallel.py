#!/usr/bin/env python3
"""
KAGE Agentic Observability — async + parallel patterns

Two concurrency styles you'll hit in real agents:

    1. asyncio.gather(...)              — fan out tool calls in parallel
       Parent linkage works automatically: contextvars propagate across awaits
       and across child coroutines.

    2. ThreadPoolExecutor + copy_context() — fan out sync tool calls
       Each submission needs its OWN context copy (a Context can only be
       entered once at a time).

Both produce KAGE events where the parallel children share one parent_span_id.
"""
import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor

from kage import agent, configure, llm_call, log_llm_usage, step, tool

configure(base_path="./kage-logs", pipeline_name="parallel_agent",
          platform="agent")


# ---- Async tools ---------------------------------------------------------

@tool("vector_search")
async def vector_search(query: str):
    await asyncio.sleep(0.05)
    return [f"chunk-{query}-{i}" for i in range(3)]


@tool("keyword_search")
async def keyword_search(query: str):
    await asyncio.sleep(0.03)
    return [f"kw-{query}-{i}" for i in range(2)]


@llm_call(model="claude-opus-4-7")
async def async_llm(prompt: str):
    await asyncio.sleep(0.02)
    log_llm_usage(prompt_tokens=80, completion_tokens=160, cost_usd=0.0055)
    return f"answer for {prompt[:40]}"


# ---- async fan-out -------------------------------------------------------

@step("hybrid_retrieve")
async def hybrid_retrieve(query: str):
    """Two tool calls in parallel — both will have the same parent_span_id."""
    vec, kw = await asyncio.gather(
        vector_search(query),
        keyword_search(query),
    )
    return vec + kw


@agent("async_agent")
async def async_agent(query: str):
    docs = await hybrid_retrieve(query)
    return await async_llm(f"Q: {query}\nDocs: {docs}")


# ---- threadpool fan-out (sync) ------------------------------------------

@tool("sync_fetch")
def sync_fetch(url: str):
    return f"body-of-{url}"


@step("fan_out_fetches")
def fan_out_fetches(urls):
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [
            ex.submit(contextvars.copy_context().run, sync_fetch, u)
            for u in urls
        ]
        return [f.result() for f in futures]


@agent("sync_threaded_agent")
def threaded_agent(urls):
    return fan_out_fetches(urls)


if __name__ == "__main__":
    print("Running async agent (asyncio.gather fan-out)...")
    asyncio.run(async_agent("photosynthesis"))

    print("Running threaded agent (ThreadPoolExecutor fan-out)...")
    threaded_agent(["a.com", "b.com", "c.com", "d.com"])

    print("\nInspect parallel children:")
    print("  jq '.task_name, .parent_span_id' "
          "kage-logs/agent/event_type=task_run/dt=*/part-*.jsonl | "
          "head -40")
