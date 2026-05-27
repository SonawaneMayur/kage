#!/usr/bin/env python3
"""
KAGE Agentic Observability — Quickstart

A plain sequential agent built from four decorators. No medallion layers,
no Spark, no dbt — just spans with parent/child linkage and token tracking.

Run:
    python examples/agentic_quickstart.py
    ls kage-logs/agent/event_type=task_run/dt=*/part-*.jsonl

What you get:
    - one job_run (kind=agent, RUNNING + SUCCESS)
    - one root task_run (kind=agent, parent_span_id=None)
    - nested task_runs for each @step / @tool / @llm_call
    - parent_span_id on each child points to the enclosing span
"""
from kage import (
    agent, configure, kage_span,
    llm_call, log_llm_usage, log_metric, step, tool,
)

configure(base_path="./kage-logs", pipeline_name="research_agent",
          platform="agent", environment="dev")


# ---- "Tools" the agent can call ------------------------------------------

@tool("web_search")
def web_search(query: str):
    return ["doc-1", "doc-2", "doc-3"]


@tool("calculator")
def calculator(expression: str):
    return eval(expression, {"__builtins__": {}})


# ---- An LLM call wrapped so KAGE captures model + tokens -----------------

@llm_call(model="claude-opus-4-7", temperature=0.0)
def call_llm(prompt: str):
    # Imagine an SDK call here. Use log_llm_usage to attach the metrics
    # back onto the current span.
    log_llm_usage(prompt_tokens=120, completion_tokens=240, cost_usd=0.0084)
    return f"LLM response for: {prompt[:40]}..."


# ---- Steps composed from the above ---------------------------------------

@step("plan")
def plan(query: str):
    response = call_llm(f"Break this question into search queries: {query}")
    return [q.strip() for q in response.split(",")][:3]


@step("gather_evidence")
def gather_evidence(subqueries):
    """Sequential tool calls — each becomes a sibling span under this step."""
    all_results = []
    for q in subqueries:
        all_results.extend(web_search(q))
    log_metric(docs_retrieved=len(all_results))
    return all_results


@step("synthesize")
def synthesize(query, evidence):
    return call_llm(
        f"Question: {query}\nEvidence: {evidence}\nAnswer:"
    )


# ---- The agent entry point -----------------------------------------------

@agent("research_agent", agent_version="1.0")
def research(query: str):
    subqueries = plan(query)
    evidence = gather_evidence(subqueries)
    return synthesize(query, evidence)


if __name__ == "__main__":
    answer = research("What is photosynthesis?")
    print("\nAgent answer:", answer)
    print("\nLogs written under ./kage-logs/agent/")
    print("Try:")
    print("  cat kage-logs/agent/event_type=task_run/dt=*/part-*.jsonl | "
          "python -m json.tool | less")
