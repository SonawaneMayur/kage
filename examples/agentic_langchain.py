#!/usr/bin/env python3
"""
KAGE x LangChain / LangGraph — Quickstart

Drop the KageLangChainCallback into any LangChain / LangGraph runnable and
KAGE captures every chain / LLM / tool / retriever run as a span.

Install:
    pip install langchain-core langchain-openai  # (or your provider)

Usage:
    handler = KageLangChainCallback()
    chain.invoke(input, config={"callbacks": [handler]})

Below is a self-contained skeleton; uncomment the LangChain import + the
real chain to wire it up.
"""
from kage import configure
from kage.integrations.langchain import KageLangChainCallback

configure(
    base_path="./kage-logs",
    pipeline_name="lc_agent",
    platform="langchain",
)


def run_with_handler():
    # ---- Plug into ANY LangChain runnable ---------------------------------
    #
    # from langchain_openai import ChatOpenAI
    # from langchain_core.prompts import ChatPromptTemplate
    #
    # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    # prompt = ChatPromptTemplate.from_messages([
    #     ("system", "You are a helpful assistant."),
    #     ("human", "{query}"),
    # ])
    # chain = prompt | llm
    #
    handler = KageLangChainCallback(agent_name="qa_agent")
    #
    # result = chain.invoke({"query": "what is photosynthesis?"},
    #                       config={"callbacks": [handler]})
    # print(result)

    # ---- LangGraph (same handler works) -----------------------------------
    #
    # from langgraph.graph import StateGraph
    # graph = StateGraph(...).compile()
    # graph.invoke({"input": "..."}, config={"callbacks": [handler]})

    # ---- What KAGE captures ----------------------------------------------
    #
    # For each LangChain run_id, one task_run is emitted:
    #   - kind="chain" for chains (incl. RunnableSequence)
    #   - kind="llm_call" for LLM / chat-model invocations (with model + tokens)
    #   - kind="tool" for tool calls (with tool_input / tool_output preview)
    #   - kind="retrieval" for retrievers (with doc_count)
    #
    # parent_run_id -> parent_span_id, so you can rebuild the full call tree:
    #
    # SELECT
    #   task_run_id, task_name, kind, parent_span_id, status, latency_ms,
    #   custom_fields.model, custom_fields.total_tokens
    # FROM json.`kage-logs/langchain/event_type=task_run/`
    # ORDER BY event_timestamp;
    print("Handler ready:", handler)


if __name__ == "__main__":
    run_with_handler()
