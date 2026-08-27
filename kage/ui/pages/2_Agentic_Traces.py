"""Agentic Traces page - span tree, tokens, cost, latency."""
from __future__ import annotations

import polars as pl
import plotly.express as px
import streamlit as st

from kage.ui.data import (
    get_base_path,
    load_events,
    sidebar_filters,
    empty_state,
    unnest_custom_fields,
)

st.set_page_config(page_title="KAGE - Agentic", layout="wide")
st.title("Agentic Traces")
st.caption("Spans from @agent / @step / @tool / @llm_call and LangChain.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

task = load_events(base_path, "task_run", filt["platforms"], filt["since_days"])

if task.is_empty():
    empty_state("No task_run events found.")
    st.stop()

if "kind" not in task.columns:
    st.warning("No `kind` column. Agentic events need v1.3+ — older spans "
               "(ETL tasks) are excluded from this page.")
    st.stop()

# Keep only agentic kinds
agentic_kinds = {"agent", "step", "tool", "llm_call", "chain", "retrieval"}
agentic = task.filter(pl.col("kind").is_in(list(agentic_kinds)))

if agentic.is_empty():
    st.warning("No agentic spans in this window.")
    st.stop()

agentic = unnest_custom_fields(agentic)

# --- KPIs ------------------------------------------------------------------

end_events = agentic.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))

c1, c2, c3, c4 = st.columns(4)
c1.metric("Agent runs",
          agentic.filter(pl.col("kind") == "agent").filter(
              pl.col("status") == "RUNNING").height
          if "status" in agentic.columns else 0)
c2.metric("Tool calls",
          agentic.filter(pl.col("kind") == "tool").filter(
              pl.col("status") == "RUNNING").height
          if "status" in agentic.columns else 0)
c3.metric("LLM calls",
          agentic.filter(pl.col("kind") == "llm_call").filter(
              pl.col("status") == "RUNNING").height
          if "status" in agentic.columns else 0)
if "total_tokens" in agentic.columns:
    tot = agentic.select(pl.col("total_tokens").cast(pl.Float64, strict=False)
                         .sum()).item() or 0
    c4.metric("Total tokens", f"{int(tot):,}")
elif "prompt_tokens" in agentic.columns:
    tot = agentic.select(pl.col("prompt_tokens").cast(pl.Float64, strict=False)
                         .sum()).item() or 0
    c4.metric("Prompt tokens", f"{int(tot):,}")

st.divider()

# --- Latency histogram -----------------------------------------------------

if "latency_ms" in agentic.columns and not end_events.is_empty():
    st.subheader("Latency distribution (ms)")
    lat = end_events.filter(pl.col("latency_ms").is_not_null())
    if not lat.is_empty():
        fig = px.histogram(
            lat.to_pandas(), x="latency_ms", color="kind", nbins=40,
            labels={"latency_ms": "Latency (ms)"},
        )
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

# --- Token usage over time -------------------------------------------------

if "total_tokens" in agentic.columns:
    st.subheader("Token usage over time")
    tok = (agentic
           .filter(pl.col("status") == "SUCCESS")
           .filter(pl.col("total_tokens").is_not_null())
           .with_columns(
               pl.col("total_tokens").cast(pl.Float64, strict=False).alias("tt")
           ))
    if not tok.is_empty():
        binned = (tok
                  .with_columns(pl.col("ts").dt.truncate("1h").alias("hour"))
                  .group_by("hour")
                  .agg(pl.col("tt").sum().alias("tokens"))
                  .sort("hour"))
        fig = px.area(binned.to_pandas(), x="hour", y="tokens",
                      labels={"hour": "Time", "tokens": "Tokens"})
        fig.update_layout(height=280, margin=dict(l=20, r=20, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

# --- Span tree explorer ---------------------------------------------------

st.subheader("Span tree explorer")
st.caption("Pick a job_run_id to see the full call tree.")

if "job_run_id" in agentic.columns:
    job_ids = (agentic.select("job_run_id").unique()
                      .drop_nulls().to_series().to_list())
    if job_ids:
        chosen = st.selectbox("job_run_id", options=job_ids[:200])
        sub = agentic.filter(pl.col("job_run_id") == chosen)
        # Only show one row per span (the RUNNING start, fall back to SUCCESS)
        starts = sub.filter(pl.col("status") == "RUNNING") \
                    if "status" in sub.columns else sub
        if starts.is_empty():
            starts = sub

        # Build a depth-first ordering using parent_span_id
        spans = starts.to_dicts()
        children: dict = {}
        roots = []
        for s in spans:
            parent = s.get("parent_span_id")
            if parent is None:
                roots.append(s)
            else:
                children.setdefault(parent, []).append(s)

        lines = []
        def walk(node, depth):
            indent = "    " * depth
            label = f"{node.get('task_name','?')}  [{node.get('kind','?')}]"
            extras = []
            if node.get("latency_ms") is not None:
                extras.append(f"{node['latency_ms']}ms")
            if node.get("model"):
                extras.append(str(node["model"]))
            if extras:
                label += "  -- " + ", ".join(extras)
            lines.append(f"{indent}- {label}")
            for c in children.get(node.get("task_run_id"), []):
                walk(c, depth + 1)
        for r in roots:
            walk(r, 0)
        st.code("\n".join(lines) or "(empty)", language="text")

# --- Raw table ------------------------------------------------------------

with st.expander("Raw agentic span rows", expanded=False):
    cols = [c for c in ("ts", "kind", "task_name", "status", "parent_span_id",
                        "latency_ms", "model", "prompt_tokens",
                        "completion_tokens", "total_tokens", "cost_usd",
                        "error_type", "error_message")
            if c in agentic.columns]
    st.dataframe(agentic.select(cols).sort("ts", descending=True)
                 .head(500).to_pandas(),
                 use_container_width=True, hide_index=True)
