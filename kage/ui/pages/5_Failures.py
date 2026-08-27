"""Failures page - errors across every adapter."""
from __future__ import annotations

import polars as pl
import plotly.express as px
import streamlit as st

from kage.ui.data import (
    get_base_path,
    load_all,
    sidebar_filters,
    empty_state,
    unnest_custom_fields,
)

st.set_page_config(page_title="KAGE - Failures", layout="wide")
st.title("Failures")
st.caption("Unified error explorer across pipelines, agents, dbt, Airflow.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

dfs = load_all(base_path, filt["platforms"], filt["since_days"])

# Pull all FAILED rows from all three event types
failed_frames = []
for ev_type, df in dfs.items():
    if df.is_empty() or "status" not in df.columns:
        continue
    fails = df.filter(pl.col("status") == "FAILED")
    if fails.is_empty():
        continue
    fails = unnest_custom_fields(fails)
    keep = [c for c in ("ts", "platform", "pipeline_name", "task_name",
                        "job_name", "layer", "kind", "error_type",
                        "error_message", "stack_trace")
            if c in fails.columns]
    f = fails.select(keep).with_columns(pl.lit(ev_type).alias("event_type"))
    failed_frames.append(f)

if not failed_frames:
    empty_state("No FAILED events in the selected window. Good news.")
    st.stop()

failures = pl.concat(failed_frames, how="diagonal_relaxed").sort("ts", descending=True)

# --- KPIs ------------------------------------------------------------------

c1, c2, c3 = st.columns(3)
c1.metric("Total failures", failures.height)
if "platform" in failures.columns:
    c2.metric("Affected platforms",
              failures.select("platform").n_unique() if "platform" in failures.columns else 0)
if "error_type" in failures.columns:
    c3.metric("Distinct error types",
              failures.filter(pl.col("error_type").is_not_null())
                      .select("error_type").n_unique())

st.divider()

# --- Top error types ------------------------------------------------------

if "error_type" in failures.columns:
    st.subheader("Top error types")
    top = (failures
           .filter(pl.col("error_type").is_not_null())
           .group_by("error_type").len()
           .rename({"len": "count"})
           .sort("count", descending=True)
           .head(20))
    if not top.is_empty():
        fig = px.bar(top.to_pandas(), x="count", y="error_type",
                     orientation="h",
                     labels={"count": "Occurrences", "error_type": "Error"})
        fig.update_layout(height=380, margin=dict(l=20, r=20, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

# --- Failures over time ---------------------------------------------------

if "ts" in failures.columns:
    st.subheader("Failures over time")
    binned = (failures
              .with_columns(pl.col("ts").dt.truncate("1h").alias("hour"))
              .group_by(["hour", "platform"]).len()
              .rename({"len": "count"})
              .sort("hour"))
    fig = px.line(binned.to_pandas(), x="hour", y="count", color="platform",
                  labels={"hour": "Time", "count": "Failures"})
    fig.update_layout(height=280, margin=dict(l=20, r=20, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

# --- Drill-in -------------------------------------------------------------

st.subheader("Failure detail")
if "error_type" in failures.columns:
    types = failures.select("error_type").drop_nulls().unique().to_series().to_list()
    chosen = st.selectbox("Filter by error_type",
                          options=["(all)"] + sorted(types))
    if chosen != "(all)":
        sub = failures.filter(pl.col("error_type") == chosen)
    else:
        sub = failures
else:
    sub = failures

st.dataframe(sub.head(500).to_pandas(),
             use_container_width=True, hide_index=True)

if "stack_trace" in sub.columns and sub.height > 0:
    with st.expander("Stack trace - first failure"):
        first = sub.row(0, named=True)
        st.code(first.get("stack_trace", "(none)"), language="text")
