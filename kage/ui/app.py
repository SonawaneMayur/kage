"""
KAGE UI - Overview (home page).

Launch:
    kage ui                          # uses env KAGE_LOG_PATH or ./kage-logs
    KAGE_LOG_PATH=/path streamlit run kage/ui/app.py
"""
from __future__ import annotations

import polars as pl
import plotly.express as px
import streamlit as st

from kage.ui.data import (
    get_base_path,
    load_all,
    list_platforms,
    sidebar_filters,
    empty_state,
)

st.set_page_config(page_title="KAGE Observability", layout="wide",
                   page_icon=None, initial_sidebar_state="expanded")

st.title("KAGE Observability")
st.caption("Universal events stream. Pipelines + agents, one schema, one storage.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

dfs = load_all(base_path, filt["platforms"], filt["since_days"])
total_events = sum(len(df) for df in dfs.values())

if total_events == 0:
    empty_state("No KAGE events found. Run a pipeline or set KAGE_LOG_PATH "
                "to a directory containing JSONL events.")
    st.stop()

# --- Top KPIs --------------------------------------------------------------

job = dfs["job_run"]
task = dfs["task_run"]
ds = dfs["dataset_event"]

c1, c2, c3, c4 = st.columns(4)

def _success_rate(df: pl.DataFrame) -> float:
    if df.is_empty() or "status" not in df.columns:
        return 0.0
    completed = df.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))
    if completed.is_empty():
        return 0.0
    success = completed.filter(pl.col("status") == "SUCCESS").height
    return 100.0 * success / completed.height

c1.metric("Job runs", job.filter(pl.col("status") == "RUNNING").height
          if not job.is_empty() and "status" in job.columns else 0)
c2.metric("Task spans", task.height)
c3.metric("Dataset events", ds.height)
c4.metric("Job success rate", f"{_success_rate(job):.1f}%")

st.divider()

# --- Platform breakdown ----------------------------------------------------

st.subheader("Events by platform")

rows = []
for ev_type, df in dfs.items():
    if df.is_empty() or "platform" not in df.columns:
        continue
    by_platform = df.group_by("platform").len().rename({"len": "count"})
    for r in by_platform.iter_rows(named=True):
        rows.append({"platform": r["platform"], "event_type": ev_type,
                     "count": r["count"]})

if rows:
    breakdown = pl.DataFrame(rows)
    fig = px.bar(
        breakdown.to_pandas(),
        x="platform", y="count", color="event_type",
        barmode="stack",
        labels={"count": "Events"},
        color_discrete_map={
            "job_run": "#2563EB",
            "task_run": "#059669",
            "dataset_event": "#D29922",
        },
    )
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=10),
                      legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No `platform` field on events yet.")

st.divider()

# --- Recent activity timeline ---------------------------------------------

st.subheader("Recent activity")

frames = []
for ev_type, df in dfs.items():
    if df.is_empty() or "ts" not in df.columns:
        continue
    f = df.select(["ts", "platform"]).with_columns(
        pl.lit(ev_type).alias("event_type"),
    )
    frames.append(f)

if frames:
    activity = pl.concat(frames, how="diagonal_relaxed").sort("ts")
    # Bin into 1-hour buckets
    binned = (activity
              .with_columns(pl.col("ts").dt.truncate("1h").alias("hour"))
              .group_by(["hour", "event_type"]).len()
              .rename({"len": "count"}))
    fig = px.line(
        binned.to_pandas().sort_values("hour"),
        x="hour", y="count", color="event_type",
        labels={"hour": "Time (hourly buckets)", "count": "Events"},
    )
    fig.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=10),
                      legend=dict(orientation="h", y=1.05))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Drill-down hint -------------------------------------------------------

cols = st.columns(5)
pages = [
    ("ETL Pipelines", "Medallion flow + SLA + lineage"),
    ("Agentic Traces", "Span tree, tokens, cost, latency"),
    ("dbt", "Model history + status"),
    ("Airflow", "DAG runs + task durations"),
    ("Failures", "Errors across all platforms"),
]
for col, (name, desc) in zip(cols, pages):
    with col:
        st.markdown(f"**{name}**")
        st.caption(desc)
