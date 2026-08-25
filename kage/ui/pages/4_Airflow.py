"""Airflow page - DAG runs + task instance durations."""
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

st.set_page_config(page_title="KAGE - Airflow", layout="wide")
st.title("Airflow DAGs & Tasks")
st.caption("Emitted from `kage_task_callbacks` + `kage_dag_callbacks`.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

job = load_events(base_path, "job_run", filt["platforms"], filt["since_days"])
task = load_events(base_path, "task_run", filt["platforms"], filt["since_days"])

if "platform" in job.columns:
    job = job.filter(pl.col("platform") == "airflow")
if "platform" in task.columns:
    task = task.filter(pl.col("platform") == "airflow")

if job.is_empty() and task.is_empty():
    empty_state("No Airflow platform events found. Wire `kage_task_callbacks` "
                "into your DAG's `default_args`.")
    st.stop()

# --- KPIs ------------------------------------------------------------------

completed = (task.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))
             if not task.is_empty() and "status" in task.columns
             else pl.DataFrame())

c1, c2, c3 = st.columns(3)
c1.metric("DAG runs", job.height)
c2.metric("Task instances", completed.height)
if not completed.is_empty():
    rate = 100.0 * completed.filter(pl.col("status") == "SUCCESS").height \
           / max(completed.height, 1)
    c3.metric("Task success rate", f"{rate:.1f}%")

st.divider()

# --- Task duration heatmap ------------------------------------------------

st.subheader("Task durations")

if not completed.is_empty():
    flat = unnest_custom_fields(completed)
    if "duration_sec" in flat.columns:
        dur_col = "duration_sec"
    elif "latency_ms" in flat.columns:
        flat = flat.with_columns((pl.col("latency_ms") / 1000.0).alias("duration_sec"))
        dur_col = "duration_sec"
    else:
        dur_col = None

    if dur_col is not None and "task_name" in flat.columns:
        durations = (flat
                     .filter(pl.col(dur_col).is_not_null())
                     .group_by("task_name")
                     .agg(
                         pl.col(dur_col).mean().alias("avg_sec"),
                         pl.col(dur_col).max().alias("max_sec"),
                         pl.len().alias("runs"),
                     )
                     .sort("avg_sec", descending=True)
                     .head(50))
        if not durations.is_empty():
            fig = px.bar(
                durations.to_pandas(),
                x="avg_sec", y="task_name", orientation="h",
                labels={"avg_sec": "Avg duration (s)", "task_name": "Task"},
                hover_data=["max_sec", "runs"],
            )
            fig.update_layout(height=400, margin=dict(l=20, r=20, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

# --- DAG run timeline -----------------------------------------------------

st.subheader("Recent DAG runs")

if not job.is_empty():
    flat = unnest_custom_fields(job).filter(
        pl.col("status").is_in(["SUCCESS", "FAILED"])
        if "status" in job.columns else pl.lit(True)
    )
    cols = [c for c in ("ts", "job_run_id", "airflow_dag_id", "status",
                        "error_type", "error_message")
            if c in flat.columns]
    if cols:
        st.dataframe(flat.select(cols).sort("ts", descending=True).head(200).to_pandas(),
                     use_container_width=True, hide_index=True)

# --- Task table -----------------------------------------------------------

with st.expander("Task instance details", expanded=False):
    if not completed.is_empty():
        flat = unnest_custom_fields(completed)
        cols = [c for c in ("ts", "task_name", "status",
                            "airflow_dag_id", "airflow_run_id",
                            "duration_sec", "airflow_try_number",
                            "error_type", "error_message")
                if c in flat.columns]
        st.dataframe(flat.select(cols).sort("ts", descending=True)
                     .head(500).to_pandas(),
                     use_container_width=True, hide_index=True)
