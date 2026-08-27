"""dbt page - model run history, status, schema drift."""
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

st.set_page_config(page_title="KAGE - dbt", layout="wide")
st.title("dbt Runs")
st.caption("Ingested from `target/run_results.json` + `target/manifest.json`.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

job = load_events(base_path, "job_run", filt["platforms"], filt["since_days"])
task = load_events(base_path, "task_run", filt["platforms"], filt["since_days"])
ds = load_events(base_path, "dataset_event", filt["platforms"], filt["since_days"])

if "platform" in job.columns:
    job = job.filter(pl.col("platform") == "dbt")
if "platform" in task.columns:
    task = task.filter(pl.col("platform") == "dbt")
if "platform" in ds.columns:
    ds = ds.filter(pl.col("platform") == "dbt")

if job.is_empty() and task.is_empty():
    empty_state("No dbt platform events found. Run "
                "`python -m kage.integrations.dbt target/` after `dbt run`.")
    st.stop()

# --- KPIs ------------------------------------------------------------------

completed_jobs = (job.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))
                  if not job.is_empty() and "status" in job.columns
                  else pl.DataFrame())
completed_tasks = (task.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))
                   if not task.is_empty() and "status" in task.columns
                   else pl.DataFrame())

c1, c2, c3, c4 = st.columns(4)
c1.metric("dbt invocations", completed_jobs.height)
c2.metric("Models executed", completed_tasks.height)
if not completed_tasks.is_empty():
    success = completed_tasks.filter(pl.col("status") == "SUCCESS").height
    rate = 100.0 * success / completed_tasks.height
    c3.metric("Model success rate", f"{rate:.1f}%")
c4.metric("Materialised datasets",
          ds.filter(pl.col("event_action") == "WRITE").height
          if not ds.is_empty() and "event_action" in ds.columns else 0)

st.divider()

# --- Status by layer -------------------------------------------------------

if not completed_tasks.is_empty() and "layer" in completed_tasks.columns:
    st.subheader("Model outcomes by medallion layer")
    breakdown = (completed_tasks
                 .group_by(["layer", "status"]).len()
                 .rename({"len": "count"}))
    fig = px.bar(
        breakdown.to_pandas(),
        x="layer", y="count", color="status", barmode="group",
        color_discrete_map={"SUCCESS": "#059669", "FAILED": "#DC2626"},
        category_orders={"layer": ["landing", "bronze", "silver", "gold"]},
    )
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

# --- Model run history ----------------------------------------------------

st.subheader("Recent model runs")

if not completed_tasks.is_empty():
    flat = unnest_custom_fields(completed_tasks)
    cols = [c for c in ("ts", "task_name", "layer", "status",
                        "dbt_unique_id", "dbt_resource_type",
                        "execution_time_sec", "error_message")
            if c in flat.columns]
    st.dataframe(
        flat.select(cols).sort("ts", descending=True).head(500).to_pandas(),
        use_container_width=True, hide_index=True,
    )

# --- Lineage --------------------------------------------------------------

if not ds.is_empty() and "upstream_datasets" in ds.columns:
    st.subheader("dbt lineage")
    lineage = (ds
               .filter(pl.col("event_action") == "WRITE")
               .filter(pl.col("upstream_datasets").list.len() > 0)
               .with_columns(pl.col("upstream_datasets").explode().alias("upstream"))
               .group_by(["dataset_name", "upstream"]).len()
               .rename({"len": "count"})
               .sort("count", descending=True)
               .head(50))
    if not lineage.is_empty():
        st.dataframe(lineage.to_pandas(),
                     use_container_width=True, hide_index=True)
