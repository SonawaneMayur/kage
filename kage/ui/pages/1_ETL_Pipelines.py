"""ETL Pipelines page - medallion flow, SLA, lineage."""
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

st.set_page_config(page_title="KAGE - ETL Pipelines", layout="wide")
st.title("ETL Pipelines")
st.caption("Medallion-aware view: landing -> bronze -> silver -> gold.")

base_path = get_base_path()
filt = sidebar_filters(base_path)

ds = load_events(base_path, "dataset_event", filt["platforms"], filt["since_days"])
job = load_events(base_path, "job_run", filt["platforms"], filt["since_days"])

if ds.is_empty():
    empty_state("No dataset_event records found.")
    st.stop()

# Keep only ETL events (i.e. rows that carry a medallion `layer`)
medallion = {"landing", "bronze", "silver", "gold"}
if "layer" in ds.columns:
    etl = ds.filter(pl.col("layer").is_in(list(medallion)))
else:
    etl = ds

if etl.is_empty():
    st.warning("No events with medallion layers in this window.")
    st.stop()

# --- Volume per layer ------------------------------------------------------

st.subheader("Volume by medallion layer")
vol = (etl
       .filter(pl.col("event_action") == "WRITE")
       .group_by("layer")
       .agg(pl.col("record_count").sum().alias("rows"))
       .sort("rows", descending=True))

if not vol.is_empty():
    layer_order = ["landing", "bronze", "silver", "gold"]
    fig = px.bar(
        vol.to_pandas(),
        x="layer", y="rows",
        color="layer",
        category_orders={"layer": layer_order},
        labels={"rows": "Rows written"},
        color_discrete_map={
            "landing": "#6B7280", "bronze": "#CD7F32",
            "silver": "#A0A0A0", "gold": "#D29922",
        },
    )
    fig.update_layout(height=320, showlegend=False,
                      margin=dict(l=20, r=20, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

# --- Pipeline SLA table ----------------------------------------------------

st.subheader("Pipeline SLA")

if not job.is_empty() and "pipeline_name" in job.columns:
    completed = job.filter(pl.col("status").is_in(["SUCCESS", "FAILED"]))
    if not completed.is_empty():
        sla = (completed
               .group_by("pipeline_name")
               .agg(
                   pl.len().alias("runs"),
                   (pl.col("status") == "SUCCESS").sum().alias("success_runs"),
                   (pl.col("status") == "FAILED").sum().alias("failed_runs"),
               )
               .with_columns(
                   (100.0 * pl.col("success_runs") / pl.col("runs"))
                   .round(1).alias("success_rate_pct"),
               )
               .sort("runs", descending=True))
        st.dataframe(sla.to_pandas(), use_container_width=True, hide_index=True)
    else:
        st.info("No completed jobs yet.")

# --- Lineage (top upstream relationships) ---------------------------------

st.subheader("Lineage - top upstream dependencies")

if "upstream_datasets" in etl.columns:
    lineage = (etl
               .filter(pl.col("event_action") == "WRITE")
               .filter(pl.col("upstream_datasets").list.len() > 0)
               .with_columns(pl.col("upstream_datasets").explode().alias("upstream"))
               .group_by(["dataset_name", "upstream"]).len()
               .rename({"len": "count"})
               .sort("count", descending=True)
               .head(50))
    if not lineage.is_empty():
        st.dataframe(lineage.to_pandas(), use_container_width=True,
                     hide_index=True)
    else:
        st.info("No `upstream_datasets` recorded in this window.")

# --- Raw events table -----------------------------------------------------

with st.expander("Raw dataset_event rows", expanded=False):
    cols = [c for c in ("ts", "platform", "pipeline_name", "layer",
                        "event_action", "dataset_name", "record_count",
                        "upstream_datasets") if c in etl.columns]
    if cols:
        st.dataframe(etl.select(cols).sort("ts", descending=True).head(500).to_pandas(),
                     use_container_width=True, hide_index=True)
