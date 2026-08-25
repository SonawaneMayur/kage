"""
KAGE UI - polars-backed event loader with caching.

All Streamlit pages import from here so the loader code stays in one place.
Uses polars.scan_ndjson for lazy reads + glob (much faster than pandas at
modest scale, and the schema unification "ignore_errors" handles JSONL
files written with slightly different field sets across adapters).
"""
from __future__ import annotations

import glob
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import polars as pl
import streamlit as st


def get_base_path() -> str:
    """Resolve the KAGE log base path from env var or default."""
    return os.environ.get("KAGE_LOG_PATH", "./kage-logs")


def list_platforms(base_path: str) -> List[str]:
    """Discover the platforms present (the directory level right after base)."""
    p = Path(base_path)
    if not p.exists():
        return []
    return sorted([d.name for d in p.iterdir() if d.is_dir()])


def _glob_files(base_path: str, event_type: str,
                platforms: Optional[List[str]] = None) -> List[str]:
    platforms = platforms or ["*"]
    files: List[str] = []
    for plat in platforms:
        pattern = f"{base_path}/{plat}/event_type={event_type}/dt=*/part-*.jsonl"
        files.extend(glob.glob(pattern))
    return sorted(files)


@st.cache_data(ttl=15, show_spinner=False)
def load_events(
    base_path: str,
    event_type: str,
    platforms: Optional[List[str]] = None,
    since_days: int = 30,
) -> pl.DataFrame:
    """Load events of one event_type into a polars DataFrame.

    Returns an empty DataFrame if no files match.
    """
    files = _glob_files(base_path, event_type, platforms)
    if not files:
        return pl.DataFrame()
    # polars.read_ndjson per file, then concat; tolerate per-file schema drift.
    frames = []
    for f in files:
        try:
            frames.append(pl.read_ndjson(f, ignore_errors=True))
        except Exception:
            continue
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="diagonal_relaxed")
    # Parse event_timestamp once, filter by recency
    if "event_timestamp" in df.columns:
        df = df.with_columns(
            pl.col("event_timestamp")
              .str.to_datetime(strict=False)
              .alias("ts"),
        )
        if since_days > 0:
            cutoff = datetime.utcnow() - timedelta(days=since_days)
            df = df.filter(pl.col("ts") >= cutoff)
    return df


def load_all(base_path: str,
             platforms: Optional[List[str]] = None,
             since_days: int = 30) -> dict:
    """Load all three event types at once. Returns dict[event_type] -> DF."""
    return {
        "job_run": load_events(base_path, "job_run", platforms, since_days),
        "task_run": load_events(base_path, "task_run", platforms, since_days),
        "dataset_event": load_events(base_path, "dataset_event", platforms, since_days),
    }


def sidebar_filters(base_path: str) -> dict:
    """Render shared filter controls in the sidebar and return their values."""
    st.sidebar.header("Filters")

    platforms = list_platforms(base_path)
    chosen_platforms = st.sidebar.multiselect(
        "Platforms", options=platforms, default=platforms,
        help="Directories directly under base_path",
    )
    if not chosen_platforms:
        chosen_platforms = None  # treat empty as "all"

    since_days = st.sidebar.slider("Days back", min_value=1, max_value=90,
                                   value=30, step=1)
    st.sidebar.caption(f"Base path: `{base_path}`")
    st.sidebar.caption(f"Cache TTL: 15s (use refresh button to force reload)")
    if st.sidebar.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    return {"platforms": chosen_platforms, "since_days": since_days}


def empty_state(message: str) -> None:
    """Render an empty-state callout."""
    st.info(f"{message}\n\nBase path: `{get_base_path()}`")


def unnest_custom_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Bring `custom_fields.<x>` into top-level columns when present."""
    if "custom_fields" not in df.columns:
        return df
    try:
        return df.unnest("custom_fields")
    except Exception:
        # Already flat or not a struct - leave alone
        return df
