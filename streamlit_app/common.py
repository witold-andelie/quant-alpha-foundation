"""Shared data-loading utilities for all dashboard pages."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
ENERGY_DB = ROOT / "data/warehouse/second_foundation.duckdb"
EQUITY_DB = ROOT / "data/warehouse/quant_alpha.duckdb"
DATA_BACKEND = os.getenv("STREAMLIT_DATA_BACKEND", "duckdb").lower()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "second_foundation_quant")


@st.cache_data(ttl=300)
def load_table(db_path: Path, table: str) -> pd.DataFrame:
    if DATA_BACKEND == "bigquery":
        return _load_bq(table)
    if not db_path.exists():
        return pd.DataFrame()
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
            if table not in tables:
                return pd.DataFrame()
            return con.execute(f"SELECT * FROM {table}").df()
    except Exception:
        return pd.DataFrame()


def _load_bq(table: str) -> pd.DataFrame:
    if not GCP_PROJECT_ID:
        return pd.DataFrame()
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        return client.query(f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table}`").to_dataframe()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def list_tables(db_path: Path) -> list[str]:
    if not db_path.exists():
        return []
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            return sorted(r[0] for r in con.execute("SHOW TABLES").fetchall())
    except Exception:
        return []


@st.cache_data(ttl=300)
def table_row_count(db_path: Path, table: str) -> int:
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0


def pick(db_path: Path, *candidates: str) -> pd.DataFrame:
    """Return first non-empty table from candidates list."""
    for t in candidates:
        df = load_table(db_path, t)
        if not df.empty:
            return df
    return pd.DataFrame()


ENERGY_TABLES = {
    "backtest": ("fct_energy_backtest_daily", "energy_backtest_daily"),
    "metrics": ("energy_backtest_metrics",),
    "diagnostics": ("fct_energy_alpha_diagnostics", "energy_alpha_diagnostics"),
    "decay": ("fct_energy_alpha_decay", "energy_alpha_decay"),
    "value_added": ("energy_alpha_value_added",),
    "registry": ("energy_alpha_registry",),
    "features": ("power_market_features",),
    "raw": ("power_market_raw",),
    "correlation": ("energy_alpha_correlation",),
    "turnover": ("energy_alpha_turnover",),
    "walk_forward": ("energy_walk_forward_ic",),
    "live": ("live_energy_signals",),
    "quality": ("energy_quality_report",),
}

EQUITY_TABLES = {
    "backtest": ("backtest_daily",),
    "metrics": ("backtest_metrics",),
    "diagnostics": ("alpha_diagnostics",),
    "decay": ("fct_alpha_decay", "alpha_decay"),
    "value_added": ("alpha_value_added",),
    "registry": ("alpha_registry",),
    "features": ("factor_panel",),
    "correlation": ("alpha_correlation",),
    "turnover": ("alpha_turnover",),
    "walk_forward": ("alpha_walk_forward",),
}


def render_track_selector(*, key: str = "track_selector", show_back_home: bool = True):
    """Render the track selector at top of every page; persists choice in session_state."""
    default_track = st.session_state.get("track", "Second Foundation Energy")
    cols = st.columns([3, 1, 1] if show_back_home else [1])

    with cols[0]:
        track = st.radio(
            "Track",
            ["Second Foundation Energy", "US Equities Demo"],
            index=0 if default_track == "Second Foundation Energy" else 1,
            horizontal=True,
            key=key,
            label_visibility="collapsed",
        )

    if show_back_home:
        with cols[1]:
            if st.button("🔄 Refresh", use_container_width=True, help="Clear cache and reload"):
                st.cache_data.clear()
                st.rerun()
        with cols[2]:
            if st.button("🏠 Home", use_container_width=True, key=f"{key}_home"):
                st.switch_page("home.py")

    st.session_state["track"] = track
    is_energy = track == "Second Foundation Energy"
    return track, is_energy, (ENERGY_DB if is_energy else EQUITY_DB), (ENERGY_TABLES if is_energy else EQUITY_TABLES)
