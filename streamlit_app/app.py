"""Quant Alpha Foundation — entry point with explicit st.navigation registration."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

st.set_page_config(
    page_title="Quant Alpha Foundation",
    page_icon="📐",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── First-run data bootstrap (Streamlit Cloud, etc.) ─────────────────────────
def _bootstrap_demo_data() -> None:
    """Generate synthetic DuckDB warehouses on first launch when data dir is empty."""
    if st.session_state.get("_data_bootstrapped"):
        return

    project_root = Path(__file__).resolve().parents[1]
    energy_db = project_root / "data/warehouse/second_foundation.duckdb"
    equity_db = project_root / "data/warehouse/quant_alpha.duckdb"

    if energy_db.exists() and equity_db.exists():
        st.session_state["_data_bootstrapped"] = True
        return

    sys.path.insert(0, str(project_root / "src"))
    with st.spinner("First-run setup: generating synthetic demo data (~30 seconds)…"):
        try:
            from quant_alpha.pipeline_energy import run_energy_pipeline
            from quant_alpha.pipeline import run_pipeline

            if not energy_db.exists():
                run_energy_pipeline(
                    project_root / "configs/second_foundation_project.yaml",
                    project_root,
                    source_override="synthetic",
                )
            if not equity_db.exists():
                run_pipeline(
                    project_root / "configs/project.yaml",
                    project_root,
                    offline=True,
                )
            st.session_state["_data_bootstrapped"] = True
            st.success("Demo data ready. Reloading…")
            st.rerun()
        except Exception as exc:
            st.warning(f"Demo data bootstrap skipped: {exc}")
            st.session_state["_data_bootstrapped"] = True


_bootstrap_demo_data()

home = st.Page("home.py", title="Home", icon="🏠", default=True)
performance = st.Page("pages/1_Performance.py", title="Performance", icon="📈")
factor_research = st.Page("pages/2_Factor_Research.py", title="Factor Research", icon="🔬")
alpha_decay = st.Page("pages/3_Alpha_Decay.py", title="Alpha Decay", icon="📉")
market_data = st.Page("pages/4_Market_Data.py", title="Market Data", icon="⚡")
live_streaming = st.Page("pages/5_Live_Streaming.py", title="Live Streaming", icon="🔴")
data_pipeline = st.Page("pages/6_Data_Pipeline.py", title="Data Pipeline", icon="🔧")
overview = st.Page("pages/7_Overview.py", title="Cross-Track Overview", icon="📊")

pg = st.navigation({
    " ": [home],
    "Research": [performance, factor_research, alpha_decay, market_data],
    "Operations": [live_streaming, data_pipeline, overview],
})
pg.run()
