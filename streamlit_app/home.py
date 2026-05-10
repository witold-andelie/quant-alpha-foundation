"""Home page — clickable navigation cards. Loaded via st.navigation in app.py."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from common import (
    ENERGY_DB, EQUITY_DB,
    ENERGY_TABLES, EQUITY_TABLES,
    list_tables, pick,
)

st.title("Quant Alpha Foundation")
st.caption(
    "Second Foundation Energy · US Equities Demo · "
    "WorldQuant-style alpha research with full DataTalksClub Zoomcamp stack"
)

energy_ok = ENERGY_DB.exists()
equity_ok = EQUITY_DB.exists()

if not energy_ok and not equity_ok:
    st.warning(
        "No data found. Run `quant-alpha energy-run` or `quant-alpha run --offline` "
        "from the project root to generate the warehouse data."
    )
    st.stop()

# ── Track selector (persists across pages) ───────────────────────────────────
default_track = st.session_state.get("track", "Second Foundation Energy")
track = st.radio(
    "Active research track",
    ["Second Foundation Energy", "US Equities Demo"],
    index=0 if default_track == "Second Foundation Energy" else 1,
    horizontal=True,
)
st.session_state["track"] = track
is_energy = track == "Second Foundation Energy"
db = ENERGY_DB if is_energy else EQUITY_DB
tables_map = ENERGY_TABLES if is_energy else EQUITY_TABLES

if not db.exists():
    st.info(f"Run the {'energy' if is_energy else 'equity'} pipeline to generate data.")
    st.stop()

# ── Top metrics summary ──────────────────────────────────────────────────────
metrics = pick(db, *tables_map["metrics"])
diagnostics = pick(db, *tables_map["diagnostics"])
registry = pick(db, *tables_map["registry"])

def _fmt(df: pd.DataFrame, col: str, fmt: str) -> str:
    if df.empty or col not in df.columns:
        return "—"
    try:
        return fmt.format(float(df[col].iloc[0]))
    except Exception:
        return "—"

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Total Return", _fmt(metrics, "total_return", "{:.1%}"))
with c2:
    st.metric("Ann. Sharpe", _fmt(metrics, "sharpe", "{:.2f}"))
with c3:
    st.metric("Sortino", _fmt(metrics, "sortino", "{:.2f}"))
with c4:
    st.metric("Max Drawdown", _fmt(metrics, "max_drawdown", "{:.1%}"))
with c5:
    n_alphas = len(registry) if not registry.empty else "—"
    st.metric("Alpha factors", n_alphas)

st.divider()
st.subheader("Research Modules")
st.caption("Click any card to dive into that module")

# ── Clickable feature cards ──────────────────────────────────────────────────
CARDS = [
    {"icon": "📈", "title": "Performance",         "page": "pages/1_Performance.py",
     "tagline": "P&L curve · drawdown · rolling Sharpe · attribution"},
    {"icon": "🔬", "title": "Factor Research",     "page": "pages/2_Factor_Research.py",
     "tagline": "4-gate scorecard · IS/OOS scatter · correlation heatmap"},
    {"icon": "📉", "title": "Alpha Decay",         "page": "pages/3_Alpha_Decay.py",
     "tagline": "IC decay · walk-forward stability · turnover vs Sharpe"},
    {"icon": "⚡", "title": "Market Data",         "page": "pages/4_Market_Data.py",
     "tagline": "Spot · residual load · imbalance · Spark features"},
    {"icon": "🔴", "title": "Live Streaming",      "page": "pages/5_Live_Streaming.py",
     "tagline": "Redpanda buffer · RisingWave simulator · scarcity alerts"},
    {"icon": "🔧", "title": "Data Pipeline",       "page": "pages/6_Data_Pipeline.py",
     "tagline": "Bruin lineage · table inventory · quality · null rates"},
    {"icon": "📊", "title": "Cross-Track Overview", "page": "pages/7_Overview.py",
     "tagline": "Energy + Equity side-by-side · 11-module health matrix"},
]

def _render_card(card: dict):
    with st.container(border=True):
        st.markdown(
            f"""
            <div style="min-height: 100px;">
              <h4 style="margin: 0 0 0.5rem 0; font-size: 1.1rem;">
                {card['icon']} &nbsp;{card['title']}
              </h4>
              <p style="color: rgba(140,140,150,0.95); font-size: 0.85rem;
                        margin: 0; line-height: 1.45;">
                {card['tagline']}
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button(
            "Open  →",
            key=f"btn_{card['title']}",
            type="primary",
            use_container_width=True,
        ):
            st.switch_page(card["page"])

# Row 1: 4 cards
row1 = st.columns(4)
for col, card in zip(row1, CARDS[:4]):
    with col:
        _render_card(card)

# Row 2: 3 cards (last column intentionally empty for visual balance)
row2 = st.columns(4)
for col, card in zip(row2[:3], CARDS[4:]):
    with col:
        _render_card(card)

# ── Quick Actions (separate horizontal section) ──────────────────────────────
st.divider()
st.subheader("Quick Actions")

qa1, qa2, qa3 = st.columns(3)
with qa1:
    if st.button("🔄 Refresh all caches", use_container_width=True, key="qa_refresh"):
        st.cache_data.clear()
        st.rerun()
with qa2:
    if st.button("🌱 Seed live signals (48 h)", use_container_width=True, key="qa_seed"):
        try:
            from quant_alpha.streaming.demo_signals import seed_demo_signals
            n = seed_demo_signals(ENERGY_DB, n_hours=48)
            st.success(f"Wrote {n} synthetic rows to live_energy_signals")
        except Exception as exc:
            st.error(f"Failed: {exc}")
with qa3:
    if st.button("📋 Show all DB tables", use_container_width=True, key="qa_tables"):
        st.session_state["show_tables"] = not st.session_state.get("show_tables", False)

if st.session_state.get("show_tables", False):
    st.divider()
    st.subheader("All DuckDB Tables")
    col_e, col_q = st.columns(2)
    with col_e:
        e_tables = list_tables(ENERGY_DB)
        st.markdown(f"**Energy** ({len(e_tables)})")
        st.code("\n".join(e_tables) if e_tables else "(none)", language=None)
    with col_q:
        q_tables = list_tables(EQUITY_DB)
        st.markdown(f"**Equity** ({len(q_tables)})")
        st.code("\n".join(q_tables) if q_tables else "(none)", language=None)

st.divider()
st.caption(
    f"Active track: **{track}** · "
    f"Backend: `{'BigQuery' if 'bigquery' in str(db) else 'DuckDB'}` · "
    f"Path: `{db}`"
)
