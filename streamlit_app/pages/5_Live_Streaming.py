"""Page 6 — Live Streaming: RisingWave real-time alpha scores, scarcity alerts, signal replay."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ENERGY_DB, ENERGY_TABLES, load_table, pick

st.title("🔴 Live Streaming — Redpanda · RisingWave")

# Streaming is energy-only; show home button
col_l, col_r = st.columns([5, 1])
with col_l:
    st.caption("Live signals come from the energy track only.")
with col_r:
    if st.button("🏠 Home", use_container_width=True, key="live_home"):
        st.switch_page("home.py")

db = ENERGY_DB
tm = ENERGY_TABLES

# ── Control bar ───────────────────────────────────────────────────────────────
col_btn, col_rw, col_info = st.columns([1, 1, 3])
with col_btn:
    if st.button("Seed 48 h demo signals", type="primary"):
        from quant_alpha.streaming.demo_signals import seed_demo_signals
        n = seed_demo_signals(db, n_hours=48)
        st.success(f"Wrote {n} rows to live_energy_signals")
        st.cache_data.clear()
        st.rerun()
with col_rw:
    run_sim = st.button("Run RisingWave simulator", help="Compute streaming alpha scores offline via DuckDB")
with col_info:
    st.caption(
        "Production: `docker compose up -d redpanda risingwave` → signals flow automatically. "
        "Demo: seed synthetic data then use the simulator for offline alpha scoring."
    )

# ── Live signal buffer ────────────────────────────────────────────────────────
live = load_table(db, "live_energy_signals")

if not live.empty:
    live["timestamp"] = pd.to_datetime(live["timestamp"])

    st.subheader("Signal Buffer Status")
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric("Messages in buffer", f"{len(live):,}")
    with m2: st.metric("Markets", live["market"].nunique() if "market" in live.columns else "—")
    with m3:
        latest = live["timestamp"].max()
        st.metric("Latest signal", str(latest)[:16] if pd.notna(latest) else "—")
    with m4:
        earliest = live["timestamp"].min()
        span_h = (latest - earliest).total_seconds() / 3600 if pd.notna(earliest) else 0
        st.metric("Time span", f"{span_h:.1f} h")

    # Spot price feed
    if "spot_price" in live.columns and "market" in live.columns:
        fig = px.line(
            live.sort_values("timestamp"), x="timestamp", y="spot_price", color="market",
            title="Live spot price feed",
            labels={"spot_price": "Spot price (€/MWh)", "timestamp": ""},
        )
        st.plotly_chart(fig, use_container_width=True)

    # Fundamental signals
    fund_cols = [c for c in ("residual_load", "wind_forecast", "solar_forecast",
                              "load_forecast", "actual_load", "imbalance_price", "gas_price")
                 if c in live.columns]
    if fund_cols:
        sel_mkt = st.selectbox("Market", sorted(live["market"].unique()), key="live_mkt")
        mkt_live = live[live["market"] == sel_mkt].sort_values("timestamp")
        selected_fund = st.multiselect("Fundamental series", fund_cols, default=fund_cols[:3])
        if selected_fund:
            fig2 = px.line(mkt_live, x="timestamp", y=selected_fund,
                           title=f"Live fundamentals — {sel_mkt}")
            st.plotly_chart(fig2, use_container_width=True)

    # Live alpha signals if already computed
    alpha_live_cols = [c for c in live.columns if c.startswith("alpha_energy_")]
    if alpha_live_cols:
        st.subheader("Live Alpha Signals (pre-computed)")
        sel_mkt2 = st.selectbox("Market", sorted(live["market"].unique()), key="live_alpha_mkt")
        mkt_live2 = live[live["market"] == sel_mkt2].sort_values("timestamp")
        fig3 = px.line(mkt_live2, x="timestamp", y=alpha_live_cols,
                       title=f"Alpha signals — {sel_mkt2}")
        st.plotly_chart(fig3, use_container_width=True)

    with st.expander("Raw signal buffer (last 50 rows)"):
        st.dataframe(live.sort_values("timestamp", ascending=False).head(50),
                     use_container_width=True, hide_index=True)
else:
    st.info("No live signals found. Click 'Seed 48 h demo signals' above.")

st.divider()

# ── RisingWave simulator ──────────────────────────────────────────────────────
st.subheader("RisingWave Alpha Scores (Simulator / Offline)")

sim_triggered = run_sim or st.session_state.get("rw_sim_done", False)

if sim_triggered and not live.empty:
    try:
        from quant_alpha.streaming.risingwave.simulator import build_realtime_alpha_panel, get_scarcity_alerts
        with st.spinner("Running RisingWave simulator…"):
            scores = build_realtime_alpha_panel(live)
            alerts = get_scarcity_alerts(live)
        st.session_state["rw_sim_done"] = True
        st.session_state["rw_scores"] = scores
        st.session_state["rw_alerts"] = alerts
    except Exception as exc:
        st.warning(f"Simulator error: {exc}")
        scores = pd.DataFrame()
        alerts = pd.DataFrame()
elif "rw_scores" in st.session_state:
    scores = st.session_state["rw_scores"]
    alerts = st.session_state["rw_alerts"]
else:
    scores = pd.DataFrame()
    alerts = pd.DataFrame()

if not scores.empty:
    alpha_score_cols = [c for c in scores.columns if c.startswith("alpha_")]

    # Real-time alpha score heatmap (latest timestamp, all markets)
    ts_col = next((c for c in ("timestamp",) if c in scores.columns), None)
    if ts_col:
        scores[ts_col] = pd.to_datetime(scores[ts_col])
        latest_scores = scores[scores[ts_col] == scores[ts_col].max()]
        if "market" in latest_scores.columns and alpha_score_cols:
            pivot = latest_scores.set_index("market")[alpha_score_cols]
            fig4 = px.imshow(
                pivot,
                color_continuous_scale="RdYlGn",
                zmin=0, zmax=1,
                aspect="auto",
                title="Real-time alpha percentile scores — latest timestamp",
                text_auto=".2f",
            )
            st.plotly_chart(fig4, use_container_width=True)

    # Time series of composite alpha
    if alpha_score_cols and ts_col and "market" in scores.columns:
        scores["composite_alpha"] = scores[alpha_score_cols].mean(axis=1)
        fig5 = px.line(scores.sort_values(ts_col), x=ts_col, y="composite_alpha", color="market",
                       title="Composite real-time alpha score (equal-weight)",
                       labels={"composite_alpha": "Alpha percentile [0,1]", ts_col: ""})
        fig5.add_hline(y=0.5, line_dash="dot", line_color="gray")
        st.plotly_chart(fig5, use_container_width=True)

    # Scarcity alerts
    if not alerts.empty:
        st.subheader("Scarcity Alerts")
        level_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
        for _, row in alerts.iterrows():
            lvl = row.get("scarcity_level", "")
            icon = level_colors.get(lvl, "⚪")
            mkt = row.get("market", "")
            ts = str(row.get("timestamp", ""))[:16]
            price = row.get("spot_price", float("nan"))
            st.write(f"{icon} **{lvl}** — {mkt}  |  {ts}  |  Spot: {price:.1f} €/MWh")

        fig6 = px.scatter(
            alerts, x="timestamp" if "timestamp" in alerts.columns else alerts.columns[0],
            y="spot_price" if "spot_price" in alerts.columns else None,
            color="scarcity_level" if "scarcity_level" in alerts.columns else None,
            color_discrete_map={"HIGH": "#EF4444", "MEDIUM": "#F59E0B", "LOW": "#22C55E"},
            size_max=12,
            title="Scarcity alert events",
            labels={"spot_price": "Spot price (€/MWh)"},
        )
        st.plotly_chart(fig6, use_container_width=True)
        st.dataframe(alerts, use_container_width=True, hide_index=True)
elif sim_triggered:
    st.info("Simulator produced no output. Seed signals first.")
else:
    st.caption("Click **Run RisingWave simulator** to compute offline alpha scores from the signal buffer.")
