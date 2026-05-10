"""Page 5 — Market Data: energy market OHLCV, Spark rolling features, cross-market spread."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import load_table, pick, render_track_selector

st.title("⚡ Market Data Explorer")
track, is_energy, db, tm = render_track_selector(key="market_track")

raw = pick(db, *tm.get("raw", ()))
features = pick(db, *tm.get("features", ()))

# ═══════════════════════════════════════════════════════════════════════════════
# ENERGY TRACK
# ═══════════════════════════════════════════════════════════════════════════════
if is_energy:
    if raw.empty and features.empty:
        st.info("Run `quant-alpha energy-run` to generate market data.")
        st.stop()

    src = features if not features.empty else raw
    time_col = next((c for c in ("timestamp", "market_ts") if c in src.columns), None)
    if time_col:
        src[time_col] = pd.to_datetime(src[time_col])

    markets = sorted(src["market"].dropna().unique()) if "market" in src.columns else []
    selected_markets = st.multiselect("Markets", markets, default=markets[:3] if len(markets) >= 3 else markets)
    if not selected_markets:
        st.stop()

    panel = src[src["market"].isin(selected_markets)].sort_values(time_col) if time_col else src

    # Date range slider
    if time_col and not panel.empty:
        min_d, max_d = panel[time_col].min(), panel[time_col].max()
        date_range = st.slider("Date range", min_value=min_d.to_pydatetime(),
                               max_value=max_d.to_pydatetime(),
                               value=(min_d.to_pydatetime(), max_d.to_pydatetime()),
                               format="YYYY-MM-DD")
        panel = panel[(panel[time_col] >= date_range[0]) & (panel[time_col] <= date_range[1])]

    # ── Spot price ──────────────────────────────────────────────────────────
    if "spot_price" in panel.columns:
        st.subheader("Spot Price (€/MWh)")
        fig = px.line(panel, x=time_col, y="spot_price", color="market",
                      title="Spot price by market",
                      labels={"spot_price": "€/MWh", time_col: ""})
        st.plotly_chart(fig, use_container_width=True)

    # ── Supply / demand panel ────────────────────────────────────────────────
    supply_cols = [c for c in ("residual_load", "wind_forecast", "solar_forecast", "actual_load", "load_forecast")
                   if c in panel.columns]
    if supply_cols:
        st.subheader("Supply & Demand Fundamentals")
        sel_mkt = st.selectbox("Market (supply panel)", selected_markets, key="supply_mkt")
        mkt_panel = panel[panel["market"] == sel_mkt] if "market" in panel.columns else panel
        fig2 = px.line(mkt_panel, x=time_col, y=supply_cols,
                       title=f"Fundamentals — {sel_mkt}",
                       labels={time_col: ""})
        fig2.update_layout(height=380)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Imbalance premium ────────────────────────────────────────────────────
    if "imbalance_price" in panel.columns and "spot_price" in panel.columns:
        panel["imbalance_premium"] = panel["imbalance_price"] - panel["spot_price"]
        st.subheader("Imbalance Premium (Balancing − Spot, €/MWh)")
        fig3 = px.line(panel, x=time_col, y="imbalance_premium", color="market",
                       labels={"imbalance_premium": "€/MWh premium", time_col: ""},
                       color_discrete_sequence=px.colors.qualitative.Pastel)
        fig3.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Cross-market spread ──────────────────────────────────────────────────
    if "spot_price" in panel.columns and time_col and len(selected_markets) > 1:
        st.subheader("Cross-Market Spread vs European Average")
        cross_mean = panel.groupby(time_col)["spot_price"].transform("mean")
        panel = panel.copy()
        panel["spread_vs_avg"] = panel["spot_price"] - cross_mean
        fig4 = px.line(panel, x=time_col, y="spread_vs_avg", color="market",
                       title="Market spread vs cross-market average",
                       labels={"spread_vs_avg": "Spread (€/MWh)", time_col: ""})
        fig4.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig4, use_container_width=True)

    # ── Spark rolling features ───────────────────────────────────────────────
    spark_cols = [c for c in panel.columns if any(k in c for k in
                  ("rolling_spot", "rolling_residual", "residual_load_shock", "scarcity_flag",
                   "spot_return_1h", "imbalance_premium"))]
    if spark_cols:
        st.subheader("Spark Batch Rolling Features")
        sel_spark_mkt = st.selectbox("Market (Spark features)", selected_markets, key="spark_mkt")
        spark_panel = panel[panel["market"] == sel_spark_mkt] if "market" in panel.columns else panel
        selected_spark = st.multiselect("Features", spark_cols, default=spark_cols[:3])
        if selected_spark:
            fig5 = px.line(spark_panel, x=time_col, y=selected_spark,
                           title=f"Rolling features — {sel_spark_mkt}")
            st.plotly_chart(fig5, use_container_width=True)

    # ── Raw data table ───────────────────────────────────────────────────────
    with st.expander("Raw data sample (last 200 rows)"):
        st.dataframe(panel.tail(200), use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# EQUITY TRACK
# ═══════════════════════════════════════════════════════════════════════════════
else:
    if features.empty:
        st.info("Run `quant-alpha run --offline` to generate equity data.")
        st.stop()

    time_col = next((c for c in ("date",) if c in features.columns), None)
    if time_col:
        features[time_col] = pd.to_datetime(features[time_col])

    symbols = sorted(features["symbol"].dropna().unique()) if "symbol" in features.columns else []
    selected_sym = st.multiselect("Symbols", symbols, default=symbols[:5])
    if not selected_sym:
        st.stop()

    panel = features[features["symbol"].isin(selected_sym)].sort_values(time_col) if time_col else features

    price_cols = [c for c in ("adj_close", "close", "open", "high", "low") if c in panel.columns]
    if price_cols:
        st.subheader("Price History")
        fig = px.line(panel, x=time_col, y=price_cols[0], color="symbol",
                      title="Adjusted close price",
                      labels={price_cols[0]: "Price (USD)", time_col: ""})
        st.plotly_chart(fig, use_container_width=True)

    if "ret_1d" in panel.columns:
        st.subheader("Daily Returns")
        col_l, col_r = st.columns(2)
        with col_l:
            fig2 = px.line(panel, x=time_col, y="ret_1d", color="symbol",
                           title="Daily log return", labels={"ret_1d": "Return", time_col: ""})
            st.plotly_chart(fig2, use_container_width=True)
        with col_r:
            fig3 = px.histogram(panel, x="ret_1d", color="symbol", barmode="overlay",
                                nbins=80, title="Return distribution", opacity=0.6,
                                labels={"ret_1d": "Daily return"})
            st.plotly_chart(fig3, use_container_width=True)

    if "volume" in panel.columns:
        st.subheader("Volume")
        fig4 = px.bar(panel, x=time_col, y="volume", color="symbol", barmode="group",
                      title="Daily volume", labels={"volume": "Shares traded", time_col: ""})
        st.plotly_chart(fig4, use_container_width=True)

    with st.expander("Factor panel sample (last 200 rows)"):
        st.dataframe(panel.tail(200), use_container_width=True, hide_index=True)
