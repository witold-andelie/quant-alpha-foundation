"""Page 2 — Performance: equity curve, drawdown, P&L distribution, per-alpha attribution."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import load_table, pick, render_track_selector

st.title("📈 Performance Analysis")
track, is_energy, db, tm = render_track_selector(key="perf_track")

backtest = pick(db, *tm["backtest"])
metrics = pick(db, *tm["metrics"])

if backtest.empty:
    st.info("Run the pipeline to generate backtest data.")
    st.stop()

x_col = next((c for c in ("market_ts", "date") if c in backtest.columns), "date")
backtest = backtest.sort_values(x_col).copy()
backtest[x_col] = pd.to_datetime(backtest[x_col])

# ── Summary metrics ───────────────────────────────────────────────────────────
def _m(col: str, fmt: str) -> str:
    if metrics.empty or col not in metrics.columns:
        return "—"
    try:
        return fmt.format(float(metrics[col].iloc[0]))
    except Exception:
        return "—"

c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1: st.metric("Total Return",   _m("total_return",          "{:.1%}"))
with c2: st.metric("Ann. Return",    _m("annualized_return",     "{:.1%}"))
with c3: st.metric("Ann. Vol",       _m("annualized_volatility", "{:.1%}"))
with c4: st.metric("Sharpe",         _m("sharpe",                "{:.2f}"))
with c5: st.metric("Sortino",        _m("sortino",               "{:.2f}"))
with c6: st.metric("Max Drawdown",   _m("max_drawdown",          "{:.1%}"))

st.divider()

# ── Equity curve + drawdown ───────────────────────────────────────────────────
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    row_heights=[0.65, 0.35],
    subplot_titles=["Equity Curve (NAV)", "Drawdown"],
    vertical_spacing=0.08,
)

fig.add_trace(
    go.Scatter(x=backtest[x_col], y=backtest["equity_curve"],
               mode="lines", name="NAV", line=dict(color="#2563EB", width=2)),
    row=1, col=1,
)
fig.add_hline(y=1, line_dash="dot", line_color="gray", row=1, col=1)

if "equity_curve" in backtest.columns:
    dd = backtest["equity_curve"] / backtest["equity_curve"].cummax() - 1
    fig.add_trace(
        go.Scatter(x=backtest[x_col], y=dd, mode="lines", name="Drawdown",
                   fill="tozeroy", line=dict(color="#EF4444", width=1),
                   fillcolor="rgba(239,68,68,0.15)"),
        row=2, col=1,
    )

fig.update_layout(height=500, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# ── Rolling Sharpe (63-day) ───────────────────────────────────────────────────
if "portfolio_return" in backtest.columns:
    ret = backtest.set_index(x_col)["portfolio_return"]
    roll_sharpe = (ret.rolling(63).mean() / ret.rolling(63).std(ddof=0)) * np.sqrt(252)
    fig2 = px.line(
        x=roll_sharpe.index, y=roll_sharpe.values,
        title="Rolling 63-day Sharpe ratio",
        labels={"x": "", "y": "Sharpe"},
        color_discrete_sequence=["#7C3AED"],
    )
    fig2.add_hline(y=0, line_dash="dot", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)

# ── Return distribution ───────────────────────────────────────────────────────
if "portfolio_return" in backtest.columns:
    col_l, col_r = st.columns(2)
    with col_l:
        fig3 = px.histogram(
            backtest, x="portfolio_return", nbins=60,
            title="Daily return distribution",
            labels={"portfolio_return": "Daily return"},
            color_discrete_sequence=["#2563EB"],
        )
        fig3.add_vline(x=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig3, use_container_width=True)
    with col_r:
        ret_vals = backtest["portfolio_return"].dropna()
        win_rate = (ret_vals > 0).mean()
        skew = float(ret_vals.skew())
        kurt = float(ret_vals.kurt())
        st.metric("Win rate", f"{win_rate:.1%}")
        st.metric("Skewness", f"{skew:.2f}")
        st.metric("Excess kurtosis", f"{kurt:.2f}")
        if "observations" in metrics.columns and not metrics.empty:
            st.metric("Trading days", int(metrics["observations"].iloc[0]))

# ── Long / short count history ────────────────────────────────────────────────
if "long_count" in backtest.columns and "short_count" in backtest.columns:
    st.subheader("Long / Short Holdings per Day")
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=backtest[x_col], y=backtest["long_count"],
                              mode="lines", name="Longs", line=dict(color="#22C55E")))
    fig4.add_trace(go.Scatter(x=backtest[x_col], y=-backtest["short_count"],
                              mode="lines", name="Shorts (inverted)", line=dict(color="#EF4444")))
    fig4.update_layout(height=250, yaxis_title="Count")
    st.plotly_chart(fig4, use_container_width=True)

# ── Per-alpha cumulative P&L ──────────────────────────────────────────────────
features = pick(db, *tm["features"])
alpha_cols = [c for c in features.columns if c.startswith("alpha_")] if not features.empty else []
if alpha_cols and not features.empty:
    st.subheader("Factor Signal Snapshot — Latest Date")
    fx_col = next((c for c in ("market_ts", "timestamp", "date") if c in features.columns), None)
    if fx_col:
        features[fx_col] = pd.to_datetime(features[fx_col])
        latest = features[features[fx_col] == features[fx_col].max()]
        group_col = next((c for c in ("market", "symbol") if c in latest.columns), None)
        if group_col:
            melt = latest.melt(id_vars=[fx_col, group_col], value_vars=alpha_cols,
                               var_name="alpha", value_name="score")
            fig5 = px.bar(
                melt, x="alpha", y="score", color=group_col, barmode="group",
                title="Alpha scores — latest snapshot",
                labels={"alpha": "", "score": "Signal value"},
            )
            fig5.update_layout(height=350)
            st.plotly_chart(fig5, use_container_width=True)
