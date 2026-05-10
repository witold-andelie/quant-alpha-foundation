"""Page 4 — Alpha Decay: IC decay curves, walk-forward stability, turnover vs Sharpe."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import load_table, pick, render_track_selector

st.title("📉 Alpha Decay & Walk-Forward Stability")
track, is_energy, db, tm = render_track_selector(key="decay_track")

decay = pick(db, *tm["decay"])
wf = pick(db, *tm["walk_forward"])
turnover = pick(db, *tm["turnover"])
diagnostics = pick(db, *tm["diagnostics"])

# ── IC decay curves ───────────────────────────────────────────────────────────
if not decay.empty:
    x_col = next((c for c in ("horizon_hours", "horizon_days") if c in decay.columns), None)
    if x_col:
        st.subheader("Alpha Decay Curve — IC by Forward Horizon")
        col_l, col_r = st.columns([2, 1])
        with col_l:
            fig = px.line(
                decay.dropna(subset=["ic"]),
                x=x_col, y="ic", color="alpha_name",
                markers=True,
                title="Rank IC across holding horizons",
                labels={x_col: "Forward horizon (hours)" if "hours" in x_col else "Forward days",
                        "ic": "Rank IC"},
            )
            fig.add_hline(y=0, line_dash="dot", line_color="gray")
            fig.update_layout(height=400, legend=dict(orientation="h", yanchor="bottom", y=-0.4))
            st.plotly_chart(fig, use_container_width=True)
        with col_r:
            st.caption("IC decay interpretation")
            st.markdown("""
- **Slow decay** → factor captures lasting structural inefficiency
- **Fast decay** → microstructure / noise (short-lived)
- **Sign flip** → momentum reversal at longer horizon
- Target: IC > 0.02 at primary holding horizon
""")

        # Decay heatmap
        pivot = decay.dropna(subset=["ic"]).pivot_table(
            index="alpha_name", columns=x_col, values="ic"
        )
        if not pivot.empty:
            fig2 = px.imshow(
                pivot,
                color_continuous_scale="RdBu_r",
                color_continuous_midpoint=0,
                aspect="auto",
                title=f"IC heatmap — alpha × horizon",
                text_auto=".3f",
            )
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(decay, use_container_width=True, hide_index=True)
else:
    st.info("No decay data found. Run the pipeline to generate alpha decay curves.")

st.divider()

# ── Walk-forward IC stability ─────────────────────────────────────────────────
st.subheader("Walk-Forward OOS IC Stability")
if not wf.empty and "alpha_name" in wf.columns:
    alpha_options = sorted(wf["alpha_name"].dropna().unique())
    selected = st.selectbox("Alpha factor", alpha_options)
    sub = wf[wf["alpha_name"] == selected].sort_values("window" if "window" in wf.columns else wf.columns[0])

    fig3 = px.bar(
        sub,
        x="window" if "window" in sub.columns else sub.columns[0],
        y="ic_mean",
        color="ic_mean",
        color_continuous_scale=["#EF4444", "#D1D5DB", "#22C55E"],
        color_continuous_midpoint=0,
        title=f"Walk-forward OOS IC per window — {selected}",
        labels={"window": "Rolling window", "ic_mean": "OOS IC mean"},
        error_y="ic_ir" if "ic_ir" in sub.columns else None,
    )
    fig3.add_hline(y=0, line_dash="dot", line_color="gray")
    fig3.update_layout(height=320, coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

    # IC IR over windows
    if "ic_ir" in sub.columns:
        fig4 = px.line(sub, x="window", y="ic_ir", markers=True,
                       title=f"Walk-forward IC IR — {selected}",
                       color_discrete_sequence=["#7C3AED"])
        fig4.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig4, use_container_width=True)

    tbl_cols = [c for c in ["window", "oos_start", "oos_end", "ic_mean", "ic_ir", "n_days"] if c in sub.columns]
    st.dataframe(sub[tbl_cols], use_container_width=True, hide_index=True)

    # Stability summary across all alphas
    st.subheader("Cross-Alpha Stability Summary")
    stability = (
        wf.groupby("alpha_name")["ic_mean"]
        .agg(
            windows="count",
            mean_ic="mean",
            std_ic="std",
            positive_windows=lambda s: (s > 0).sum(),
        )
        .reset_index()
    )
    stability["positive_rate"] = stability["positive_windows"] / stability["windows"]
    stability["ic_ir"] = stability["mean_ic"] / stability["std_ic"].clip(lower=1e-9)
    st.dataframe(
        stability.sort_values("mean_ic", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Walk-forward data not available — requires at least 2 years of history.")

st.divider()

# ── Turnover vs Sharpe ────────────────────────────────────────────────────────
st.subheader("Turnover vs OOS Sharpe — Cost Efficiency")
if not turnover.empty and not diagnostics.empty and "alpha_name" in turnover.columns:
    merged = turnover.merge(
        diagnostics[["alpha_name"] + [c for c in ["oos_sharpe", "consistency_score"] if c in diagnostics.columns]],
        on="alpha_name", how="inner",
    )
    if not merged.empty and "mean_daily_turnover" in merged.columns:
        color_col = "consistency_score" if "consistency_score" in merged.columns else None
        fig5 = px.scatter(
            merged,
            x="mean_daily_turnover",
            y="oos_sharpe" if "oos_sharpe" in merged.columns else merged.columns[-1],
            text="alpha_name",
            color=color_col,
            color_continuous_scale="RdYlGn",
            size="mean_daily_turnover",
            title="Turnover vs OOS Sharpe (lower-right = efficient)",
            labels={"mean_daily_turnover": "Mean daily turnover", "oos_sharpe": "OOS Sharpe"},
        )
        fig5.update_traces(textposition="top center")
        st.plotly_chart(fig5, use_container_width=True)

        st.dataframe(turnover, use_container_width=True, hide_index=True)
elif not turnover.empty:
    st.dataframe(turnover, use_container_width=True, hide_index=True)
else:
    st.info("Turnover data not available.")
