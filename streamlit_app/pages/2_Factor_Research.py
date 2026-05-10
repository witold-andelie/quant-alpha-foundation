"""Page 3 — Factor Research: 4-gate scorecard, IS/OOS scatter, correlation heatmap."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import load_table, pick, render_track_selector

st.title("🔬 Factor Research")
track, is_energy, db, tm = render_track_selector(key="factor_track")

diagnostics = pick(db, *tm["diagnostics"])
registry = pick(db, *tm["registry"])
corr_df = pick(db, *tm["correlation"])
features = pick(db, *tm["features"])

# ── Alpha registry ────────────────────────────────────────────────────────────
if not registry.empty:
    st.subheader("Alpha Registry")
    reg_cols = [c for c in ["alpha_name", "family", "expression", "hypothesis", "expected_direction"]
                if c in registry.columns]
    st.dataframe(registry[reg_cols], use_container_width=True, hide_index=True)

# ── 4-Gate scorecard ──────────────────────────────────────────────────────────
if not diagnostics.empty:
    st.subheader("Four-Gate Validation")

    def _gate_icon(val, threshold=0.5) -> str:
        if pd.isna(val):
            return "⬜"
        return "✅" if float(val) >= threshold else "❌"

    diag = diagnostics.copy()
    # Robustness gate: robustness_score >= 0.5
    if "robustness_score" in diag.columns:
        diag["Gate: Robustness"] = diag["robustness_score"].apply(_gate_icon)
    # Uniqueness gate: max pairwise correlation < 0.7 (use flag if available, else derive)
    if "consistency_score" in diag.columns:
        diag["Gate: Consistency"] = diag["consistency_score"].apply(_gate_icon)
    # Direction gate: IS and OOS IC same sign
    if "is_oos_ic_same_sign" in diag.columns:
        diag["Gate: Direction"] = diag["is_oos_ic_same_sign"].apply(
            lambda v: "✅" if v is True else ("❌" if v is False else "⬜")
        )
    # OOS Sharpe gate
    if "oos_sharpe" in diag.columns:
        diag["Gate: OOS Sharpe"] = diag["oos_sharpe"].apply(lambda v: _gate_icon(v, 0.3))

    gate_cols = ["alpha_name"] + [c for c in diag.columns if c.startswith("Gate:")]
    numeric_score_cols = [c for c in ["consistency_score", "robustness_score"] if c in diag.columns]
    display_cols = gate_cols + numeric_score_cols + [
        c for c in ["is_ic_mean", "oos_ic_mean", "oos_sharpe"] if c in diag.columns
    ]

    st.dataframe(
        diag[display_cols].sort_values("consistency_score" if "consistency_score" in diag.columns else display_cols[1],
                                       ascending=False),
        use_container_width=True,
        hide_index=True,
    )

# ── IS vs OOS IC scatter ──────────────────────────────────────────────────────
if not diagnostics.empty and "is_ic_mean" in diagnostics.columns and "oos_ic_mean" in diagnostics.columns:
    st.subheader("IS vs OOS Information Coefficient")
    sub = diagnostics.dropna(subset=["is_ic_mean", "oos_ic_mean"])
    if not sub.empty:
        fig = px.scatter(
            sub,
            x="is_ic_mean", y="oos_ic_mean",
            text="alpha_name" if "alpha_name" in sub.columns else None,
            color="consistency_score" if "consistency_score" in sub.columns else None,
            color_continuous_scale="RdYlGn",
            title="In-sample IC vs Out-of-sample IC (ideal: upper-right quadrant, same sign)",
            labels={"is_ic_mean": "IS IC mean", "oos_ic_mean": "OOS IC mean"},
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        # Diagonal guide
        rng = max(abs(sub["is_ic_mean"].max()), abs(sub["oos_ic_mean"].max()), 0.01)
        fig.add_shape(type="line", x0=-rng, y0=-rng, x1=rng, y1=rng,
                      line=dict(dash="dot", color="steelblue"))
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)

# ── Correlation heatmap ───────────────────────────────────────────────────────
if not corr_df.empty and "alpha_left" in corr_df.columns:
    st.subheader("Alpha Pairwise Correlation Heatmap")
    pivot = corr_df.pivot(index="alpha_left", columns="alpha_right", values="spearman_corr")
    fig2 = px.imshow(
        pivot,
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        aspect="auto",
        title="Spearman rank correlation between alpha signals",
        text_auto=".2f",
    )
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("Values near ±1 indicate redundant factors. Target: |corr| < 0.7 for uniqueness gate.")
elif not features.empty:
    alpha_cols = [c for c in features.columns if c.startswith("alpha_")]
    if len(alpha_cols) >= 2:
        st.subheader("Alpha Pairwise Correlation Heatmap (computed from features)")
        corr_mat = features[alpha_cols].corr(method="spearman")
        fig2 = px.imshow(
            corr_mat,
            color_continuous_scale="RdBu_r",
            zmin=-1, zmax=1,
            aspect="auto",
            text_auto=".2f",
            title="Spearman correlation (from live feature data)",
        )
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, use_container_width=True)

# ── Factor history ────────────────────────────────────────────────────────────
if not features.empty:
    st.subheader("Factor Signal History")
    time_col = next((c for c in ("timestamp", "date", "market_ts") if c in features.columns), None)
    group_col = next((c for c in ("market", "symbol") if c in features.columns), None)
    alpha_cols = [c for c in features.columns if c.startswith("alpha_")]

    if time_col and group_col and alpha_cols:
        features[time_col] = pd.to_datetime(features[time_col])
        groups = sorted(features[group_col].dropna().unique())
        selected = st.selectbox(group_col.title(), groups)
        sub = features[features[group_col] == selected].sort_values(time_col)
        selected_alphas = st.multiselect("Factors to display", alpha_cols, default=alpha_cols[:4])
        if selected_alphas:
            fig3 = px.line(sub, x=time_col, y=selected_alphas,
                           title=f"Factor history — {selected}",
                           labels={time_col: ""})
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)
