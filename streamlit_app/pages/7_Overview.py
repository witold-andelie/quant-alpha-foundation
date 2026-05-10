"""Page 1 — Overview: both-track summary, platform health, module coverage."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ENERGY_DB, EQUITY_DB, ENERGY_TABLES, EQUITY_TABLES, ROOT, load_table, list_tables, pick

st.title("📊 Platform Overview")
st.caption("Cross-track summary — Second Foundation Energy · US Equities Demo")

col_l, col_r = st.columns([5, 1])
with col_r:
    if st.button("🏠 Home", use_container_width=True, key="overview_home"):
        st.switch_page("home.py")

# ── Both-track metrics side by side ──────────────────────────────────────────
col_e, col_q = st.columns(2)

def _metric_block(db: Path, tm: dict, label: str, col):
    with col:
        st.markdown(f"### {label}")
        metrics = pick(db, *tm["metrics"])
        registry = pick(db, *tm["registry"])
        diagnostics = pick(db, *tm["diagnostics"])

        def _m(col_name, fmt):
            if metrics.empty or col_name not in metrics.columns:
                return "—"
            try:
                return fmt.format(float(metrics[col_name].iloc[0]))
            except Exception:
                return "—"

        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Sharpe", _m("sharpe", "{:.2f}"))
        with c2: st.metric("Max DD", _m("max_drawdown", "{:.1%}"))
        with c3: st.metric("Factors", len(registry) if not registry.empty else "—")

        if not diagnostics.empty and "consistency_score" in diagnostics.columns:
            passing = (diagnostics["consistency_score"] >= 0.5).sum()
            st.metric("Factors passing consistency gate", f"{passing} / {len(diagnostics)}")

        backtest = pick(db, *tm["backtest"])
        if not backtest.empty:
            x_col = next((c for c in ("market_ts", "date") if c in backtest.columns), None)
            if x_col:
                backtest[x_col] = pd.to_datetime(backtest[x_col])
                fig = px.line(backtest.sort_values(x_col), x=x_col, y="equity_curve",
                              labels={x_col: "", "equity_curve": "NAV"},
                              color_discrete_sequence=["#2563EB"])
                fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=0),
                                  showlegend=False, yaxis_title="NAV")
                st.plotly_chart(fig, use_container_width=True)

_metric_block(ENERGY_DB, ENERGY_TABLES, "⚡ Second Foundation Energy", col_e)
_metric_block(EQUITY_DB, EQUITY_TABLES, "📈 US Equities Demo", col_q)

st.divider()

# ── Platform health matrix ────────────────────────────────────────────────────
st.subheader("Module Coverage — Health Matrix")

energy_tbls = set(list_tables(ENERGY_DB))
equity_tbls = set(list_tables(EQUITY_DB))

def _chk(condition: bool) -> str:
    return "✅" if condition else "⬜"

health = [
    {"Module": "M1 — Containerization", "Technology": "Docker · Terraform", "Status": _chk((ROOT / "infra/terraform").exists())},
    {"Module": "M2 — Orchestration", "Technology": "Kestra (5 flows)", "Status": _chk((ROOT / "flows/kestra").exists())},
    {"Module": "Workshop 1 — dlt Ingestion", "Technology": "dlt v1.26 incremental", "Status": _chk(bool(energy_tbls | equity_tbls))},
    {"Module": "M3 — Data Warehouse", "Technology": "DuckDB · BigQuery", "Status": _chk(ENERGY_DB.exists() or EQUITY_DB.exists())},
    {"Module": "M4 — Analytics Eng.", "Technology": "dbt (19 models)", "Status": _chk((ROOT / "dbt_energy_alpha").exists())},
    {"Module": "M5 — Data Platforms", "Technology": "Bruin (8 assets)", "Status": _chk((ROOT / "bruin/pipelines").exists())},
    {"Module": "M6 — Batch Processing", "Technology": "Apache Spark (7 features)", "Status": _chk((ROOT / "src/quant_alpha/batch").exists())},
    {"Module": "M7 — Streaming", "Technology": "Redpanda · Avro", "Status": _chk("live_energy_signals" in energy_tbls)},
    {"Module": "Workshop 2 — RisingWave", "Technology": "5 materialized views", "Status": _chk((ROOT / "src/quant_alpha/streaming/risingwave").exists())},
    {"Module": "Cloud + K8s", "Technology": "Helm chart · GKE · WI", "Status": _chk((ROOT / "infra/helm").exists())},
    {"Module": "CI/CD", "Technology": "GitHub Actions (7-stage)", "Status": _chk((ROOT / ".github/workflows").exists())},
]
health_df = pd.DataFrame(health)
st.dataframe(health_df, use_container_width=True, hide_index=True)

st.divider()

# ── Alpha universe radar ──────────────────────────────────────────────────────
st.subheader("Alpha Factor Families")

e_reg = pick(ENERGY_DB, *ENERGY_TABLES["registry"])
q_reg = pick(EQUITY_DB, *EQUITY_TABLES["registry"])

col_r1, col_r2 = st.columns(2)

def _family_chart(reg: pd.DataFrame, title: str, col):
    with col:
        if reg.empty or "family" not in reg.columns:
            st.info("No registry data")
            return
        fam = reg["family"].value_counts().reset_index()
        fam.columns = ["family", "count"]
        fig = px.pie(fam, names="family", values="count", title=title,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

_family_chart(e_reg, "Energy alpha families", col_r1)
_family_chart(q_reg, "Equity alpha families", col_r2)

# ── Diagnostics comparison ────────────────────────────────────────────────────
e_diag = pick(ENERGY_DB, *ENERGY_TABLES["diagnostics"])
q_diag = pick(EQUITY_DB, *EQUITY_TABLES["diagnostics"])

if not e_diag.empty or not q_diag.empty:
    st.subheader("Alpha Diagnostics — Cross-Track Comparison")

    frames = []
    if not e_diag.empty:
        e_diag["track"] = "Energy"
        frames.append(e_diag)
    if not q_diag.empty:
        q_diag["track"] = "Equity"
        frames.append(q_diag)

    combined = pd.concat(frames, ignore_index=True)
    score_cols = [c for c in ("consistency_score", "robustness_score") if c in combined.columns]

    if score_cols and "track" in combined.columns and "alpha_name" in combined.columns:
        fig2 = px.bar(
            combined.sort_values(score_cols[0], ascending=False),
            x="alpha_name", y=score_cols[0], color="track",
            barmode="group",
            title=f"{score_cols[0].replace('_', ' ').title()} by alpha and track",
            labels={"alpha_name": "", score_cols[0]: "Score [0,1]"},
            color_discrete_map={"Energy": "#2563EB", "Equity": "#22C55E"},
        )
        fig2.add_hline(y=0.5, line_dash="dot", line_color="gray",
                       annotation_text="threshold=0.5")
        fig2.update_layout(height=350, xaxis_tickangle=-30)
        st.plotly_chart(fig2, use_container_width=True)

# ── Quick command reference ───────────────────────────────────────────────────
st.divider()
st.subheader("Quick Commands")
st.code("""
# Energy pipeline (synthetic data)
quant-alpha energy-run

# Equity pipeline (offline)
quant-alpha run --offline

# dlt incremental ingestion
quant-alpha dlt-energy --start 2024-01-01
quant-alpha dlt-equity --offline

# Bruin asset graph
quant-alpha bruin-lineage
quant-alpha bruin-run --dry-run

# Streaming stack
docker compose up -d redpanda redpanda-console
docker compose -f docker-compose.risingwave.yml up -d
python -m quant_alpha.streaming.demo_signals
""", language="bash")
