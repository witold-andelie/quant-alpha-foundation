"""Page 7 — Data Pipeline: Bruin asset graph, table inventory, data quality, dlt status."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ENERGY_DB, EQUITY_DB, ROOT, load_table, list_tables, table_row_count, pick, render_track_selector

st.title("🔧 Data Pipeline Status")
track, is_energy, db, _ = render_track_selector(key="pipeline_track")

# ── Table inventory ───────────────────────────────────────────────────────────
st.subheader("DuckDB Table Inventory")

energy_tables = list_tables(ENERGY_DB)
equity_tables = list_tables(EQUITY_DB)

col_e, col_q = st.columns(2)

def _table_inventory(db_path: Path, tables: list[str], label: str):
    if not tables:
        st.info(f"No {label} database found.")
        return
    rows = []
    for t in tables:
        n = table_row_count(db_path, t)
        rows.append({"table": t, "rows": n, "status": "✅" if n > 0 else "⚠️ empty"})
    df = pd.DataFrame(rows)
    st.markdown(f"**{label}** — `{db_path.name}` ({len(tables)} tables)")
    st.dataframe(df, use_container_width=True, hide_index=True)
    total = df["rows"].sum()
    st.metric(f"Total rows ({label})", f"{total:,}")

with col_e:
    _table_inventory(ENERGY_DB, energy_tables, "Energy")
with col_q:
    _table_inventory(EQUITY_DB, equity_tables, "Equity")

st.divider()

# ── Bruin asset graph ─────────────────────────────────────────────────────────
st.subheader("Bruin Asset Graph — Lineage")

bruin_root = ROOT / "bruin"
col_btn1, col_btn2 = st.columns(2)
with col_btn1:
    run_lineage = st.button("Load Bruin lineage", type="primary")
with col_btn2:
    run_dry = st.button("Dry-run asset graph")

if run_lineage or run_dry or st.session_state.get("bruin_loaded"):
    try:
        from quant_alpha.platform.bruin_graph import AssetGraph
        graph = AssetGraph(bruin_root)
        st.session_state["bruin_loaded"] = True

        order = graph.topological_order()
        st.success(f"Loaded {len(graph.nodes)} assets in topological order")

        # Asset graph table
        rows = []
        for name in order:
            node = graph.nodes[name]
            rows.append({
                "asset": name,
                "type": node.asset_type,
                "connection": node.connection,
                "depends_on": ", ".join(node.depends) if node.depends else "—",
                "owner": node.owner,
                "tags": ", ".join(node.tags) if node.tags else "—",
            })
        asset_df = pd.DataFrame(rows)
        st.dataframe(asset_df, use_container_width=True, hide_index=True)

        # Lineage text
        with st.expander("Full lineage report"):
            st.code(graph.lineage_report(), language=None)

        # Upstream / downstream explorer
        st.subheader("Lineage Explorer")
        col_a, col_b = st.columns(2)
        with col_a:
            sel_asset = st.selectbox("Asset", order)
            if sel_asset:
                up = graph.upstream(sel_asset)
                dn = graph.downstream(sel_asset)
                st.markdown(f"**Upstream of `{sel_asset}`:** {', '.join(up) if up else 'none'}")
                st.markdown(f"**Downstream of `{sel_asset}`:** {', '.join(dn) if dn else 'none'}")

        if run_dry:
            st.subheader("Dry-Run Result")
            results = graph.run(dry_run=True)
            dry_rows = [{"asset": k, "status": v.value} for k, v in results.items()]
            st.dataframe(pd.DataFrame(dry_rows), use_container_width=True, hide_index=True)

    except Exception as exc:
        st.warning(f"Could not load Bruin graph: {exc}")

st.divider()

# ── Data quality report ───────────────────────────────────────────────────────
st.subheader("Data Quality Report")

quality = pick(ENERGY_DB, "energy_quality_report")
if not quality.empty:
    st.dataframe(quality, use_container_width=True, hide_index=True)
else:
    if st.button("Run energy quality checks"):
        try:
            from quant_alpha.platform.quality import run_energy_quality_checks
            raw = pick(ENERGY_DB, "power_market_raw", "power_market_features")
            if raw.empty:
                st.warning("No raw energy data found.")
            else:
                with st.spinner("Running quality checks…"):
                    report = run_energy_quality_checks(raw)
                st.success("Quality checks complete")
                if isinstance(report, dict):
                    st.json(report)
                elif hasattr(report, "items"):
                    st.dataframe(pd.DataFrame([report]), use_container_width=True, hide_index=True)
        except Exception as exc:
            st.warning(f"Quality check error: {exc}")
    else:
        st.info("Click 'Run energy quality checks' to validate the raw data.")

# Quick schema checks from features table
features = pick(db, "power_market_features") if is_energy else pick(EQUITY_DB, "factor_panel")
if not features.empty:
    st.subheader("Feature Table Schema & Null Rates")
    null_rates = (features.isnull().mean() * 100).reset_index()
    null_rates.columns = ["column", "null_rate_%"]
    null_rates["dtype"] = null_rates["column"].map(lambda c: str(features[c].dtype))
    null_rates["sample_value"] = null_rates["column"].map(
        lambda c: str(features[c].dropna().iloc[0]) if not features[c].dropna().empty else "—"
    )
    fig = px.bar(
        null_rates[null_rates["null_rate_%"] > 0].sort_values("null_rate_%", ascending=False),
        x="column", y="null_rate_%",
        title="Columns with null values (% of rows)",
        labels={"null_rate_%": "Null rate %", "column": ""},
        color="null_rate_%",
        color_continuous_scale=["#22C55E", "#F59E0B", "#EF4444"],
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(null_rates, use_container_width=True, hide_index=True)

st.divider()

# ── dlt pipeline metadata ─────────────────────────────────────────────────────
st.subheader("dlt Pipeline State")

dlt_tables_energy = [t for t in energy_tables if t.startswith("_dlt") or "dlt" in t.lower()]
dlt_tables_equity = [t for t in equity_tables if t.startswith("_dlt") or "dlt" in t.lower()]

if dlt_tables_energy or dlt_tables_equity:
    for db_path, tbls, label in [
        (ENERGY_DB, dlt_tables_energy, "Energy"),
        (EQUITY_DB, dlt_tables_equity, "Equity"),
    ]:
        if tbls:
            st.markdown(f"**{label} dlt metadata tables:** {', '.join(tbls)}")
            if "_dlt_loads" in tbls:
                loads = load_table(db_path, "_dlt_loads")
                if not loads.empty:
                    st.dataframe(loads.sort_values(loads.columns[0], ascending=False).head(10),
                                 use_container_width=True, hide_index=True)
else:
    st.info("No dlt metadata tables found. Run `quant-alpha dlt-energy` or `quant-alpha dlt-equity` first.")

# ── Kestra flow inventory ──────────────────────────────────────────────────────
st.subheader("Kestra Flow Inventory")
flow_dir = ROOT / "flows/kestra"
if flow_dir.exists():
    flows = sorted(flow_dir.glob("*.yaml"))
    for f in flows:
        with st.expander(f.stem):
            st.code(f.read_text(), language="yaml")
else:
    st.info("No Kestra flow directory found.")
