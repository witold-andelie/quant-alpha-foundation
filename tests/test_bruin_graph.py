from __future__ import annotations

from pathlib import Path


from quant_alpha.platform.bruin_graph import AssetGraph, AssetStatus

BRUIN_ROOT = Path(__file__).parent.parent / "bruin"


def test_graph_loads_assets() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    assert len(graph.nodes) >= 4, f"Expected ≥4 assets, got {len(graph.nodes)}"


def test_all_expected_assets_present() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    expected = {
        "raw_equity_ohlcv",
        "raw_power_market",
        "stg_equity_ohlcv",
        "stg_power_market",
        "fct_equity_alpha_panel",
        "fct_energy_alpha_panel",
        "fct_alpha_diagnostics",
        "rpt_backtest_summary",
    }
    missing = expected - set(graph.nodes)
    assert not missing, f"Missing assets: {missing}"


def test_topological_order_respects_dependencies() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    order = graph.topological_order()

    def pos(name: str) -> int:
        return order.index(name) if name in order else -1

    # raw must come before staging
    assert pos("raw_equity_ohlcv") < pos("stg_equity_ohlcv")
    assert pos("raw_power_market") < pos("stg_power_market")
    # staging before analytics
    assert pos("stg_equity_ohlcv") < pos("fct_equity_alpha_panel")
    assert pos("stg_power_market") < pos("fct_energy_alpha_panel")
    # diagnostics depends on panel
    assert pos("fct_equity_alpha_panel") < pos("fct_alpha_diagnostics")


def test_upstream_traversal() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    upstream = graph.upstream("fct_alpha_diagnostics")
    assert "fct_equity_alpha_panel" in upstream
    assert "stg_equity_ohlcv" in upstream


def test_downstream_traversal() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    downstream = graph.downstream("raw_equity_ohlcv")
    assert "stg_equity_ohlcv" in downstream
    assert "fct_equity_alpha_panel" in downstream


def test_dry_run_marks_all_skipped() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    results = graph.run(dry_run=True)
    statuses = set(results.values())
    assert statuses == {AssetStatus.SKIPPED}, f"Unexpected statuses: {statuses}"


def test_lineage_report_contains_asset_names() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    report = graph.lineage_report()
    assert "raw_equity_ohlcv" in report
    assert "fct_alpha_diagnostics" in report


def test_contracts_cover_all_tables() -> None:
    from quant_alpha.platform.contracts import ALL_DATASETS

    names = {d.name for d in ALL_DATASETS}
    assert "raw_prices" in names
    assert "alpha_diagnostics" in names
    assert "power_market_raw" in names


def test_asset_metadata_fields() -> None:
    graph = AssetGraph(BRUIN_ROOT)
    node = graph.nodes["raw_power_market"]
    assert node.owner == "energy-research"
    assert "energy" in node.tags
    assert len(node.columns) > 5
    assert len(node.custom_checks) >= 2
