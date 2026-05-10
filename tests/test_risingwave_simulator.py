from __future__ import annotations

from quant_alpha.streaming.risingwave.simulator import build_realtime_alpha_panel, get_scarcity_alerts


def test_realtime_alpha_panel_shape() -> None:
    panel = build_realtime_alpha_panel(markets=["DE_LU", "FR"], hours=12)
    assert not panel.empty
    # 12h × 2 markets = 24 rows (approximately — may vary at boundaries)
    assert len(panel) >= 20


def test_realtime_alpha_panel_columns() -> None:
    panel = build_realtime_alpha_panel(markets=["DE_LU"], hours=6)
    required = {
        "timestamp", "market", "spot_price",
        "alpha_residual_load_rank",
        "alpha_imbalance_premium",
        "alpha_cross_market_spread",
        "alpha_demand_surprise",
        "alpha_solar_penetration",
        "alpha_momentum_6h",
        "alpha_gas_spark_spread",
    }
    missing = required - set(panel.columns)
    assert not missing, f"Missing columns: {missing}"


def test_alpha_scores_bounded_01() -> None:
    panel = build_realtime_alpha_panel(markets=["DE_LU", "CZ", "FR"], hours=24)
    alpha_cols = [c for c in panel.columns if c.startswith("alpha_")]
    for col in alpha_cols:
        valid = panel[col].dropna()
        assert (valid >= 0).all() and (valid <= 1).all(), f"{col} out of [0,1]"


def test_all_markets_present() -> None:
    markets = ["DE_LU", "CZ", "FR"]
    panel = build_realtime_alpha_panel(markets=markets, hours=6)
    assert set(panel["market"].unique()) == set(markets)


def test_scarcity_alerts_subset_of_panel() -> None:
    panel = build_realtime_alpha_panel(markets=["DE_LU", "CZ", "FR"], hours=48)
    alerts = get_scarcity_alerts(panel, threshold=0.8)
    assert len(alerts) <= len(panel)
    if not alerts.empty:
        assert (alerts["alpha_residual_load_rank"] > 0.8).all()


def test_scarcity_level_values() -> None:
    panel = build_realtime_alpha_panel(markets=["DE_LU", "CZ", "FR"], hours=48)
    alerts = get_scarcity_alerts(panel, threshold=0.0)  # all rows
    valid_levels = {"HIGH", "MEDIUM"}
    assert set(alerts["scarcity_level"].unique()).issubset(valid_levels)


def test_views_sql_parses() -> None:
    """Verify views.sql file is readable and contains expected view names."""
    from pathlib import Path
    sql_path = Path(__file__).parent.parent / "src/quant_alpha/streaming/risingwave/views.sql"
    sql = sql_path.read_text()
    for view in (
        "mv_energy_hourly_window",
        "mv_energy_momentum_6h",
        "mv_cross_market_spread",
        "mv_realtime_alpha_scores",
        "mv_scarcity_alerts",
    ):
        assert view in sql, f"Missing view: {view}"


def test_client_split_statements() -> None:
    from quant_alpha.streaming.risingwave.client import _split_statements
    sql = """
    -- comment
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_foo AS SELECT 1;
    CREATE SOURCE IF NOT EXISTS bar () WITH (connector='kafka');
    """
    stmts = _split_statements(sql)
    assert len(stmts) == 2
    assert all("CREATE" in s.upper() for s in stmts)
