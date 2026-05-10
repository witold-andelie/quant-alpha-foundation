from __future__ import annotations

from quant_alpha.backtest.alpha_decay import compute_alpha_decay
from quant_alpha.config import ProjectConfig, Universe
from quant_alpha.features.alpha_factors import BASE_FACTOR_COLUMNS, add_alpha_factors
from quant_alpha.ingestion.yahoo import generate_synthetic_prices


def test_alpha_panel_contains_expected_columns() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-06-30")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE"])

    prices = generate_synthetic_prices(cfg, universe)
    panel = add_alpha_factors(prices, cfg)

    for column in BASE_FACTOR_COLUMNS + ["alpha_composite", "forward_return"]:
        assert column in panel.columns

    assert panel["alpha_composite"].notna().sum() > 0
    assert panel["forward_return"].notna().sum() > 0


def test_alpha_panel_has_ten_factors() -> None:
    assert len(BASE_FACTOR_COLUMNS) == 10, f"Expected 10 factors, got {len(BASE_FACTOR_COLUMNS)}"


def test_factors_are_lagged_at_start() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-06-30")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC"])

    prices = generate_synthetic_prices(cfg, universe)
    panel = add_alpha_factors(prices, cfg)
    first_rows = panel.sort_values(["symbol", "date"]).groupby("symbol").head(1)

    assert first_rows["alpha_trend_021_medium_momentum"].isna().all()


def test_alpha_decay_returns_correct_shape() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2022-12-31")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE"])
    prices = generate_synthetic_prices(cfg, universe)
    panel = add_alpha_factors(prices, cfg)

    test_cols = BASE_FACTOR_COLUMNS[:2]
    horizons = [1, 5, 10]
    decay = compute_alpha_decay(panel, test_cols, horizons=horizons)

    assert len(decay) == len(test_cols) * len(horizons)
    assert set(decay.columns) >= {"alpha_name", "horizon_days", "ic"}
    # IC values should be finite floats (or NaN for short windows)
    valid_ic = decay["ic"].dropna()
    assert (valid_ic.abs() <= 1.0).all()


def test_new_equity_factors_produce_values() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-12-31")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
    prices = generate_synthetic_prices(cfg, universe)
    panel = add_alpha_factors(prices, cfg)

    new_factors = [
        "alpha_wq_007_price_to_ma_reversion",
        "alpha_wq_008_overnight_gap",
        "alpha_wq_009_volume_weighted_return",
        "alpha_wq_010_gap_quality",
    ]
    for col in new_factors:
        assert col in panel.columns, f"Missing factor: {col}"
        assert panel[col].notna().sum() > 0, f"Factor all-NaN: {col}"
