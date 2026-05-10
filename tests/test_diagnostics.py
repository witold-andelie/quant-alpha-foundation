from __future__ import annotations

from quant_alpha.backtest.diagnostics import alpha_correlation, evaluate_alpha_suite
from quant_alpha.config import ProjectConfig, Universe
from quant_alpha.features.alpha_factors import BASE_FACTOR_COLUMNS, add_alpha_factors
from quant_alpha.ingestion.yahoo import generate_synthetic_prices


def test_alpha_diagnostics_are_created_for_each_expression() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-12-31")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
    panel = add_alpha_factors(generate_synthetic_prices(cfg, universe), cfg)

    diagnostics, metrics, backtests = evaluate_alpha_suite(panel, BASE_FACTOR_COLUMNS, cfg.backtest)

    assert set(diagnostics["alpha_name"]) == set(BASE_FACTOR_COLUMNS)
    assert set(metrics["alpha_name"]) == set(BASE_FACTOR_COLUMNS)
    assert not backtests.empty
    assert diagnostics["consistency_score"].between(0, 1).all()


def test_alpha_correlation_matrix_is_square() -> None:
    cfg = ProjectConfig(start_date="2021-01-01", end_date="2021-06-30")
    universe = Universe(name="test", symbols=["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
    panel = add_alpha_factors(generate_synthetic_prices(cfg, universe), cfg)

    corr = alpha_correlation(panel, BASE_FACTOR_COLUMNS)

    assert len(corr) == len(BASE_FACTOR_COLUMNS) ** 2
