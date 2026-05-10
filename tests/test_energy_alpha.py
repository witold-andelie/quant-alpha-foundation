from __future__ import annotations

from quant_alpha.backtest.alpha_decay import compute_energy_alpha_decay
from quant_alpha.features.energy_alpha import ENERGY_ALPHA_EXPRESSIONS, add_energy_alpha_features
from quant_alpha.ingestion.energy import generate_synthetic_power_market


def test_energy_alpha_module_produces_expression_columns() -> None:
    market = generate_synthetic_power_market(["DE_LU", "CZ"], "2024-01-01", "2024-02-01")
    panel = add_energy_alpha_features(market)

    for column in ENERGY_ALPHA_EXPRESSIONS:
        assert column in panel.columns

    assert panel[list(ENERGY_ALPHA_EXPRESSIONS)].notna().sum().sum() > 0


def test_energy_alpha_registry_has_eight_factors() -> None:
    from quant_alpha.features.energy_alpha import ENERGY_ALPHA_REGISTRY
    assert len(ENERGY_ALPHA_REGISTRY) == 8, f"Expected 8, got {len(ENERGY_ALPHA_REGISTRY)}"


def test_new_energy_factors_produce_values() -> None:
    market = generate_synthetic_power_market(["DE_LU", "CZ", "FR"], "2024-01-01", "2024-04-01")
    panel = add_energy_alpha_features(market)

    new_factors = [
        "alpha_energy_cross_market_spread",
        "alpha_energy_demand_surprise",
        "alpha_energy_solar_penetration",
        "alpha_energy_price_momentum_6h",
        "alpha_energy_gas_spark_spread",
    ]
    for col in new_factors:
        assert col in panel.columns, f"Missing energy factor: {col}"
        assert panel[col].notna().sum() > 0, f"Energy factor all-NaN: {col}"


def test_synthetic_power_market_has_gas_and_actual_load() -> None:
    market = generate_synthetic_power_market(["DE_LU"], "2024-01-01", "2024-01-08")
    assert "gas_price" in market.columns
    assert "actual_load" in market.columns
    assert (market["gas_price"] > 0).all()


def test_energy_alpha_decay_shape() -> None:
    market = generate_synthetic_power_market(["DE_LU", "CZ"], "2023-01-01", "2024-01-01")
    panel = add_energy_alpha_features(market)
    alpha_cols = [
        "alpha_energy_residual_load_shock",
        "alpha_energy_imbalance_premium",
    ]
    horizons = [1, 6, 12]
    decay = compute_energy_alpha_decay(panel, alpha_cols, horizons=horizons)

    assert len(decay) == len(alpha_cols) * len(horizons)
    assert set(decay.columns) >= {"alpha_name", "horizon_hours", "ic"}
