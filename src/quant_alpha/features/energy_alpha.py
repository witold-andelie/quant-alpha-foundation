from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EnergyAlphaDefinition:
    name: str
    expression: str
    family: str
    hypothesis: str


ENERGY_ALPHA_REGISTRY = [
    EnergyAlphaDefinition(
        name="alpha_energy_residual_load_shock",
        expression="zscore(residual_load, 168)",
        family="scarcity",
        hypothesis="Residual load spikes proxy short-term scarcity in power markets.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_wind_forecast_error",
        expression="-zscore(delta(wind_forecast, 24), 168)",
        family="renewables",
        hypothesis="Sharp wind forecast shifts proxy renewable supply surprise.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_imbalance_premium",
        expression="zscore(imbalance_price - spot_price, 72)",
        family="imbalance",
        hypothesis="Balancing premium captures system stress and repricing pressure.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_cross_market_spread",
        expression="zscore(spot_price - mean_cross_market_spot, 168)",
        family="arbitrage",
        hypothesis="Markets priced far above the cross-European average face arbitrage convergence pressure; spread reversion is the primary force.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_demand_surprise",
        expression="zscore(actual_load - load_forecast, 72)",
        family="fundamental",
        hypothesis="Positive demand forecast errors reveal unexpected consumption, tightening the dispatch stack and lifting spot prices.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_solar_penetration",
        expression="-zscore(solar_forecast / load_forecast, 168)",
        family="renewables",
        hypothesis="High solar penetration relative to load creates mid-day price depression; markets with high penetration ratios show negative price pressure.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_price_momentum_6h",
        expression="rank(spot_price / delay(spot_price, 6) - 1)",
        family="momentum",
        hypothesis="Power dispatch inertia creates 6-hour momentum; high-cost peakers take time to ramp, sustaining price direction.",
    ),
    EnergyAlphaDefinition(
        name="alpha_energy_gas_spark_spread",
        expression="zscore(spot_price - gas_price * 2.0, 168)",
        family="fundamental",
        hypothesis="When spot power prices exceed the gas-implied spark spread, additional scarcity or storage premium exists; persistent spread predicts mean reversion.",
    ),
]

ENERGY_ALPHA_EXPRESSIONS = {alpha.name: alpha.expression for alpha in ENERGY_ALPHA_REGISTRY}


def _zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std.replace(0, pd.NA)


def add_energy_alpha_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.sort_values(["market", "timestamp"]).copy()
    # Real API payloads can include nullable/object numerics; normalize before math.
    numeric_cols = [
        "spot_price",
        "load_forecast",
        "actual_load",
        "wind_forecast",
        "solar_forecast",
        "residual_load",
        "imbalance_price",
        "gas_price",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cross-market mean spot price at each timestamp (used by arbitrage alpha)
    df["cross_market_spot_mean"] = df.groupby("timestamp")["spot_price"].transform("mean")

    # Pre-compute difference columns so all factors can use groupby.transform
    # (transform always returns a same-shaped Series, avoiding the single-group 2D issue)
    df["_imbalance_diff"] = df["imbalance_price"] - df["spot_price"]
    df["_spread_diff"] = df["spot_price"] - df["cross_market_spot_mean"]
    df["_solar_pen"] = df["solar_forecast"] / df["load_forecast"].clip(lower=1.0)

    grouped = df.groupby("market", group_keys=False)

    # --- Original three factors ---
    df["alpha_energy_residual_load_shock"] = grouped["residual_load"].transform(
        lambda s: _zscore(s, 168).shift(1)
    )
    df["alpha_energy_wind_forecast_error"] = grouped["wind_forecast"].transform(
        lambda s: _zscore(s.diff(24), 168).shift(1) * -1.0
    )
    df["alpha_energy_imbalance_premium"] = grouped["_imbalance_diff"].transform(
        lambda s: _zscore(s, 72).shift(1)
    )

    # --- Five new WorldQuant-inspired energy factors ---

    # 1. Cross-market spread: each market vs European average
    df["alpha_energy_cross_market_spread"] = grouped["_spread_diff"].transform(
        lambda s: _zscore(s, 168).shift(1)
    )

    # 2. Demand surprise: actual load minus forecast
    if "actual_load" in df.columns:
        df["_demand_surprise"] = df["actual_load"] - df["load_forecast"]
        df["alpha_energy_demand_surprise"] = grouped["_demand_surprise"].transform(
            lambda s: _zscore(s, 72).shift(1)
        )
        df.drop(columns=["_demand_surprise"], inplace=True)
    else:
        df["alpha_energy_demand_surprise"] = grouped["load_forecast"].transform(
            lambda s: _zscore(s.diff(6), 72).shift(1)
        )

    # 3. Solar penetration (negative: more solar → lower prices)
    df["alpha_energy_solar_penetration"] = grouped["_solar_pen"].transform(
        lambda s: -_zscore(s, 168).shift(1)
    )

    # 4. 6-hour price momentum (dispatch inertia)
    df["alpha_energy_price_momentum_6h"] = grouped["spot_price"].transform(
        lambda s: (s / s.shift(6).replace(0, np.nan) - 1).shift(1)
    )

    # 5. Gas–power spark spread (heat rate = 2.0 as simplified thermal equivalent)
    if "gas_price" in df.columns:
        df["_gas_spark"] = df["spot_price"] - df["gas_price"] * 2.0
        df["alpha_energy_gas_spark_spread"] = grouped["_gas_spark"].transform(
            lambda s: _zscore(s, 168).shift(1)
        )
        df.drop(columns=["_gas_spark"], inplace=True)
    else:
        df["alpha_energy_gas_spark_spread"] = np.nan

    df.drop(columns=["_imbalance_diff", "_spread_diff", "_solar_pen"], inplace=True)

    return df


def energy_alpha_registry_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "alpha_name": alpha.name,
                "expression": alpha.expression,
                "family": alpha.family,
                "hypothesis": alpha.hypothesis,
                "expected_direction": 1,
            }
            for alpha in ENERGY_ALPHA_REGISTRY
        ]
    )
