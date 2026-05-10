from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd


def _seed(name: str) -> int:
    return int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)


def generate_synthetic_power_market(
    markets: list[str],
    start: str,
    end: str,
    freq: str = "h",
) -> pd.DataFrame:
    timestamps = pd.date_range(start, end, freq=freq)
    frames: list[pd.DataFrame] = []

    for market in markets:
        rng = np.random.default_rng(_seed(market))
        hour = timestamps.hour.to_numpy()
        day_of_year = timestamps.dayofyear.to_numpy()

        load = 55 + 12 * np.sin((hour - 7) / 24 * 2 * np.pi) + rng.normal(0, 3, len(timestamps))
        wind = 18 + 8 * np.sin(day_of_year / 365 * 2 * np.pi) + rng.normal(0, 5, len(timestamps))
        solar = np.maximum(0, 14 * np.sin((hour - 6) / 12 * np.pi)) + rng.normal(0, 1, len(timestamps))
        residual_load = load - wind - solar
        scarcity = np.maximum(residual_load - np.quantile(residual_load, 0.8), 0)
        spot = 45 + 1.4 * residual_load + 2.0 * scarcity + rng.normal(0, 8, len(timestamps))
        spot = np.maximum(spot, 5)
        imbalance = np.maximum(spot + rng.normal(0, 15, len(timestamps)), 1)

        # Gas price: seasonal cycle + mean-reverting noise (€/MWh thermal equivalent)
        gas_base = 35 + 10 * np.sin((day_of_year - 60) / 365 * 2 * np.pi)
        gas_price = gas_base + rng.normal(0, 4, len(timestamps))
        gas_price = np.maximum(gas_price, 10)

        # Actual load = forecast + measurement error (used for demand-surprise alpha)
        actual_load = load + rng.normal(0, 2.5, len(timestamps))

        frames.append(
            pd.DataFrame(
                {
                    "timestamp": timestamps,
                    "market": market,
                    "spot_price": spot,
                    "load_forecast": load,
                    "actual_load": actual_load,
                    "wind_forecast": wind,
                    "solar_forecast": solar,
                    "residual_load": residual_load,
                    "imbalance_price": imbalance,
                    "gas_price": gas_price,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)
