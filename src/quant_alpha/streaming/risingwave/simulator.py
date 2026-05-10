"""
DuckDB-backed simulator for RisingWave materialized views.

Executes the same alpha logic as views.sql but against a local DuckDB
database — useful for unit tests and offline demos without a running
RisingWave cluster.
"""
from __future__ import annotations


import duckdb
import pandas as pd

from quant_alpha.ingestion.energy import generate_synthetic_power_market
from quant_alpha.features.energy_alpha import add_energy_alpha_features


def build_realtime_alpha_panel(
    markets: list[str],
    hours: int = 48,
    db_path: str = ":memory:",
) -> pd.DataFrame:
    """
    Simulate mv_realtime_alpha_scores locally using DuckDB window functions.
    Returns a wide DataFrame equivalent to the RisingWave materialized view.
    """
    end = pd.Timestamp.utcnow().floor("h")
    start = end - pd.Timedelta(hours=hours)
    raw = generate_synthetic_power_market(markets, start.isoformat(), end.isoformat(), freq="h")
    features = add_energy_alpha_features(raw)

    con = duckdb.connect(db_path)
    con.register("power_market_signals", features)

    result = con.execute("""
        WITH market_mean AS (
            SELECT timestamp, AVG(spot_price) AS cross_market_avg
            FROM power_market_signals
            GROUP BY timestamp
        ),
        base AS (
            SELECT
                s.timestamp,
                s.market,
                s.spot_price,
                s.residual_load,
                COALESCE(s.actual_load, s.load_forecast) - s.load_forecast AS demand_surprise,
                s.imbalance_price - s.spot_price                            AS imbalance_premium,
                s.spot_price - COALESCE(s.gas_price, 35.0)                 AS gas_spark_spread,
                s.solar_forecast / NULLIF(s.load_forecast, 0)              AS solar_penetration,
                s.spot_price / NULLIF(
                    AVG(s.spot_price) OVER (
                        PARTITION BY s.market
                        ORDER BY s.timestamp
                        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                    ), 0) - 1                                               AS momentum_6h,
                s.spot_price - m.cross_market_avg                          AS cross_market_spread
            FROM power_market_signals s
            JOIN market_mean m ON s.timestamp = m.timestamp
        )
        SELECT
            timestamp,
            market,
            spot_price,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY residual_load)    AS alpha_residual_load_rank,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY imbalance_premium) AS alpha_imbalance_premium,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY cross_market_spread) AS alpha_cross_market_spread,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY demand_surprise)   AS alpha_demand_surprise,
            1.0 - PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY solar_penetration) AS alpha_solar_penetration,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY momentum_6h)       AS alpha_momentum_6h,
            PERCENT_RANK() OVER (PARTITION BY timestamp ORDER BY gas_spark_spread)  AS alpha_gas_spark_spread
        FROM base
        ORDER BY timestamp DESC, market
    """).df()

    con.close()
    return result


def get_scarcity_alerts(
    panel: pd.DataFrame,
    threshold: float = 0.8,
) -> pd.DataFrame:
    """Return rows where residual load rank exceeds the threshold (scarcity events)."""
    alerts = panel[panel["alpha_residual_load_rank"] > threshold].copy()
    alerts["scarcity_level"] = "MEDIUM"
    alerts.loc[
        (alerts["alpha_residual_load_rank"] > 0.9) & (alerts["alpha_momentum_6h"] > 0.7),
        "scarcity_level",
    ] = "HIGH"
    return alerts.sort_values("timestamp", ascending=False).reset_index(drop=True)
