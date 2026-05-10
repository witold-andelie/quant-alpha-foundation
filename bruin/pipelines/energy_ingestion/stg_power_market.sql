/* @asset
name: stg_power_market
type: duckdb.table
connection: duckdb_energy
description: Cleaned hourly power-market data with demand-surprise and gas-spark computed.
depends:
  - raw_power_market
owner: energy-research
tags:
  - energy
  - staging
columns:
  - name: timestamp
    checks: [not_null]
  - name: market
    checks: [not_null]
  - name: spot_price
    checks: [not_null]
  - name: demand_surprise
    description: actual_load − load_forecast (GW)
  - name: gas_spark_spread
    description: spot_price − gas_price (€/MWh proxy for clean spark spread)
*/

SELECT
    timestamp,
    market,
    spot_price,
    load_forecast,
    COALESCE(actual_load, load_forecast)                   AS actual_load,
    wind_forecast,
    solar_forecast,
    residual_load,
    imbalance_price,
    gas_price,
    COALESCE(actual_load, load_forecast) - load_forecast   AS demand_surprise,
    spot_price - COALESCE(gas_price, 35.0)                 AS gas_spark_spread
FROM power_market_raw
WHERE
    spot_price IS NOT NULL
    AND spot_price BETWEEN -500 AND 3000
QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, market ORDER BY timestamp) = 1
ORDER BY market, timestamp
