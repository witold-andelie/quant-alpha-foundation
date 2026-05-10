-- =============================================================================
-- RisingWave Streaming SQL — Second Foundation Energy Alpha
-- Workshop 2 pattern: declarative materialized views over a Kafka/Redpanda source
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Source: connect to Redpanda topic (power_market_signals)
-- ---------------------------------------------------------------------------
CREATE SOURCE IF NOT EXISTS power_market_source (
    timestamp       TIMESTAMPTZ,
    market          VARCHAR,
    spot_price      DOUBLE,
    load_forecast   DOUBLE,
    actual_load     DOUBLE,
    wind_forecast   DOUBLE,
    solar_forecast  DOUBLE,
    residual_load   DOUBLE,
    imbalance_price DOUBLE,
    gas_price       DOUBLE
)
WITH (
    connector     = 'kafka',
    topic         = 'power_market_signals',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
)
FORMAT PLAIN ENCODE JSON;


-- ---------------------------------------------------------------------------
-- 2. Materialized view: 1-hour tumbling window aggregates per market
--    Used for: rolling spot price stats, residual load level detection
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_energy_hourly_window AS
SELECT
    TUMBLE_START(timestamp, INTERVAL '1 hour')  AS window_start,
    TUMBLE_END(timestamp, INTERVAL '1 hour')    AS window_end,
    market,
    AVG(spot_price)                             AS avg_spot_price,
    MAX(spot_price)                             AS max_spot_price,
    MIN(spot_price)                             AS min_spot_price,
    STDDEV_SAMP(spot_price)                     AS std_spot_price,
    AVG(residual_load)                          AS avg_residual_load,
    AVG(imbalance_price)                        AS avg_imbalance_price,
    AVG(gas_price)                              AS avg_gas_price,
    COUNT(*)                                    AS record_count
FROM TUMBLE(power_market_source, timestamp, INTERVAL '1 hour')
GROUP BY window_start, window_end, market;


-- ---------------------------------------------------------------------------
-- 3. Materialized view: 6-hour sliding window — momentum signal
--    alpha_energy_price_momentum_6h = spot_price / avg(spot_price, 6h) - 1
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_energy_momentum_6h AS
SELECT
    timestamp,
    market,
    spot_price,
    AVG(spot_price) OVER (
        PARTITION BY market
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND CURRENT ROW
    )                                                   AS avg_spot_6h,
    spot_price / NULLIF(AVG(spot_price) OVER (
        PARTITION BY market
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND CURRENT ROW
    ), 0) - 1                                           AS momentum_6h,
    residual_load,
    imbalance_price - spot_price                        AS imbalance_premium,
    spot_price - COALESCE(gas_price, 35.0)              AS gas_spark_spread,
    CASE WHEN actual_load IS NOT NULL
         THEN actual_load - load_forecast
         ELSE NULL
    END                                                  AS demand_surprise,
    COALESCE(solar_forecast, 0.0) / NULLIF(load_forecast, 0) AS solar_penetration
FROM power_market_source;


-- ---------------------------------------------------------------------------
-- 4. Materialized view: cross-market spread (real-time alpha signal)
--    Compares each market's spot price to the cross-market mean
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cross_market_spread AS
WITH market_mean AS (
    SELECT
        timestamp,
        AVG(spot_price) AS cross_market_avg
    FROM power_market_source
    GROUP BY timestamp
)
SELECT
    s.timestamp,
    s.market,
    s.spot_price,
    m.cross_market_avg,
    s.spot_price - m.cross_market_avg AS cross_market_spread
FROM power_market_source s
JOIN market_mean m ON s.timestamp = m.timestamp;


-- ---------------------------------------------------------------------------
-- 5. Materialized view: real-time alpha scores (composite signal)
--    Emits one row per (timestamp, market) with all 8 alpha factor values
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_realtime_alpha_scores AS
SELECT
    mom.timestamp,
    mom.market,
    mom.spot_price,

    -- Alpha 1: residual load rank (supply tightness)
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.residual_load
    ) AS alpha_residual_load_rank,

    -- Alpha 2: imbalance premium
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.imbalance_premium
    ) AS alpha_imbalance_premium,

    -- Alpha 3: cross-market spread
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY cms.cross_market_spread
    ) AS alpha_cross_market_spread,

    -- Alpha 4: demand surprise
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.demand_surprise
    ) AS alpha_demand_surprise,

    -- Alpha 5: solar penetration (inverted — high solar = lower prices)
    1.0 - PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.solar_penetration
    ) AS alpha_solar_penetration,

    -- Alpha 6: 6-hour momentum
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.momentum_6h
    ) AS alpha_momentum_6h,

    -- Alpha 7: gas-spark spread
    PERCENT_RANK() OVER (
        PARTITION BY mom.timestamp ORDER BY mom.gas_spark_spread
    ) AS alpha_gas_spark_spread

FROM mv_energy_momentum_6h AS mom
JOIN mv_cross_market_spread AS cms
  ON mom.timestamp = cms.timestamp AND mom.market = cms.market;


-- ---------------------------------------------------------------------------
-- 6. Materialized view: alert sink — high-scarcity events
--    Emits a row whenever residual load rank > 0.9 (top 10% scarcity)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_scarcity_alerts AS
SELECT
    timestamp,
    market,
    spot_price,
    alpha_residual_load_rank,
    alpha_momentum_6h,
    CASE
        WHEN alpha_residual_load_rank > 0.9 AND alpha_momentum_6h > 0.7 THEN 'HIGH'
        WHEN alpha_residual_load_rank > 0.8 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS scarcity_level
FROM mv_realtime_alpha_scores
WHERE alpha_residual_load_rank > 0.8;
