# Architecture

This project mirrors the Data Engineering Zoomcamp capstone flow while replacing taxi data with market data and replacing operational dashboards with alpha research outputs.

The strategic priority is now the Second Foundation-inspired energy track. The US equities track remains in the repo as a cleaner portfolio demo and a baseline for alpha-expression research.

## Layers

1. Ingestion
   - Current: Yahoo Finance daily OHLCV, deterministic synthetic prices, and synthetic hourly power-market data.
   - Next: Polygon, Databento, EPEX, ENTSO-E, broker feeds, and forecast vendors.

2. Data lake
   - Current: local Parquet under `data/raw` and `data/processed`.
   - Next: GCS/S3/Azure Blob with partitioning by dataset, date, and symbol.

3. Warehouse
   - Current: DuckDB for local reproducibility.
   - Next: BigQuery or Snowflake with partitioned and clustered tables.

4. Transformation
   - Current: Python for factor math, dbt for warehouse marts, Spark batch features for energy datasets.
   - Next: larger real universes and cross-asset history.

5. Orchestration
   - Current: Kestra daily equity flow and hourly energy flow.
   - Next: scheduled cloud deployment with backfills and stronger data quality gates.

6. Research application
   - Current: Streamlit factor and backtest dashboard.
   - Next: research notebook templates, experiment tracking, model registry, and risk reports.

## Production Boundary

The skeleton is for research and portfolio demonstration. It does not place orders, manage broker state, or provide investment advice.
