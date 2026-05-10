# dbt — Energy Alpha Marts

This dbt project transforms raw energy warehouse tables into clean, tested marts for the Second Foundation energy track. It supports both DuckDB (local) and BigQuery (cloud) targets.

## Project Structure

```
dbt_energy_alpha/
├── dbt_project.yml           Project settings and materialization defaults
├── profiles.yml              DuckDB and BigQuery connection profiles
├── models/
│   ├── sources.yml           Source declarations for energy and decay tables
│   ├── staging/
│   │   ├── stg_power_market.sql    Cleaned hourly power-market view
│   │   └── stg_energy_alphas.sql   Cleaned alpha feature view
│   └── marts/
│       ├── schema.yml                   Column-level tests
│       ├── fct_energy_backtest_daily.sql    Long-short daily returns
│       ├── fct_energy_alpha_diagnostics.sql IS/OOS metrics per energy factor
│       ├── fct_energy_market_quality.sql    Data quality check results
│       └── fct_energy_alpha_decay.sql       IC by forward hours
```

## Models

### Staging Layer (views)

| Model | Source | Description |
|---|---|---|
| `stg_power_market` | `power_market_raw` | Type-cast hourly power records |
| `stg_energy_alphas` | `power_market_features` | Alpha-augmented panel with market and timestamp |

### Mart Layer (tables)

| Model | Description |
|---|---|
| `fct_energy_backtest_daily` | Daily cross-market long-short returns and equity curve |
| `fct_energy_alpha_diagnostics` | IS/OOS IC, consistency, and robustness per energy factor |
| `fct_energy_market_quality` | Pass/fail results for all data quality checks |
| `fct_energy_alpha_decay` | IC labeled by forward hour and IC regime |

## Running dbt

```bash
cd dbt_energy_alpha

# Default target (DuckDB)
dbt build --profiles-dir .

# Cloud target (BigQuery)
ENERGY_SOURCE_SCHEMA=second_foundation_quant dbt build --profiles-dir . --target bigquery

# Single model
dbt run --select fct_energy_alpha_decay --profiles-dir .

# Docs
dbt docs generate --profiles-dir .
dbt docs serve
```

## Source Declarations

Two source groups are registered:

```yaml
- name: energy_raw     # Power market tables
- name: energy_alpha   # Decay and turnover tables
```

The schema is controlled by the `ENERGY_SOURCE_SCHEMA` environment variable, defaulting to `main` (DuckDB) or the BigQuery dataset name.

## Schema Tests

| Model | Tested Columns |
|---|---|
| `fct_energy_backtest_daily` | `market_ts`, `equity_curve` |
| `fct_energy_alpha_diagnostics` | `alpha_name`, `consistency_score` |
| `fct_energy_market_quality` | `check_name`, `passed` |

## BigQuery Integration

When `cloud.enabled: true` is set in the energy project config, the pipeline exports all tables to BigQuery. dbt then transforms them into marts using the BigQuery target profile. This enables the Streamlit dashboard to switch to BigQuery as its data backend:

```bash
STREAMLIT_DATA_BACKEND=bigquery \
GCP_PROJECT_ID=your-project \
BQ_DATASET=second_foundation_quant \
streamlit run streamlit_app/app.py
```
