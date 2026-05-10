# dbt — Equity Alpha Marts

This dbt project transforms raw warehouse tables into clean, tested analytics marts for the equity track. It covers the zoomcamp Module 4 (Analytics Engineering) knowledge points: data modeling, sources, staging, mart layers, schema tests, and documentation.

## Project Structure

```
dbt_quant_alpha/
├── dbt_project.yml           Project name, model materialization defaults
├── profiles.yml              DuckDB connection (local) and BigQuery (cloud)
├── models/
│   ├── sources.yml           Source declarations (raw warehouse tables)
│   ├── staging/
│   │   ├── stg_prices.sql        Cleaned price view
│   │   └── stg_factor_panel.sql  Cleaned factor view
│   └── marts/
│       ├── schema.yml            Column-level tests
│       ├── fct_alpha_diagnostics.sql   IS/OOS metrics per factor
│       ├── fct_alpha_panel.sql         Factor panel with signal date
│       ├── fct_backtest_daily.sql      Daily portfolio returns
│       ├── fct_alpha_decay.sql         IC by forward horizon
│       └── fct_alpha_turnover.sql      Per-factor turnover (if added)
```

## Models

### Staging Layer (views)

| Model | Source Table | Description |
|---|---|---|
| `stg_prices` | `raw_prices` | Type-cast and renamed price columns |
| `stg_factor_panel` | `factor_panel` | Normalized factor panel with date casting |

### Mart Layer (tables)

| Model | Description |
|---|---|
| `fct_alpha_diagnostics` | IS/OOS IC, consistency score, and robustness score per factor |
| `fct_alpha_panel` | Factor signals joined with prices, ready for dashboard consumption |
| `fct_backtest_daily` | Long-short daily returns and equity curve |
| `fct_alpha_decay` | IC labeled by forward horizon and IC regime (positive/negative/flat) |

## Running dbt

```bash
cd dbt_quant_alpha

# Build all models and run tests
dbt build --profiles-dir .

# Build specific model
dbt run --select fct_alpha_diagnostics --profiles-dir .

# Run tests only
dbt test --profiles-dir .

# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve
```

## Schema Tests

All mart tables include `not_null` tests on critical columns:

```yaml
# schema.yml
- name: fct_alpha_diagnostics
  columns:
    - name: alpha_name
      tests: [not_null]
    - name: consistency_score
      tests: [not_null]
```

## Profiles

### DuckDB (default)

```yaml
quant_alpha:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: ../data/warehouse/quant_alpha.duckdb
      threads: 4
```

### BigQuery (cloud)

```bash
cd dbt_quant_alpha
dbt build --profiles-dir . --target bigquery
```

Set `GCP_PROJECT_ID` and `BQ_DATASET` environment variables before running against BigQuery.

## Adding a New Model

1. Create a `.sql` file in `marts/` using `{{ source(...) }}` or `{{ ref(...) }}`.
2. Add the model name to `schema.yml` with at least one `not_null` test on the primary key.
3. Run `dbt build --select <new_model>` to verify.
4. The model will be picked up by the dashboard if the table name is registered in `streamlit_app/app.py`.
