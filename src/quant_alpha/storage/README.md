# Storage

This module manages persistence for the platform. It supports two backends: DuckDB for local development and GCS + BigQuery for cloud deployment.

## DuckDB (`duckdb.py`)

DuckDB is the primary local warehouse. All pipeline outputs land here first.

### Functions

| Function | Description |
|---|---|
| `write_table(db_path, table_name, frame)` | Create or replace a table from a DataFrame |
| `write_metrics(db_path, metrics, table_name)` | Serialize a metrics dict as a single-row table |
| `table_exists(db_path, table_name)` | Check table presence using `information_schema` (read-only connection) |

### Key Tables

**Equity warehouse** (`data/warehouse/quant_alpha.duckdb`)

| Table | Description |
|---|---|
| `raw_prices` | Daily OHLCV for the equity universe |
| `factor_panel` | All alpha factors aligned with prices |
| `backtest_daily` | Daily portfolio returns and equity curve |
| `backtest_metrics` | Aggregate performance metrics |
| `alpha_diagnostics` | IS/OOS IC, consistency, and robustness per factor |
| `alpha_correlations` | Pairwise factor Spearman correlation |
| `alpha_value_added` | Composite vs best single-factor comparison |
| `alpha_decay` | IC by forward horizon (decay curve) |
| `alpha_walk_forward` | Rolling OOS IC per factor and window |
| `alpha_turnover` | Mean and median daily turnover per factor |

**Energy warehouse** (`data/warehouse/second_foundation.duckdb`)

| Table | Description |
|---|---|
| `power_market_raw` | Raw hourly power-market records |
| `power_market_features` | Alpha-augmented energy panel |
| `power_market_quality` | Data quality check results |
| `energy_alpha_registry` | Energy factor definitions |
| `energy_backtest_daily` | Energy long-short daily returns |
| `energy_backtest_metrics` | Energy aggregate metrics |
| `energy_alpha_diagnostics` | IS/OOS metrics per energy factor |
| `energy_alpha_decay` | IC by forward hours |
| `energy_alpha_turnover` | Per-factor turnover |
| `live_energy_signals` | Real-time Redpanda feed (or demo signals) |

### Querying with DuckDB CLI

```bash
duckdb data/warehouse/quant_alpha.duckdb
# then:
SELECT alpha_name, oos_ic_mean, consistency_score
FROM alpha_diagnostics
ORDER BY consistency_score DESC;
```

---

## GCP Cloud Export (`gcp.py`)

`export_frames_to_gcs_bigquery()` writes each DataFrame to a Parquet file in GCS and then loads it into a BigQuery table.

**Enable cloud export** (`configs/second_foundation_project.yaml`):

```yaml
cloud:
  enabled: true
  gcp_project_id: your-project
  gcs_bucket: your-bucket
  bigquery_dataset: second_foundation_quant
  bigquery_location: EU
  gcs_prefix: energy
  write_disposition: WRITE_TRUNCATE
```

**Authentication**

Set the application default credentials before running:

```bash
gcloud auth application-default login
```

Or use a service account key exported as `GOOGLE_APPLICATION_CREDENTIALS`.

**GCS layout**

```
gs://<bucket>/energy/<table_name>/<table_name>.parquet
```

**BigQuery table naming**

```
<project_id>.<dataset>.<table_name>
```

The `WRITE_TRUNCATE` disposition replaces the full table on each run. Use `WRITE_APPEND` for incremental loads.

---

## Switching Backends

The Streamlit dashboard reads from whichever backend is configured via the `STREAMLIT_DATA_BACKEND` environment variable:

```bash
# Local DuckDB (default)
streamlit run streamlit_app/app.py

# BigQuery
STREAMLIT_DATA_BACKEND=bigquery GCP_PROJECT_ID=your-project streamlit run streamlit_app/app.py
```
