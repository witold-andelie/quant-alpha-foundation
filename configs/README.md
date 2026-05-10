# Configuration

All runtime parameters are stored as YAML files in this directory. The Python package reads them via `config.py` (Pydantic models), so all values are typed and validated at startup.

## Files

| File | Purpose |
|---|---|
| `project.yaml` | Equity pipeline: dates, universe, backtest parameters |
| `second_foundation_project.yaml` | Energy pipeline: ENTSO-E, cloud export, bar interval |
| `universe.yaml` | Equity universe: ticker list, benchmarks, timezone |
| `energy_universe.yaml` | Energy universe: bidding zones, ENTSO-E EIC codes |

---

## `project.yaml` — Equity Pipeline

```yaml
project_name: quant-alpha-foundation
raw_dir: data/raw
processed_dir: data/processed
duckdb_path: data/warehouse/quant_alpha.duckdb
universe_path: configs/universe.yaml
start_date: "2021-01-01"
end_date: null          # null means today
bar_interval: 1d
data_source: synthetic  # "synthetic" or "yahoo"

backtest:
  forward_return_days: 5
  top_quantile: 0.2
  bottom_quantile: 0.2
  transaction_cost_bps: 1
  periods_per_year: 252

factor_horizons:
  momentum: 21
  reversal: 5
  volatility: 20
  breakout: 55
```

## `second_foundation_project.yaml` — Energy Pipeline

```yaml
project_name: second-foundation-energy
duckdb_path: data/warehouse/second_foundation.duckdb
universe_path: configs/energy_universe.yaml
start_date: "2023-01-01"
bar_interval: h          # hourly
data_source: synthetic   # "synthetic" or "entsoe"

entsoe:
  token_env: ENTSOE_API_KEY
  base_url: https://web-api.tp.entsoe.eu/api
  timeout_seconds: 60

cloud:
  enabled: false
  gcp_project_id: null
  gcs_bucket: null
  bigquery_dataset: second_foundation_quant
  bigquery_location: EU
  gcs_prefix: energy
  write_disposition: WRITE_TRUNCATE
```

## `universe.yaml` — Equity Universe

```yaml
name: sp500-demo
timezone: America/New_York
asset_class: equities
symbols:
  - AAPL
  - MSFT
  - ...
benchmarks:
  - SPY
```

## `energy_universe.yaml` — Energy Universe

```yaml
markets:
  - DE_LU
  - CZ
  - FR
entsoe_domains:
  DE_LU: "10Y1001A1001A82H"   # Germany-Luxembourg EIC
  CZ:    "10YCZ-CEPS-----N"   # Czech Republic EIC
  FR:    "10YFR-RTE------C"   # France EIC
```

## Overriding at Runtime

Pass `--config` to select a different config file:

```bash
quant-alpha energy-run --config configs/second_foundation_project.yaml
quant-alpha energy-run --source entsoe
```

Environment variables override `entsoe.token_env` and cloud credentials.

## Schema Reference

All config keys are validated by Pydantic models in `src/quant_alpha/config.py`. Invalid values raise a `ValidationError` at startup before any data is processed.
