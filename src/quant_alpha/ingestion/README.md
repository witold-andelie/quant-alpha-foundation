# Data Ingestion

This module handles all data collection for the platform. It provides two tracks: US equity prices and European power-market data.

## Sources

| Source | Module | Mode | Frequency |
|---|---|---|---|
| Yahoo Finance | `yahoo.py` | Live / Offline | Daily OHLCV |
| ENTSO-E Transparency Platform | `entsoe.py` | Live | Hourly power-market |
| Synthetic generator | `yahoo.py`, `energy.py` | Offline only | Configurable |
| dlt energy pipeline | `dlt_energy.py` | Live / Offline | Hourly, incremental |
| dlt equity pipeline | `dlt_equity.py` | Live / Offline | Daily, incremental |

---

## dlt-Based Ingestion (Workshop Module)

`dlt_energy.py` and `dlt_equity.py` implement the DataTalksClub Workshop pattern: declarative resource definitions, automatic schema inference, and stateful incremental loading.

### Key dlt Features Used

| Feature | How We Use It |
|---|---|
| `@dlt.source` | Groups related resources under a single pipeline |
| `@dlt.resource` | Declares each dataset with write disposition and primary keys |
| `dlt.sources.incremental` | Tracks last loaded timestamp/date; only new records are fetched on subsequent runs |
| Column hints | Explicit type declarations (`data_type`, `nullable`) enforce schema before load |
| DuckDB destination | Writes to a named schema inside our warehouse DuckDB file |
| Pipeline state | dlt stores cursor state in `_dlt_pipeline_state`; survives process restarts |

### Incremental Load Behavior

```
First run (2024-01-01 → 2024-01-08):   loads 169 rows × 2 markets
Second run (same range):                loads 0 rows  (cursor already at 2024-01-08)
Third run (extended to 2024-02-01):     loads only rows after 2024-01-08
```

### Running the dlt Pipelines

```bash
# Energy (incremental, defaults to last 1 year)
quant-alpha dlt-energy --start 2023-01-01 --end 2024-12-31

# Equity (incremental, uses project.yaml universe)
quant-alpha dlt-equity --offline

# Direct Python
python -m quant_alpha.ingestion.dlt_energy
python -m quant_alpha.ingestion.dlt_equity
```

### dlt Schema Location in DuckDB

dlt writes to a separate schema inside the same DuckDB file:

```
data/warehouse/second_foundation.duckdb
  └── dlt_energy_raw
        ├── power_market_raw        ← actual data
        ├── _dlt_loads              ← load history
        ├── _dlt_pipeline_state     ← incremental cursor state
        └── _dlt_version            ← dlt version metadata

data/warehouse/quant_alpha.duckdb
  └── dlt_equity_raw
        ├── equity_ohlcv
        └── _dlt_*
```

The main pipeline continues to write to the `main` schema. dlt tables serve as a reproducible, auditable raw layer.

---

## Equity Ingestion (`yahoo.py`)

Downloads daily OHLCV data for the configured universe using `yfinance`. Falls back to a deterministic synthetic generator when `--offline` is passed or no network is available.

**Synthetic price model**

Each symbol is seeded from a hash of its ticker string, producing reproducible GBM paths:

```
drift  ~ Normal(0.0003, 0.0001)
vol    ~ Uniform(0.012, 0.026)
close  = 100 * exp(cumsum(Normal(drift, vol)))
volume ~ Uniform(2M, 80M)
```

**Run offline**

```bash
quant-alpha run --offline
```

**Run with live data**

```bash
quant-alpha run
```

## Energy Ingestion (`energy.py`, `entsoe.py`)

### Synthetic power-market generator

Produces hourly records for any list of European bidding zones. Each market is seeded from its name hash. The model includes:

| Column | Description |
|---|---|
| `spot_price` | Day-ahead spot (€/MWh), scarcity-adjusted |
| `load_forecast` | Demand forecast (GW) with intraday and seasonal profile |
| `actual_load` | Forecast + measurement noise (used for demand-surprise alpha) |
| `wind_forecast` | Wind generation (GW), seasonal cycle |
| `solar_forecast` | Solar generation (GW), zero at night |
| `residual_load` | load − wind − solar |
| `imbalance_price` | Balancing market price (€/MWh) |
| `gas_price` | Gas price thermal equivalent (€/MWh), seasonal cycle |

**Run synthetic**

```bash
quant-alpha energy-run
```

### ENTSO-E client (`entsoe.py`)

Fetches actual transparency data via the ENTSO-E REST API. Requires a free token from [transparency.entsoe.eu](https://transparency.entsoe.eu).

```bash
export ENTSOE_API_KEY=your-token
quant-alpha energy-run --source entsoe
```

EIC domain codes for the configured bidding zones are specified in `configs/energy_universe.yaml`.

## Output

Raw data is written to:

- `data/raw/prices.parquet` — equity OHLCV
- `data/raw/power_market.parquet` — energy hourly records
- DuckDB tables `raw_prices` and `power_market_raw`

## Adding a New Source

1. Create a new file in this directory (e.g., `polygon.py`).
2. Return a DataFrame with the same schema as `PRICE_COLUMNS` (equity) or the energy schema.
3. Wire the new source into `pipeline.py` or `pipeline_energy.py` via the `data_source` config key.
4. Add a test in `tests/test_ingestion.py`.
