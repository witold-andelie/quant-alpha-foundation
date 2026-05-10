# Data Platform — M5 Data Platforms

This module provides data contracts, data quality checks, and the Bruin-style asset graph execution engine. It covers the zoomcamp Module 5 (Data Platforms) knowledge points: declarative asset definitions, lineage tracking, quality enforcement, and reproducible pipeline structure.

## Modules

| File | Purpose |
|---|---|
| `contracts.py` | Immutable `DatasetContract` dataclasses — equity and energy schema registry |
| `quality.py` | Automated quality check functions (primary key, non-null, value ranges) |
| `bruin_graph.py` | Bruin-style asset graph: topological execution, lineage traversal, dry-run |

See also: [`bruin/`](../../../../bruin/README.md) — the asset YAML definitions that feed `bruin_graph.py`.

---

## Bruin Asset Graph (`bruin_graph.py`)

`AssetGraph` loads all `.asset.yml` and SQL `@asset` blocks from the `bruin/` directory and exposes:

```python
from pathlib import Path
from quant_alpha.platform.bruin_graph import AssetGraph

graph = AssetGraph(Path("bruin"))

# Show full lineage DAG
print(graph.lineage_report())

# Traversal
graph.upstream("fct_alpha_diagnostics")   # → ['fct_equity_alpha_panel', 'stg_equity_ohlcv', ...]
graph.downstream("raw_equity_ohlcv")      # → ['stg_equity_ohlcv', 'fct_equity_alpha_panel', ...]

# Execute in topological order
graph.run(targets=["fct_equity_alpha_panel"])   # runs only target + upstream
graph.run(dry_run=True)                         # print plan, do nothing
```

CLI shortcuts:

```bash
quant-alpha bruin-lineage                          # full DAG
quant-alpha bruin-lineage --asset stg_power_market # upstream/downstream for one asset
quant-alpha bruin-run --dry-run                    # execution plan
quant-alpha bruin-run --targets fct_alpha_diagnostics
```

---

## Data Contracts (`contracts.py`)

A `DatasetContract` is a machine-readable commitment about a dataset's structure and expectations. Contracts serve as the interface between data producers (pipelines) and consumers (models, dashboards, downstream jobs).

```python
@dataclass(frozen=True)
class DatasetContract:
    name: str
    grain: str               # e.g. "hourly x market"
    owner: str
    primary_keys: tuple[str, ...]
    freshness_expectation: str  # e.g. "hourly", "daily"
```

### Registered Contracts

| Dataset | Grain | Primary Keys | Freshness |
|---|---|---|---|
| `power_market_raw` | hourly × market | `(timestamp, market)` | hourly |
| `energy_alpha_features` | hourly × market | `(timestamp, market)` | hourly |

Contracts are not yet enforced programmatically at runtime, but they document the intent and serve as the source of truth for dbt schema tests.

---

## Quality Checks (`quality.py`)

`run_energy_quality_checks(frame)` executes a suite of assertions over the raw power-market data and returns a DataFrame of pass/fail results. These results are written to `power_market_quality` in DuckDB and surfaced in the `fct_energy_market_quality` dbt mart.

### Check Suite

| Check | Description | Failure Condition |
|---|---|---|
| `no_null_spot_price` | Spot price must never be null | Any null in `spot_price` |
| `spot_price_positive` | Power prices must be positive | Any `spot_price <= 0` |
| `residual_load_range` | Residual load must be plausible | Any value outside [−50, 120] GW |
| `imbalance_price_positive` | Balancing price must be positive | Any `imbalance_price <= 0` |
| `markets_present` | At least one market must be present | `market` column empty or missing |
| `hourly_completeness` | No large gaps in the hourly series | Gaps > 2 hours in any market |

### Running Quality Checks

Quality checks are run automatically inside `run_energy_pipeline()`. They can also be run standalone:

```python
from quant_alpha.ingestion.energy import generate_synthetic_power_market
from quant_alpha.platform.quality import run_energy_quality_checks

frame = generate_synthetic_power_market(["DE_LU", "CZ"], "2024-01-01", "2024-02-01")
quality = run_energy_quality_checks(frame)
print(quality)
```

### Extending Quality Checks

Add new checks inside `run_energy_quality_checks()` following the existing pattern: each check appends a dict with `check_name`, `passed` (bool), `detail` (string), and `rows_checked` (int) to the results list.

---

## Connection to dbt

The quality results are exposed as a dbt source and materialized as `fct_energy_market_quality`. This allows the dashboard and downstream models to query quality status alongside business metrics.

```sql
-- dbt model: fct_energy_market_quality.sql
select check_name, passed, detail
from {{ source('energy_raw', 'power_market_quality') }}
```
