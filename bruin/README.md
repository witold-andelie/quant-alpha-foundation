# Bruin Asset Graph — M5 Data Platforms

This directory implements the [Bruin](https://bruin-data.github.io/bruin/) data platform pattern: declarative asset definitions, automatic lineage tracking, and dependency-aware execution.

## What Bruin Adds

| Capability | Without Bruin | With Bruin |
|---|---|---|
| Asset lineage | Implicit (code wiring) | Explicit `depends:` graph |
| Schema contracts | Runtime assertions | Declarative column specs in YAML |
| Data quality | Ad-hoc `assert` | `custom_checks` block per asset |
| Execution order | Manually scripted | Topological sort, automatic |
| Environment routing | Hard-coded paths | Named connections in `.bruin.yml` |

---

## Directory Structure

```
bruin/
├── .bruin.yml                          ← project config (connections, environments)
└── pipelines/
    ├── equity_ingestion/
    │   ├── raw_equity_ohlcv.asset.yml  ← Python asset: Yahoo Finance / synthetic
    │   ├── run_equity_ingestion.py     ← runner script called by the asset
    │   └── stg_equity_ohlcv.sql        ← SQL asset: clean + compute ret_1d
    ├── energy_ingestion/
    │   ├── raw_power_market.asset.yml  ← Python asset: ENTSO-E / synthetic
    │   ├── run_energy_ingestion.py     ← runner script
    │   └── stg_power_market.sql        ← SQL asset: demand surprise, gas-spark
    ├── alpha_research/
    │   ├── fct_equity_alpha_panel.asset.yml  ← wide alpha factor table
    │   ├── fct_energy_alpha_panel.asset.yml  ← energy alpha panel
    │   └── fct_alpha_diagnostics.asset.yml   ← four-gate evaluation results
    └── reporting/
        └── rpt_backtest_summary.sql    ← unified equity + energy P&L report
```

---

## Asset Lineage

```
raw_equity_ohlcv  ──►  stg_equity_ohlcv  ──►  fct_equity_alpha_panel  ──►  fct_alpha_diagnostics
                                                                                      │
raw_power_market  ──►  stg_power_market  ──►  fct_energy_alpha_panel               │
                                                                                      ▼
                                                                         rpt_backtest_summary
```

---

## Asset Types

### Python assets (`.asset.yml` with `type: python`)

Declare the shape of the output table, quality checks, and point to a Python runner:

```yaml
name: raw_equity_ohlcv
type: python
connection: duckdb_local
depends: []
columns:
  - name: date
    checks: [not_null]
  - name: close
    checks: [not_null, positive]
custom_checks:
  - name: primary_key_unique
    query: SELECT count(*) FROM (...) HAVING n > 1
    value: 0
run:
  type: python
  file: run_equity_ingestion.py
```

### SQL assets (inline `@asset` frontmatter)

Embed the asset definition inside a SQL comment block:

```sql
/* @asset
name: stg_equity_ohlcv
type: duckdb.table
depends:
  - raw_equity_ohlcv
*/

SELECT date, symbol, ln(adj_close / LAG(adj_close) OVER (...)) AS ret_1d
FROM raw_prices
```

---

## CLI Commands

```bash
# Print the full asset lineage graph
quant-alpha bruin-lineage

# Show upstream and downstream for a specific asset
quant-alpha bruin-lineage --asset fct_alpha_diagnostics

# Dry-run: print execution plan without running anything
quant-alpha bruin-run --dry-run

# Run a specific asset and all its upstream dependencies
quant-alpha bruin-run --targets fct_equity_alpha_panel

# Run all assets
quant-alpha bruin-run
```

Example output of `bruin-lineage`:

```
Asset Lineage Graph
==================================================
  raw_equity_ohlcv
    type=python  owner=ingestion-team  tags=['equity', 'raw', 'daily']
  raw_power_market
    type=python  owner=energy-research  tags=['energy', 'raw', 'hourly']
  stg_equity_ohlcv  ← raw_equity_ohlcv
    type=duckdb.table  owner=alpha-research  tags=['equity', 'staging']
  stg_power_market  ← raw_power_market
    type=duckdb.table  owner=energy-research  tags=['energy', 'staging']
  fct_equity_alpha_panel  ← stg_equity_ohlcv
  fct_energy_alpha_panel  ← stg_power_market
  fct_alpha_diagnostics  ← fct_equity_alpha_panel
  rpt_backtest_summary  ← fct_alpha_diagnostics, fct_equity_alpha_panel
```

---

## Environments and Connections

`.bruin.yml` defines two environments:

| Environment | Connection | Backend |
|---|---|---|
| `local` | `duckdb_local` | `data/warehouse/quant_alpha.duckdb` |
| `local` | `duckdb_energy` | `data/warehouse/second_foundation.duckdb` |
| `cloud` | `bigquery_main` | GCP BigQuery (requires `GCP_PROJECT_ID`) |
| `cloud` | `gcs_raw` | Google Cloud Storage raw layer |

Switch environments by setting `BRUIN_ENV=cloud` before running.

---

## Data Quality Checks

Each asset can declare:

- **Column checks**: `not_null`, `positive` — enforced before downstream runs
- **Custom SQL checks**: arbitrary queries with expected result values/operators

The local `AssetGraph` runner reports check results inline. In production Bruin these run as pre- and post-conditions around each asset execution.

---

## Python Implementation

`src/quant_alpha/platform/bruin_graph.py` provides the local simulation:

- `AssetGraph`: loads all `.asset.yml` and SQL `@asset` files from `bruin/`
- `topological_order()`: Kahn's algorithm for dependency-safe execution order
- `upstream(name)` / `downstream(name)`: lineage traversal
- `run(targets, dry_run)`: execute assets in order, skipping if upstream failed
- `lineage_report()` / `status_report()`: human-readable output

`src/quant_alpha/platform/contracts.py` holds `DatasetContract` dataclasses for both equity and energy tracks — a lightweight schema registry decoupled from the execution graph.
