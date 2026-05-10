# Streamlit Research Dashboard

Multi-page Streamlit app for exploring alpha factor research, backtest performance, signal decay, live energy signals, and the data pipeline status.

## Architecture

| File | Role |
|---|---|
| `app.py` | Entry point — `st.set_page_config`, explicit page registration via `st.Page()` + `st.navigation()` |
| `home.py` | Home page content — top metrics, 7 clickable navigation cards, Quick Actions |
| `common.py` | Shared utilities — DuckDB/BigQuery loaders, table catalogs, track selector |
| `pages/N_*.py` | 7 deep-dive pages (auto-loaded via `st.navigation`) |

## Pages

| # | Page | Module Coverage |
|---|---|---|
| Home | `home.py` | Cross-track summary metrics + 7 navigation cards |
| 1 | Performance | Equity curve + drawdown, rolling Sharpe (63d), return distribution, win/skew/kurtosis, long/short holdings, latest factor snapshot |
| 2 | Factor Research | 4-gate scorecard (✅/❌ icons), IS vs OOS IC scatter (with diagonal), pairwise correlation heatmap, factor history per market/symbol |
| 3 | Alpha Decay | IC decay curves, alpha × horizon heatmap, walk-forward OOS IC stability, IC IR per window, cross-alpha stability summary, turnover vs OOS Sharpe scatter |
| 4 | Market Data | Spot prices, supply/demand fundamentals, imbalance premium, cross-market spread, Spark batch rolling features (energy); price/return/volume (equity) |
| 5 | Live Streaming | Redpanda signal buffer status, fundamentals timeline, RisingWave simulator (one-click), real-time alpha percentile heatmap, scarcity alerts (HIGH/MEDIUM/LOW) |
| 6 | Data Pipeline | DuckDB table inventory + row counts, Bruin asset graph (lineage explorer), data quality + null-rate analysis, dlt load history, Kestra flow inventory |
| 7 | Cross-Track Overview | Energy/Equity metrics side-by-side, 11-module Zoomcamp coverage matrix, alpha family pies, cross-track diagnostics comparison |

## Starting the Dashboard

```bash
# From project root
streamlit run streamlit_app/app.py

# Docker
docker compose up --build dashboard
# Open http://localhost:8501
```

## Track Selector

Two research tracks: **Second Foundation Energy** and **US Equities Demo**. The selector appears at the top of every page (component `render_track_selector` in `common.py`) and persists in `st.session_state["track"]` across page navigation.

Each page's data resolution uses `ENERGY_TABLES` / `EQUITY_TABLES` catalogs from `common.py`, with table-name fallback chains via `pick(db, *candidates)` (e.g. tries `fct_energy_backtest_daily` first, falls back to `energy_backtest_daily`).

## Navigation Mechanism

`st.navigation()` + `st.Page()` (Streamlit 1.36+) is used for explicit page registration. Cards on the home page use `st.button` + `st.switch_page("pages/N_X.py")` for direct navigation. This API is more reliable than the legacy `pages/` auto-discovery, which had `url_pathname` registration issues in some versions.

## Data Backend

Defaults to local DuckDB. Switch to BigQuery for cloud deployment:

```bash
STREAMLIT_DATA_BACKEND=bigquery \
GCP_PROJECT_ID=your-project \
BQ_DATASET=second_foundation_quant \
streamlit run streamlit_app/app.py
```

## Caching

All database reads use `@st.cache_data(ttl=300)` (5-minute auto-expiry). To force a refresh:

- Click the **🔄 Refresh** button in any page's track selector row
- Click **🔄 Refresh all caches** in the Quick Actions card on home
- Or restart Streamlit (`pkill -f streamlit; streamlit run streamlit_app/app.py`)

## Live Signals Demo Mode

The home page Quick Actions card and the Live Streaming page both expose a **🌱 Seed 48 h demo signals** button, which calls `quant_alpha.streaming.demo_signals.seed_demo_signals()` to generate ~147 rows of synthetic signals into `live_energy_signals` (DuckDB).

For production streaming, start Redpanda + RisingWave first:

```bash
docker compose -f docker-compose.risingwave.yml up -d
python -m quant_alpha.streaming.redpanda_consumer
```

## Prerequisites

Data must exist before the dashboard renders meaningfully:

```bash
# Energy track (synthetic data)
quant-alpha energy-run

# Equity track (offline)
quant-alpha run --offline

# Optional: rebuild dbt marts
cd dbt_energy_alpha && dbt build --profiles-dir .
cd ../dbt_quant_alpha && dbt build --profiles-dir .
```

## Tech Stack

- **Streamlit 1.56+** (multi-page nav via `st.Page` / `st.navigation`)
- **Plotly** for all charts (`px.line`, `px.scatter`, `px.imshow` heatmaps, `px.histogram`, subplots via `make_subplots`)
- **DuckDB** read-only connections (`con.execute(...).df()`)
- **pandas** for in-memory transforms

## Adding a New Page

1. Create `pages/N_New_Page.py` (`N_` prefix sets sidebar order)
2. Import shared utilities: `from common import render_track_selector, pick, ...`
3. Skip `st.set_page_config` — only `app.py` calls it
4. Register the page in `app.py`:

```python
new_page = st.Page("pages/N_New_Page.py", title="New Page", icon="🆕")
pg = st.navigation({
    " ": [home],
    "Research": [..., new_page],
    ...
})
```
