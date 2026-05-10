# Alpha Factor Engine

This module implements the full factor computation stack: an expression registry, cross-sectional operators, and the compute pipeline for both equity and energy universes.

## Design Principles

Each factor must satisfy four gates before being included in the composite:

| Gate | Meaning | Enforcement |
|---|---|---|
| Robustness | Stable OOS evidence, not many fragile signals | OOS IC, IC-IR, max drawdown measured per factor |
| Uniqueness | Each expression captures one clear phenomenon | Pairwise Spearman correlation matrix; high overlap is a warning |
| Value-added | Composite should improve on the best single factor | `value_added_report()` compares composite OOS Sharpe with best individual |
| Consistency | IS and OOS must tell a compatible story | IS/OOS IC sign agreement and magnitude ratio tracked in diagnostics |

## Equity Factor Registry (`registry.py`)

All factors are defined as `AlphaDefinition` dataclasses: name, expression string, family, hypothesis, expected direction, and a compute callable. This makes the research process auditable — the expression and the code that evaluates it live in the same record.

### Available Operators

| Operator | Signature | Description |
|---|---|---|
| `cs_rank` | `(series)` | Cross-sectional percentile rank, centered at 0 |
| `ts_rank` | `(series, window)` | Time-series percentile rank over rolling window |
| `delta` | `(series, periods)` | First difference over `periods` |
| `delay` | `(series, periods)` | Lag by `periods` |
| `ts_corr` | `(left, right, window)` | Rolling Pearson correlation |
| `ts_std` | `(series, window)` | Rolling standard deviation |
| `ts_mean` | `(series, window)` | Rolling mean |
| `safe_divide` | `(left, right, eps)` | Division with zero-guard |

### Equity Factor Panel (10 factors)

| Name | Expression | Family | Hypothesis |
|---|---|---|---|
| `alpha_wq_001_reversal_rank` | `-rank(delta(close, 5))` | short_reversal | Five-day moves tend to partially mean-revert |
| `alpha_wq_002_volume_price_divergence` | `-correlation(rank(delta(log(volume), 2)), rank((close-open)/open), 6)` | volume_price | Volume acceleration aligned with intraday return marks crowded pressure |
| `alpha_wq_003_intraday_range_position` | `rank((close-open)/(high-low+0.001))` | intraday_pressure | Close location inside the range captures directional pressure |
| `alpha_trend_021_medium_momentum` | `rank(delay(close / close_21d_ago - 1, 1))` | medium_momentum | Medium-horizon winners persist after a one-day lag |
| `alpha_risk_020_low_volatility` | `-rank(delay(stddev(returns, 20), 1))` | risk_premium | Lower realized volatility names carry better risk-adjusted returns |
| `alpha_liquidity_020_volume_shock` | `rank(delay(zscore(volume, 20), 1))` | liquidity | Abnormal participation is a clean liquidity-pressure feature |
| `alpha_wq_007_price_to_ma_reversion` | `-rank(close / ts_mean(close, 60) - 1)` | mean_reversion | Price stretched above its 60-day MA tends to mean-revert |
| `alpha_wq_008_overnight_gap` | `rank(open / delay(close, 1) - 1)` | microstructure | Overnight gap isolates informed after-hours flow from intraday noise |
| `alpha_wq_009_volume_weighted_return` | `rank(sum(ret * volume, 10) / sum(volume, 10))` | volume_momentum | Volume-weighted returns filter volume-lite noise moves |
| `alpha_wq_010_gap_quality` | `-rank(ts_std(open / delay(close, 1) - 1, 20))` | quality | Low overnight-gap volatility proxies earnings stability |

### Adding a New Equity Factor

1. Open `registry.py`.
2. Add an `AlphaDefinition` entry inside `make_equity_alpha_registry()`.
3. The `compute` callable receives a DataFrame indexed by `(date, symbol)` and must return a `pd.Series` with the same index.
4. Run `pytest tests/test_alpha_factors.py` to confirm the new factor appears in `BASE_FACTOR_COLUMNS` and produces non-null values.

## Energy Factor Registry (`energy_alpha.py`)

Energy factors operate on hourly cross-market panels. All factors are lagged by one period before use to prevent lookahead.

### Energy Factor Panel (8 factors)

| Name | Expression | Family | Hypothesis |
|---|---|---|---|
| `alpha_energy_residual_load_shock` | `zscore(residual_load, 168)` | scarcity | Residual load spikes proxy short-term scarcity |
| `alpha_energy_wind_forecast_error` | `-zscore(delta(wind_forecast, 24), 168)` | renewables | Wind forecast shifts proxy renewable supply surprise |
| `alpha_energy_imbalance_premium` | `zscore(imbalance_price - spot_price, 72)` | imbalance | Balancing premium captures system stress |
| `alpha_energy_cross_market_spread` | `zscore(spot_price - mean_cross_market_spot, 168)` | arbitrage | Markets priced above the European average face convergence pressure |
| `alpha_energy_demand_surprise` | `zscore(actual_load - load_forecast, 72)` | fundamental | Positive demand errors tighten the dispatch stack |
| `alpha_energy_solar_penetration` | `-zscore(solar_forecast / load_forecast, 168)` | renewables | High solar penetration creates mid-day price depression |
| `alpha_energy_price_momentum_6h` | `rank(spot_price / delay(spot_price, 6) - 1)` | momentum | Dispatch inertia creates 6-hour price momentum |
| `alpha_energy_gas_spark_spread` | `zscore(spot_price - gas_price * 2.0, 168)` | fundamental | Spot price above the gas-implied spark spread signals extra scarcity |

### Cross-market computation

`cross_market_spot_mean` is computed across all markets at each timestamp before any per-market groupby. This makes `alpha_energy_cross_market_spread` a genuine cross-sectional signal rather than a time-series z-score.

## Composite Alpha

After individual factor ranks are computed, they are averaged into `alpha_composite`:

```python
alpha_composite = mean(rank_col_1, rank_col_2, ...) - 0.5
```

The composite is recomputed after `select_consistent_alphas()` drops factors with `consistency_score < 0.5`.
