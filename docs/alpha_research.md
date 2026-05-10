# Alpha Research

This document describes the alpha factor research methodology, the full factor catalog, and the validation standards used in this project. The approach is inspired by WorldQuant's formulaic alpha style and Second Foundation's energy-market focus.

## Research Philosophy

An alpha is an expression plus evidence. A new factor is not accepted until it clears all four research gates:

| Gate | Meaning | How the platform checks it |
|---|---|---|
| Robustness | Prefer stable, simple signals over many fragile ones | OOS IC, IC-IR, walk-forward window consistency, max drawdown per factor |
| Uniqueness | Each expression captures one clear phenomenon | Pairwise Spearman correlation matrix; `|r| > 0.7` triggers a warning |
| Value-added | Combining alphas must improve the portfolio | Composite OOS Sharpe compared with best individual OOS Sharpe in `alpha_value_added` |
| Consistency | IS and OOS must tell a compatible story | IS/OOS IC sign agreement and magnitude ratio in `alpha_diagnostics` |

**Principle:** Three robust alphas beat twenty fragile ones. The research loop runs until evidence accumulates, not until a performance target is hit.

---

## Research Loop

```
1. Hypothesis       State the phenomenon in one sentence.
2. Expression       Write a compact operator expression.
3. Backtest (IS)    Run long-short, compute IC, check lag.
4. Diagnostics      Consistency score, robustness score, correlation matrix.
5. OOS test         Split at 70%, check sign agreement and IC-IR.
6. Walk-forward     Roll IS/OOS windows to test across regimes.
7. Value-added      Add to composite and measure Sharpe improvement.
8. Promote          Merge only if all gates pass.
```

---

## Equity Factor Catalog (10 factors)

| Factor | Expression | Family | Hypothesis | Expected Direction |
|---|---|---|---|---|
| `alpha_wq_001_reversal_rank` | `-rank(delta(close, 5))` | short_reversal | Five-day moves tend to partially mean-revert in liquid universes | +1 |
| `alpha_wq_002_volume_price_divergence` | `-correlation(rank(delta(log(volume), 2)), rank((close-open)/open), 6)` | volume_price | Volume acceleration aligned with intraday return marks crowded short-term pressure | +1 |
| `alpha_wq_003_intraday_range_position` | `rank((close-open)/(high-low+0.001))` | intraday_pressure | Close location inside the intraday range captures directional pressure cleanly | +1 |
| `alpha_trend_021_medium_momentum` | `rank(delay(close / close_21d_ago - 1, 1))` | medium_momentum | Medium-horizon winners can persist after a one-day lag removes lookahead | +1 |
| `alpha_risk_020_low_volatility` | `-rank(delay(stddev(returns, 20), 1))` | risk_premium | Lower realized volatility names carry better risk-adjusted forward returns | +1 |
| `alpha_liquidity_020_volume_shock` | `rank(delay(zscore(volume, 20), 1))` | liquidity | Abnormal participation is a clean, isolated liquidity-pressure feature | +1 |
| `alpha_wq_007_price_to_ma_reversion` | `-rank(close / ts_mean(close, 60) - 1)` | mean_reversion | Intermediate-horizon mean reversion distinct from the 5-day reversal | +1 |
| `alpha_wq_008_overnight_gap` | `rank(open / delay(close, 1) - 1)` | microstructure | Overnight gap isolates informed after-hours flow, orthogonal to intraday range | +1 |
| `alpha_wq_009_volume_weighted_return` | `rank(sum(ret * volume, 10) / sum(volume, 10))` | volume_momentum | Volume-weighted 10-day returns filter out volume-lite noise moves | +1 |
| `alpha_wq_010_gap_quality` | `-rank(ts_std(open / delay(close, 1) - 1, 20))` | quality | Low overnight-gap volatility proxies earnings stability and information quality | +1 |

### Uniqueness Map

The ten factors span six distinct families. No two factors share a family except where intentional (volume signals):

```
short_reversal      wq_001
volume_price        wq_002
intraday_pressure   wq_003
medium_momentum     trend_021
risk_premium        risk_020
liquidity           liquidity_020
mean_reversion      wq_007
microstructure      wq_008
volume_momentum     wq_009
quality             wq_010
```

---

## Energy Factor Catalog (8 factors)

| Factor | Expression | Family | Hypothesis |
|---|---|---|---|
| `alpha_energy_residual_load_shock` | `zscore(residual_load, 168)` | scarcity | Residual load spikes proxy short-term power scarcity over a 7-day window |
| `alpha_energy_wind_forecast_error` | `-zscore(delta(wind_forecast, 24), 168)` | renewables | Sharp 24-hour wind forecast shifts proxy renewable supply surprise |
| `alpha_energy_imbalance_premium` | `zscore(imbalance_price - spot_price, 72)` | imbalance | Balancing premium above spot captures system stress and repricing pressure |
| `alpha_energy_cross_market_spread` | `zscore(spot_price - mean_cross_market_spot, 168)` | arbitrage | Markets priced above the European cross-market average face arbitrage convergence |
| `alpha_energy_demand_surprise` | `zscore(actual_load - load_forecast, 72)` | fundamental | Positive demand forecast errors tighten the dispatch stack and lift spot prices |
| `alpha_energy_solar_penetration` | `-zscore(solar_forecast / load_forecast, 168)` | renewables | High solar penetration creates mid-day negative price pressure |
| `alpha_energy_price_momentum_6h` | `rank(spot_price / delay(spot_price, 6) - 1)` | momentum | Power dispatch inertia creates 6-hour price momentum |
| `alpha_energy_gas_spark_spread` | `zscore(spot_price - gas_price * 2.0, 168)` | fundamental | Spot above the gas-implied spark spread signals additional scarcity premium |

---

## Alpha Decay Analysis

Signal strength is measured at multiple forward horizons to determine the trading frequency implied by each factor:

**Equity horizons:** 1, 3, 5, 10, 22, 44 trading days

**Energy horizons:** 1, 3, 6, 12, 24, 48 hours

A factor peaking at 1-day horizon is a high-turnover short-term signal; one peaking at 22 days fits a monthly rebalance. Mixing incompatible horizons in a single composite degrades net performance after costs.

---

## Validation Checklist

Before promoting any factor to the live composite, verify:

- No lookahead bias: all inputs are lagged by at least one period
- Survivorship bias: use point-in-time universes for equity research
- Transaction costs: turnover-aware backtest with realistic bps
- Capacity: participation rate < 5% of average daily volume
- Risk neutralization: market-beta, sector, and common-factor exposures
- Robustness: walk-forward IC consistent across at least 6 rolling windows
- Uniqueness: pairwise correlation with all existing factors `|r| < 0.7`
- Value-added: composite OOS Sharpe improves on best single-factor

---

## References

- Kakushadze, Z. (2016). *101 Formulaic Alphas*. Wilmott, 2016(84).
- Lopez de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
- Second Foundation: [second-foundation.eu](https://www.second-foundation.eu)
- ENTSO-E Transparency Platform: [transparency.entsoe.eu](https://transparency.entsoe.eu)
