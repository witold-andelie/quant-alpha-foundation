# Backtesting

This module provides the full research evaluation stack: long-short portfolio simulation, IS/OOS diagnostics, alpha decay curves, walk-forward IC stability, and per-factor turnover.

## Modules

| File | Purpose |
|---|---|
| `long_short.py` | Daily long-short portfolio backtest with transaction costs |
| `diagnostics.py` | IS/OOS IC metrics, consistency/robustness scores, value-added report, turnover |
| `alpha_decay.py` | IC by forward horizon (decay curves) and rolling walk-forward IC |

---

## Long-Short Backtest (`long_short.py`)

On each date, stocks are ranked by the chosen alpha. The top `top_quantile` fraction are held long (equal-weighted, 0.5 total weight) and the bottom `bottom_quantile` fraction are held short (equal-weighted, −0.5 total weight).

**Transaction cost model**

```
cost_t = |Δweight_t| × (transaction_cost_bps / 10,000)
portfolio_return_t = gross_return_t − cost_t
```

**Output metrics**

| Metric | Description |
|---|---|
| `total_return` | Cumulative return over the full period |
| `annualized_return` | Mean daily return × `periods_per_year` |
| `annualized_volatility` | Daily return std × √`periods_per_year` |
| `sharpe` | Annualized return / annualized volatility |
| `sortino` | Annualized return / annualized downside deviation |
| `calmar` | Annualized return / abs(max drawdown) |
| `max_drawdown` | Worst peak-to-trough equity drawdown |
| `win_rate` | Fraction of days with positive portfolio return |
| `observations` | Number of trading days in the backtest |

**Configuration** (`configs/project.yaml`)

```yaml
backtest:
  forward_return_days: 5
  top_quantile: 0.2
  bottom_quantile: 0.2
  transaction_cost_bps: 1
  periods_per_year: 252
```

---

## Diagnostics (`diagnostics.py`)

### IS/OOS Split

By default, the first 70% of dates are in-sample (IS) and the remaining 30% are out-of-sample (OOS). A custom split date can be passed via `split_date`.

### Rank IC

Daily Spearman rank correlation between the factor and the forward return:

```
IC_t = spearman_corr(rank(alpha_t), rank(forward_return_t))
```

IC-IR = mean(IC) / std(IC). A sustained IC-IR > 0.3 is considered meaningful in academic research.

### Consistency Score

```
consistency = 0.6 × sign_agreement + 0.4 × min(|OOS IC| / |IS IC|, 1.0)
```

Factors with `consistency_score < 0.5` are excluded from the composite.

### Robustness Score

```
robustness = 0.4 × obs_score + 0.4 × ir_score + 0.2 × (1 − max_drawdown)
```

### Alpha Correlation

Pairwise Spearman correlation between all factors. High correlation (|r| > 0.7) flags redundancy and uniqueness violations.

### Value-Added Report

Compares composite OOS Sharpe with the best individual OOS Sharpe. Positive `sharpe_value_added` confirms that combining factors improves the portfolio.

### Per-Factor Turnover

Average daily fraction of the portfolio repositioned. High turnover relative to signal strength erodes net alpha after costs.

---

## Alpha Decay (`alpha_decay.py`)

### Decay Curves

IC is computed at multiple forward horizons to measure how quickly the signal loses predictive power:

**Equity horizons:** 1, 3, 5, 10, 22, 44 trading days

**Energy horizons:** 1, 3, 6, 12, 24, 48 hours

A factor with IC concentrated at short horizons is a short-term signal; one that peaks at longer horizons is a slow signal. Mixing signals with incompatible horizons in a single composite degrades performance.

### Walk-Forward IC

Rolling IS/OOS windows quantify IC stability across market regimes:

```
IS window:   252 trading days (1 year)
OOS window:  63  trading days (1 quarter)
Step:        63  trading days
```

Each OOS window produces one IC mean and IC-IR estimate. A factor with consistently positive IC-IR across most windows is genuinely robust.

---

## Running the Backtest

```bash
# Equity pipeline (runs backtest internally)
quant-alpha run --offline

# Energy pipeline
quant-alpha energy-run
```

Results are written to DuckDB and Parquet:

| Table | Content |
|---|---|
| `backtest_daily` | Daily portfolio returns and equity curve |
| `alpha_diagnostics` | IS/OOS IC, consistency, and robustness per factor |
| `alpha_correlations` | Pairwise factor correlation matrix |
| `alpha_value_added` | Composite vs best single-factor Sharpe |
| `alpha_decay` | IC by forward horizon |
| `alpha_walk_forward` | Rolling OOS IC per factor |
| `alpha_turnover` | Mean and median daily turnover per factor |
