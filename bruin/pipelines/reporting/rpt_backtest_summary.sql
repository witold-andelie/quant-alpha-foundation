/* @asset
name: rpt_backtest_summary
type: duckdb.table
connection: duckdb_local
description: >
  Daily P&L roll-up across all active alphas. Combines equity and energy
  track backtest results into a unified reporting layer for the Streamlit
  dashboard.
depends:
  - fct_alpha_diagnostics
  - fct_equity_alpha_panel
owner: research-platform
tags:
  - reporting
  - backtest
  - daily
columns:
  - name: track
    description: "equity or energy"
    checks: [not_null]
  - name: alpha_name
    checks: [not_null]
  - name: date
    checks: [not_null]
  - name: daily_pnl
    description: Daily strategy P&L
  - name: cumulative_pnl
    description: Cumulative P&L from strategy inception
  - name: sharpe_ann
    description: Annualised Sharpe ratio (252-day rolling)
  - name: max_drawdown
    description: Maximum drawdown to date
*/

SELECT
    'equity'                                                AS track,
    d.alpha_name,
    b.date,
    b.daily_pnl,
    SUM(b.daily_pnl) OVER (
        PARTITION BY d.alpha_name ORDER BY b.date
    )                                                       AS cumulative_pnl,
    b.sharpe_ann,
    b.max_drawdown
FROM backtest_daily        AS b
JOIN fct_alpha_diagnostics AS d USING (alpha_name)
WHERE d.gate_consistency = true

UNION ALL

SELECT
    'energy'                                                AS track,
    e.alpha_name,
    e.date,
    e.daily_pnl,
    SUM(e.daily_pnl) OVER (
        PARTITION BY e.alpha_name ORDER BY e.date
    )                                                       AS cumulative_pnl,
    e.sharpe_ann,
    e.max_drawdown
FROM energy_backtest_daily AS e
WHERE e.gate_consistency = true

ORDER BY alpha_name, date
