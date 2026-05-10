/* @asset
name: stg_equity_ohlcv
type: duckdb.table
connection: duckdb_local
description: Cleaned equity OHLCV — deduplicated, nulls dropped, returns computed.
depends:
  - raw_equity_ohlcv
owner: alpha-research
tags:
  - equity
  - staging
columns:
  - name: date
    checks: [not_null]
  - name: symbol
    checks: [not_null]
  - name: adj_close
    checks: [not_null, positive]
  - name: ret_1d
    description: Daily log return
*/

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    ln(adj_close / NULLIF(LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date), 0)) AS ret_1d
FROM raw_prices
WHERE
    adj_close IS NOT NULL
    AND adj_close > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY date) = 1
ORDER BY symbol, date
