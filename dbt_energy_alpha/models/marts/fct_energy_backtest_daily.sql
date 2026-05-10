select
    cast(date as timestamp) as market_ts,
    gross_return,
    transaction_cost,
    portfolio_return,
    equity_curve,
    long_count,
    short_count
from {{ source('energy_raw', 'energy_backtest_daily') }}
