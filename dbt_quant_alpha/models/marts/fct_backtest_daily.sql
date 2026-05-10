select
    cast(date as date) as signal_date,
    gross_return,
    transaction_cost,
    portfolio_return,
    equity_curve,
    long_count,
    short_count
from {{ source('quant_alpha_raw', 'backtest_daily') }}
