select
    cast(date as date) as signal_date,
    symbol,
    adj_close,
    ret_1d,
    forward_return,
    alpha_wq_001_reversal_rank,
    alpha_wq_002_volume_price_divergence,
    alpha_wq_003_intraday_range_position,
    alpha_trend_021_medium_momentum,
    alpha_risk_020_low_volatility,
    alpha_liquidity_020_volume_shock,
    alpha_composite
from {{ source('quant_alpha_raw', 'factor_panel') }}
