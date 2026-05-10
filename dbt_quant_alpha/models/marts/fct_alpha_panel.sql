select
    signal_date,
    symbol,
    alpha_composite,
    forward_return,
    alpha_wq_001_reversal_rank,
    alpha_wq_002_volume_price_divergence,
    alpha_wq_003_intraday_range_position,
    alpha_trend_021_medium_momentum,
    alpha_risk_020_low_volatility,
    alpha_liquidity_020_volume_shock,
    corr(alpha_composite, forward_return) over (
        order by signal_date
        rows between 62 preceding and current row
    ) as rolling_63d_rank_ic_proxy
from {{ ref('stg_factor_panel') }}
where alpha_composite is not null
