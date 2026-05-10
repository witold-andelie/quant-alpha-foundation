select
    alpha_name,
    is_ic_mean,
    oos_ic_mean,
    is_oos_ic_same_sign,
    consistency_score,
    robustness_score,
    oos_sharpe,
    oos_max_drawdown
from {{ source('quant_alpha_raw', 'alpha_diagnostics') }}
