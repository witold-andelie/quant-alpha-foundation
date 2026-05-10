select
    cast(timestamp as timestamp) as market_ts,
    market,
    alpha_energy_residual_load_shock,
    alpha_energy_wind_forecast_error,
    alpha_energy_imbalance_premium,
    alpha_composite,
    forward_return
from {{ source('energy_raw', 'power_market_features') }}
where alpha_composite is not null
