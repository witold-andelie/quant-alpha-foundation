select
    cast(timestamp as timestamp) as market_ts,
    market,
    spot_price,
    load_forecast,
    wind_forecast,
    solar_forecast,
    residual_load,
    imbalance_price
from {{ source('energy_raw', 'power_market_features') }}
