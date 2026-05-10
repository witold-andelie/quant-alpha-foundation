select
    cast(date as date) as price_date,
    symbol,
    open,
    high,
    low,
    close,
    adj_close,
    volume
from {{ source('quant_alpha_raw', 'raw_prices') }}
