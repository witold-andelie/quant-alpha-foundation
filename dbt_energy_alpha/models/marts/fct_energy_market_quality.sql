select
    "check" as check_name,
    "column" as column_name,
    keys as key_columns,
    passed,
    nulls,
    duplicates
from {{ source('energy_raw', 'power_market_quality') }}
