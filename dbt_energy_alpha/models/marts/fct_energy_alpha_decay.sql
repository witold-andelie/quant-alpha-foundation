{{ config(materialized="table") }}

select
    alpha_name,
    horizon_hours,
    ic,
    case
        when ic > 0.02 then 'positive'
        when ic < -0.02 then 'negative'
        else 'flat'
    end as ic_regime,
    current_timestamp as refreshed_at
from {{ source('energy_alpha', 'energy_alpha_decay') }}
where ic is not null
order by alpha_name, horizon_hours
