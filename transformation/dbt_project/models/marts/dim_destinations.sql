with destinations as (
    select distinct upper(destination_country) as destination_country
    from {{ ref('stg_policies') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['destination_country']) }} as destination_sk,
    destination_country,
    case destination_country
        when 'THAILAND' then 'Asia'
        when 'SPAIN' then 'Europe'
        when 'USA' then 'North America'
        when 'JAPAN' then 'Asia'
        when 'AUSTRALIA' then 'Oceania'
        when 'UAE' then 'Middle East'
        when 'FRANCE' then 'Europe'
        when 'ITALY' then 'Europe'
        when 'MALDIVES' then 'Asia'
        when 'UK' then 'Europe'
        else 'Other'
    end as continent
from destinations
