with source_data as (
    select *
    from {{ source('bronze', 'flight_daily_stats') }}
)

select
    flight_day as stat_date,
    origin_country,
    flight_count,
    avg_altitude,
    avg_velocity,
    pct_on_ground
from source_data
