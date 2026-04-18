with source_data as (
    select *
    from {{ source('bronze', 'bronze_flights_clean') }}
)

select
    icao24,
    nullif(trim(callsign), '') as callsign,
    trim(origin_country) as origin_country,
    cast(longitude as float) as longitude,
    cast(latitude as float) as latitude,
    cast(altitude as float) as altitude,
    cast(velocity as float) as velocity,
    cast(heading as float) as heading,
    case
        when on_ground in (true, 'true', 'TRUE', 1) then true
        else false
    end as on_ground,
    event_timestamp,
    ingested_at
from source_data
where nullif(trim(callsign), '') is not null
