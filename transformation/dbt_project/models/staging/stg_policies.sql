with source_data as (
    select *
    from {{ source('bronze', 'raw_policies') }}
),

cleaned as (
    select
        policy_id,
        customer_id,
        trim(policy_number) as policy_number,
        trim(destination_country) as destination_country,
        trip_start_date,
        trip_end_date,
        upper(trim(coverage_type)) as coverage_type,
        cast(round(try_to_decimal(premium_amount, 10, 2), 2) as decimal(10, 2)) as premium_amount,
        lower(trim(status)) as policy_status,
        broker_id,
        created_at,
        datediff('day', trip_start_date, trip_end_date) as trip_duration_days
    from source_data
    where not (lower(trim(status)) = 'cancelled' and premium_amount is null)
)

select *
from cleaned
