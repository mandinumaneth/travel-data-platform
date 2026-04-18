with source_data as (
    select *
    from {{ source('bronze', 'raw_policies') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by policy_id
            order by created_at desc
        ) as row_num
    from source_data
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
        cast(
            round(
                try_to_decimal(to_varchar(premium_amount), 10, 2),
                2
            ) as decimal(10, 2)
        ) as premium_amount,
        lower(trim(status)) as policy_status,
        broker_id,
        try_to_timestamp_ntz(to_varchar(created_at)) as created_at,
        datediff('day', trip_start_date, trip_end_date) as trip_duration_days
    from ranked
    where row_num = 1
      and not (lower(trim(status)) = 'cancelled' and premium_amount is null)
)

select *
from cleaned
