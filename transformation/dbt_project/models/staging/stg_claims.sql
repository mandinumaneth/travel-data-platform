with source_data as (
    select *
    from {{ source('bronze', 'raw_claims') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by claim_id
            order by created_at desc
        ) as row_num
    from source_data
),

cleaned as (
    select
        claim_id,
        policy_id,
        lower(trim(claim_type)) as claim_type,
        cast(
            round(
                try_to_decimal(to_varchar(claim_amount), 12, 2),
                2
            ) as decimal(12, 2)
        ) as claim_amount,
        claim_date,
        lower(trim(status)) as claim_status,
        description,
        processed_at,
        created_at,
        case
            when processed_at is null then null
            else datediff(
                'day',
                claim_date,
                to_date(try_to_timestamp_ntz(to_varchar(processed_at)))
            )
        end as days_to_process,
        case when lower(trim(status)) = 'approved' then true else false end as is_approved
    from ranked
    where row_num = 1
)

select *
from cleaned
