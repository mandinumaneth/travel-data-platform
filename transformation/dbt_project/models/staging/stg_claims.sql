with source_data as (
    select *
    from {{ source('bronze', 'raw_claims') }}
),

cleaned as (
    select
        claim_id,
        policy_id,
        lower(trim(claim_type)) as claim_type,
        cast(round(try_to_decimal(claim_amount, 12, 2), 2) as decimal(12, 2)) as claim_amount,
        claim_date,
        lower(trim(status)) as claim_status,
        description,
        processed_at,
        created_at,
        case
            when processed_at is null then null
            else datediff('day', claim_date, cast(processed_at as date))
        end as days_to_process,
        case when lower(trim(status)) = 'approved' then true else false end as is_approved
    from source_data
)

select *
from cleaned
