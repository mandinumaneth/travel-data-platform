with brokers as (
    select *
    from {{ ref('stg_brokers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(broker_id as string)']) }} as broker_sk,
    broker_id,
    broker_name,
    broker_company,
    country,
    policies_sold_today,
    total_premium_value,
    commission_rate,
    commission_percentage,
    commission_earned,
    report_date
from brokers
