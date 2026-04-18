with source_data as (
    select *
    from {{ source('bronze', 'raw_broker_commissions') }}
),

ranked as (
    select
        broker_id,
        trim(broker_name) as broker_name,
        trim(broker_company) as broker_company,
        trim(country) as country,
        cast(policies_sold_today as integer) as policies_sold_today,
        cast(total_premium_value as decimal(12, 2)) as total_premium_value,
        cast(commission_rate as float) as commission_rate,
        cast(commission_earned as decimal(12, 2)) as commission_earned,
        report_date,
        cast(commission_rate * 100 as decimal(5, 2)) as commission_percentage,
        row_number() over (
            partition by broker_id
            order by report_date desc, commission_earned desc
        ) as row_num
    from source_data
)

select
    broker_id,
    broker_name,
    broker_company,
    country,
    policies_sold_today,
    total_premium_value,
    commission_rate,
    commission_earned,
    commission_percentage,
    report_date
from ranked
where row_num = 1
