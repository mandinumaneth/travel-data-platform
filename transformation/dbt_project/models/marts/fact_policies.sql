{{
    config(
        materialized='incremental',
        unique_key='policy_id',
        on_schema_change='sync_all_columns'
    )
}}

with policies as (
    select *
    from {{ ref('stg_policies') }}
    {% if is_incremental() %}
      where created_at > (select coalesce(max(created_at), to_timestamp_ntz('1900-01-01')) from {{ this }})
    {% endif %}
),

customers as (
    select customer_id, customer_sk
    from {{ ref('dim_customers') }}
),

destinations as (
    select destination_country, destination_sk
    from {{ ref('dim_destinations') }}
),

brokers as (
    select broker_id, broker_sk
    from {{ ref('dim_brokers') }}
),

dates as (
    select date_day, date_sk
    from {{ ref('dim_date') }}
)

select
    p.policy_id,
    c.customer_sk,
    d.destination_sk,
    b.broker_sk,
    dt.date_sk,
    p.customer_id,
    upper(p.destination_country) as destination_country,
    p.broker_id,
    p.trip_start_date,
    p.trip_end_date,
    p.premium_amount,
    p.coverage_type,
    p.trip_duration_days,
    p.policy_status,
    p.created_at
from policies p
left join customers c
    on p.customer_id = c.customer_id
left join destinations d
    on upper(p.destination_country) = d.destination_country
left join brokers b
    on p.broker_id = b.broker_id
left join dates dt
    on p.trip_start_date = dt.date_day
