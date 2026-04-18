{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        on_schema_change='sync_all_columns'
    )
}}

with claims as (
    select *
    from {{ ref('stg_claims') }}
    {% if is_incremental() %}
      where created_at > (select coalesce(max(created_at), to_timestamp_ntz('1900-01-01')) from {{ this }})
    {% endif %}
),

policies as (
    select
        policy_id,
        customer_sk,
        destination_sk,
        broker_sk,
        date_sk,
        destination_country
    from {{ ref('fact_policies') }}
)

select
    c.claim_id,
    c.policy_id,
    p.customer_sk,
    p.destination_sk,
    p.broker_sk,
    p.date_sk,
    p.destination_country,
    c.claim_type,
    c.claim_amount,
    c.claim_status,
    c.is_approved,
    c.days_to_process,
    c.claim_date,
    c.processed_at,
    c.created_at
from claims c
left join policies p
    on c.policy_id = p.policy_id
