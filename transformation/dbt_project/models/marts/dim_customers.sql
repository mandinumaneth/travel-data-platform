with customers as (
    select *
    from {{ ref('stg_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(customer_id as string)']) }} as customer_sk,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    date_of_birth,
    greatest(datediff('year', date_of_birth, current_date()), 0) as customer_age,
    country_of_residence,
    country_iso_code,
    phone,
    created_at
from customers
