with source_data as (
    select *
    from {{ source('bronze', 'raw_customers') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by created_at desc
        ) as row_num
    from source_data
),

cleaned as (
    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        date_of_birth,
        trim(country_of_residence) as country_of_residence,
        case upper(trim(country_of_residence))
            when 'UNITED KINGDOM' then 'GB'
            when 'UNITED STATES' then 'US'
            when 'FRANCE' then 'FR'
            when 'GERMANY' then 'DE'
            when 'SPAIN' then 'ES'
            when 'ITALY' then 'IT'
            when 'AUSTRALIA' then 'AU'
            when 'CANADA' then 'CA'
            when 'INDIA' then 'IN'
            when 'JAPAN' then 'JP'
            when 'BRAZIL' then 'BR'
            when 'NETHERLANDS' then 'NL'
            when 'SWEDEN' then 'SE'
            when 'NORWAY' then 'NO'
            when 'UAE' then 'AE'
            when 'SINGAPORE' then 'SG'
            when 'SOUTH AFRICA' then 'ZA'
            when 'THAILAND' then 'TH'
            when 'MEXICO' then 'MX'
            when 'IRELAND' then 'IE'
            else 'UN'
        end as country_iso_code,
        trim(phone) as phone,
        try_to_timestamp_ntz(to_varchar(created_at)) as created_at,
        trim(first_name) || ' ' || trim(last_name) as full_name
    from ranked
    where row_num = 1
)

select *
from cleaned
