with source_data as (
    select *
    from {{ source('bronze', 'raw_customers') }}
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
        created_at,
        trim(first_name) || ' ' || trim(last_name) as full_name
    from source_data
)

select *
from cleaned
