with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart='day',
            start_date="to_date('2023-01-01')",
            end_date="to_date('2026-01-01')"
        )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(date_day as string)']) }} as date_sk,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend
from date_spine
where date_day <= to_date('2025-12-31')
