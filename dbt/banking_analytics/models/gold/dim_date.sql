{{ config(materialized='table') }}

-- Generate a date spine from 2025-01-01 to 2027-12-31.
-- Uses Spark's sequence + explode to generate one row per date.

with date_spine as (
    select
        explode(
            sequence(
                cast('2025-01-01' as date),
                cast('2027-12-31' as date),
                interval 1 day
            )
        ) as full_date
)

select
    cast(date_format(full_date, 'yyyyMMdd') as int)         as date_key,
    full_date,
    dayofweek(full_date)                                    as day_of_week,
    date_format(full_date, 'EEEE')                          as day_name,
    month(full_date)                                        as month_num,
    date_format(full_date, 'MMMM')                          as month_name,
    quarter(full_date)                                      as quarter_num,
    year(full_date)                                         as year_num,
    case
        when dayofweek(full_date) in (1, 7) then true
        else false
    end                                                     as is_weekend,

    -- Nepali fiscal year runs Mid-July to Mid-July (simplified: Jul 16 to Jul 15).
    -- For analytics, approximate: fiscal year = calendar year if month >= 7, else year - 1
    case
        when month(full_date) >= 7 then year(full_date) + 1
        else year(full_date)
    end                                                     as fiscal_year,
    case
        when month(full_date) >= 7 then quarter(full_date) - 2
        else quarter(full_date) + 2
    end                                                     as fiscal_quarter
from date_spine
