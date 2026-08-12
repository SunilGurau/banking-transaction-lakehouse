{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_batch__customers') }}
),

-- deduplicated as (
--     select
--         *,
--         row_number() over (
--             partition by customer_id
--             order by snapshot_date desc
--         ) as _row_num
--     from source
-- ),

cleaned as (
    select
        cast(customer_id as string)                         as customer_id,
        cast(customer_name as string)                       as customer_name,
        cast(segment as string)                             as segment,
        cast(age_band as string)                            as age_band,
        cast(province as string)                            as province,
        cast(risk_band as string)                           as risk_band,
        cast(created_date as date)                          as created_date,
        cast(snapshot_date as date)                         as snapshot_date,
        case
            when lower(cast(is_active as string)) = 'true' then true
            else false
        end                                                 as is_active,
        current_timestamp()                                 as dbt_loaded_at
    -- from deduplicated
    -- where _row_num = 1
    from source
)

select * from cleaned
