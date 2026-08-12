{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_batch__accounts') }}
),

-- deduplicated as (
--     select
--         *,
--         row_number() over (
--             partition by account_id
--             order by snapshot_date desc
--         ) as _row_num
--     from source
-- ),

cleaned as (
    select
        cast(account_id as string)                          as account_id,
        cast(customer_id as string)                         as customer_id,
        cast(branch_id as string)                           as branch_id,
        cast(account_type as string)                        as account_type,
        cast(account_status as string)                      as account_status,
        cast(opened_date as date)                           as opened_date,
        cast(currency as string)                            as currency,
        cast(snapshot_date as date)                         as snapshot_date,
        current_timestamp()                                 as dbt_loaded_at
    -- from deduplicated
    -- where _row_num = 1

    from source
)

select * from cleaned
