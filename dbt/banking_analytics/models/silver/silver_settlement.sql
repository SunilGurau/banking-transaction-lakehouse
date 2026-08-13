{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with source as (
    select * from delta.`s3a://bronze/stg_batch__settlements`
    {% if is_incremental() %}
    where cast(settlement_date as date) > (select max(settlement_date) from {{ this }})
    {% endif %}
),

-- deduplicated as (
--     select
--         *,
--         row_number() over (
--             partition by settlement_batch_id
--             order by settlement_date desc
--         ) as _row_num
--     from source
-- ),

cleaned as (
    select
        cast(settlement_batch_id as string)                 as settlement_batch_id,
        cast(settlement_date as date)                       as settlement_date,
        cast(channel as string)                             as channel,
        cast(transaction_type_code as string)               as transaction_type_code,
        cast(settled_transaction_count as int)               as settled_transaction_count,
        cast(settled_gross_amount as double)                 as settled_gross_amount,
        cast(settled_fee_amount as double)                   as settled_fee_amount,
        cast(currency as string)                            as currency,
        current_timestamp()                                 as dbt_loaded_at
    -- from deduplicated
    -- where _row_num = 1
    from source
)

select * from cleaned
