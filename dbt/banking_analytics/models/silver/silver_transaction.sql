{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with source as (
    select * from delta.`s3a://bronze/stg_batch__transactions`
    {% if is_incremental() %}
    where cast(transaction_date as date) > (select max(transaction_date) from {{ this }})
    {% endif %}
),

-- The data generator intentionally inserts duplicate transactions.
-- Deduplicate by keeping the first occurrence per transaction_id.
deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by transaction_ts desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(transaction_id as string)                      as transaction_id,
        cast(account_id as string)                          as account_id,
        cast(customer_id as string)                         as customer_id,
        cast(branch_id as string)                           as branch_id,
        cast(transaction_ts as timestamp)                   as transaction_ts,
        cast(transaction_date as date)                      as transaction_date,
        cast(transaction_type_code as string)               as transaction_type_code,
        cast(channel as string)                             as channel,
        cast(merchant_category_code as string)              as merchant_category_code,
        cast(amount as double)                              as amount,
        cast(fee_amount as double)                          as fee_amount,
        round(cast(amount as double) - cast(fee_amount as double), 2)
                                                            as net_amount,
        cast(status as string)                              as status,
        cast(currency as string)                            as currency,
        cast(original_transaction_id as string)             as original_transaction_id,
        cast(source_system as string)                       as source_system,
        current_timestamp()                                 as dbt_loaded_at
    from deduplicated
    where _row_num = 1
      and amount is not null
      and amount >= 0
)

select * from cleaned
