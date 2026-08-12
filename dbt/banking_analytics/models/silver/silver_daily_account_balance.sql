{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_batch__balances') }}
),

-- deduplicated as (
--     select
--         *,
--         row_number() over (
--             partition by account_id, balance_date
--             order by balance_date desc
--         ) as _row_num
--     from source
-- ),

cleaned as (
    select
        cast(account_id as string)                          as account_id,
        cast(balance_date as date)                          as balance_date,
        cast(opening_balance as double)                     as opening_balance,
        cast(credit_total as double)                        as credit_total,
        cast(debit_total as double)                         as debit_total,
        cast(closing_balance as double)                     as closing_balance,
        cast(currency as string)                            as currency,

        -- Validate that closing = opening + credits - debits
        -- validation without failing the pipeline, but instead flagging the record as invalid
        case
            when abs(
                cast(closing_balance as double)
                - (cast(opening_balance as double)
                   + cast(credit_total as double)
                   - cast(debit_total as double))
            ) < 0.01 then true
            else false
        end                                                 as is_balance_valid,

        current_timestamp()                                 as dbt_loaded_at
    -- from deduplicated
    -- where _row_num = 1
    from source
)

select * from cleaned
