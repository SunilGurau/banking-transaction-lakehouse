{{ config(materialized='table') }}

select
    cast(settlement_date as date) as settlement_date,
    cast(channel as string) as channel,
    cast(transaction_type as string) as transaction_type,
    cast(total_amount as double) as total_amount,
    cast(transaction_count as int) as transaction_count
from {{ ref('stg_batch__settlements') }}
