{{ config(materialized='table') }}

select
    cast(transaction_id as string) as transaction_id,
    cast(account_id as string) as account_id,
    cast(transaction_date as timestamp) as transaction_date,
    cast(amount as double) as amount,
    cast(fee as double) as fee,
    cast(status as string) as status,
    cast(channel as string) as channel,
    cast(transaction_type as string) as transaction_type
from {{ ref('stg_batch__transactions') }}
