{{ config(materialized='table') }}

select
    transaction_id,
    account_id,
    transaction_date,
    amount,
    fee,
    status,
    channel,
    transaction_type
from {{ ref('silver_transaction') }}
