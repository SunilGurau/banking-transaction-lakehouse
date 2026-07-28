{{ config(materialized='table') }}

select
    account_id,
    balance_date,
    opening_balance,
    debit_amount,
    credit_amount,
    closing_balance
from {{ ref('silver_daily_account_balance') }}
