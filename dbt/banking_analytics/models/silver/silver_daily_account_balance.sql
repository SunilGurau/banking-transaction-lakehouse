{{ config(materialized='table') }}

select
    cast(account_id as string) as account_id,
    cast(balance_date as date) as balance_date,
    cast(opening_balance as double) as opening_balance,
    cast(debit_amount as double) as debit_amount,
    cast(credit_amount as double) as credit_amount,
    cast(closing_balance as double) as closing_balance
from {{ ref('stg_batch__balances') }}
