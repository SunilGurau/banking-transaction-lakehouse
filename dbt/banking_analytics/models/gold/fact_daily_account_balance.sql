{{ config(materialized='table') }}

/*
    fact_daily_account_balance — Periodic snapshot fact
    Grain: one row per account per day
    FK references: account_id (→ dim_account), balance_date (→ dim_date)
    Enriched with customer_id and branch_id from the account dimension.
*/

with balances as (
    select * from {{ ref('silver_daily_account_balance') }}
),

accounts as (
    select
        account_id,
        customer_id,
        branch_id
    from {{ ref('silver_account') }}
)

select
    -- Dimension foreign keys
    b.account_id,
    a.customer_id,
    a.branch_id,
    b.balance_date,

    -- Measures
    b.opening_balance,
    b.credit_total,
    b.debit_total,
    b.closing_balance,
    round(b.credit_total - b.debit_total, 2)                as net_change,

    -- Data quality flag
    b.is_balance_valid,
    b.currency
from balances b
left join accounts a
    on b.account_id = a.account_id
