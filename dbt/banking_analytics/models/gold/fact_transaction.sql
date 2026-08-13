{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

/*
    fact_transaction — Core transaction fact table
    Grain: one row per unique transaction
    FK references: account_id, customer_id, branch_id, channel,
                   transaction_type_code, merchant_category_code, transaction_date
*/

with transactions as (
    select * from delta.`s3a://silver/silver_transaction`
    {% if is_incremental() %}
    where transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}
)

select
    -- Natural key
    transaction_id,

    -- Dimension foreign keys
    account_id,
    customer_id,
    branch_id,
    channel,
    transaction_type_code,
    merchant_category_code,
    transaction_date,

    -- Transaction timestamp
    transaction_ts,

    -- Measures
    amount,
    fee_amount,
    net_amount,

    -- Attributes
    status,
    currency,
    original_transaction_id,
    source_system
from transactions
