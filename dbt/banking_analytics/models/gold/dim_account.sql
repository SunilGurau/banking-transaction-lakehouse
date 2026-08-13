{{ config(materialized='table') }}

with account_source as (
    select * from delta.`s3a://silver/silver_account`
)

select
    row_number() over (order by account_id)                 as account_key,
    account_id,
    customer_id,
    branch_id,
    account_type,
    account_status,
    opened_date,
    currency
from account_source
