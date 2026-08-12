{{ config(materialized='table') }}

with txn_type_source as (
    select * from {{ ref('stg_reference__transaction_types') }}
)

select
    row_number() over (order by transaction_type_code)      as transaction_type_key,
    cast(transaction_type_code as string)                   as transaction_type_code,
    cast(transaction_type_name as string)                   as transaction_type_name,
    cast(balance_direction as string)                       as balance_direction
from txn_type_source
