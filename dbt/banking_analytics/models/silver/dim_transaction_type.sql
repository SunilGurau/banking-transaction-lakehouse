{{ config(materialized='table') }}

select
  transaction_type_code,
  transaction_type_name,
  balance_direction
from {{ ref('stg_reference__transaction_types') }}
