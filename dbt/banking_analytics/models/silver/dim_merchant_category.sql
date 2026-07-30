{{ config(materialized='table') }}

select
  merchant_category_code,
  merchant_category_name,
  risk_category
from {{ ref('stg_reference__merchant_categories') }}
