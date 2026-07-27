{{ config(materialized='table') }}

select
  branch_id,
  branch_name,
  province,
  region,
  opened_date,
  is_active
from {{ ref('stg_reference__branches') }}