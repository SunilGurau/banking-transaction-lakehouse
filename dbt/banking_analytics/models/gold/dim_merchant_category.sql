{{ config(materialized='table') }}

with mcc_source as (
    select * from delta.`s3a://bronze/stg_reference_merchant_categories`
)

select
    row_number() over (order by merchant_category_code)     as merchant_category_key,
    cast(merchant_category_code as string)                  as merchant_category_code,
    cast(merchant_category_name as string)                  as merchant_category_name,
    cast(risk_category as string)                           as risk_category
from mcc_source
