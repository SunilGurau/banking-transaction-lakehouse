{{ config(materialized='table') }}

with customer_source as (
    select * from {{ ref('silver_customer') }}
)

select
    row_number() over (order by customer_id)                as customer_key,
    customer_id,
    customer_name,
    segment,
    age_band,
    province,
    risk_band,
    is_active,
    created_date
from customer_source
