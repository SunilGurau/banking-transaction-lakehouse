{{ config(materialized='table') }}

select
    cast(customer_id as string) as customer_id,
    cast(first_name as string) as first_name,
    cast(last_name as string) as last_name,
    cast(segment as string) as segment,
    cast(risk_band as string) as risk_band,
    cast(province as string) as province
from {{ ref('stg_batch__customers') }}
