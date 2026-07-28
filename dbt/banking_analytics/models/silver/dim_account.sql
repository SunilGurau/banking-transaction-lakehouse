{{ config(materialized='table') }}

select
    cast(account_id as string) as account_id,
    cast(customer_id as string) as customer_id,
    cast(branch_id as string) as branch_id,
    cast(account_type as string) as account_type,
    cast(status as string) as status
from {{ ref('stg_batch__accounts') }}
