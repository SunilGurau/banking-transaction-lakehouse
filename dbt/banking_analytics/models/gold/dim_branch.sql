{{ config(materialized='table') }}

with branch_source as (
    select * from {{ ref('stg_reference__branches') }}
)

select
    row_number() over (order by branch_id)                  as branch_key,
    cast(branch_id as string)                               as branch_id,
    cast(branch_name as string)                             as branch_name,
    cast(province as string)                                as province,
    cast(region as string)                                  as region,
    cast(opened_date as date)                               as opened_date,
    case
        when lower(cast(is_active as string)) = 'true' then true
        else false
    end                                                     as is_active
from branch_source
