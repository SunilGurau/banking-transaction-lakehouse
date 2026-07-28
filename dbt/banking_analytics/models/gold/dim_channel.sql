{{ config(materialized='table') }}

select distinct channel from {{ ref('silver_transaction') }} where channel is not null
