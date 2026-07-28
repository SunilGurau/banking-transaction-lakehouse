{{ config(materialized='table') }}

select distinct transaction_type from {{ ref('silver_transaction') }} where transaction_type is not null
