{{ config(materialized='table', alias='stg_reference_branches') }}

{% set reference_table_uris = var('reference_table_uris', {}) %}
{% set branches_uri = reference_table_uris.get('branches') %}

{% if execute and not branches_uri %}
    {% do exceptions.raise_compiler_error('reference_table_uris.branches is required') %}
{% endif %}

select
    branch_id,
    branch_name,
    province,
    region,
    opened_date,
    cast(is_active as boolean) as is_active
from delta.`{{ branches_uri }}`