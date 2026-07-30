{{ config(materialized='table', alias='stg_reference_transaction_types') }}

{% set reference_table_uris = var('reference_table_uris', {}) %}
{% set transaction_types_uri = reference_table_uris.get('transaction_types') %}

{% if execute and not transaction_types_uri %}
    {% do exceptions.raise_compiler_error('reference_table_uris.transaction_types is required') %}
{% endif %}

select
    transaction_type_code,
    transaction_type_name,
    balance_direction
from delta.`{{ transaction_types_uri }}`
