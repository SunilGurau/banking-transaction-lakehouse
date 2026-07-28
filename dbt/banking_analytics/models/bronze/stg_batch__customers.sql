{{ config(materialized='table') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set customers_uri = batch_table_uris.get('customers') %}

{% if execute and not customers_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.customers is required') %}
{% endif %}

select * from delta.`{{ customers_uri | default('dummy', true) }}`
