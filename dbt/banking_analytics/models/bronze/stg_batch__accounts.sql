{{ config(materialized='table') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set accounts_uri = batch_table_uris.get('accounts') %}

{% if execute and not accounts_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.accounts is required') %}
{% endif %}

select * from delta.`{{ accounts_uri | default('dummy', true) }}`
