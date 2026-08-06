{{ config(materialized='incremental', incremental_strategy='append') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set balances_uri = batch_table_uris.get('balances') %}

{% if execute and not balances_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.balances is required') %}
{% endif %}

select * from csv.`{{ balances_uri | default('dummy', true) }}`
