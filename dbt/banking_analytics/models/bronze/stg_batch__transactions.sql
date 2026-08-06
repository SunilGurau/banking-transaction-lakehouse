{{ config(materialized='incremental', incremental_strategy='append') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set transactions_uri = batch_table_uris.get('transactions') %}

{% if execute and not transactions_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.transactions is required') %}
{% endif %}

select * from csv.`{{ transactions_uri | default('dummy', true) }}`
