{{ config(materialized='incremental', incremental_strategy='append') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set transactions_uri = batch_table_uris.get('transactions') %}

{% if execute and not transactions_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.transactions is required') %}
{% endif %}

-- Create a temporary view to configure the CSV reader options
{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_transactions_source
        USING csv
        OPTIONS (
            path '{{ transactions_uri }}',
            header 'true',
            inferSchema 'true'
        )
    {% endcall %}
{% endif %}

SELECT * FROM temp_transactions_source