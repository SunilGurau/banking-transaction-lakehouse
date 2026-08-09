{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['balance_date', 'account_id']
) }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set balances_uri = batch_table_uris.get('balances') %}

{% if execute and not balances_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.balances is required') %}
{% endif %}

-- Create a temporary view to configure the CSV reader options
{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_balances_source
        USING csv
        OPTIONS (
            path '{{ balances_uri }}',
            header 'true',
            inferSchema 'true'
        )
    {% endcall %}
{% endif %}

SELECT *
FROM temp_balances_source
