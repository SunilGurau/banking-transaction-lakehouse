{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['settlement_batch_id']
) }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set settlements_uri = batch_table_uris.get('settlements') %}

{% if execute and not settlements_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.settlements is required') %}
{% endif %}

-- Create a temporary view to configure the CSV reader options
{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_settlements_source
        USING csv
        OPTIONS (
            path '{{ settlements_uri }}',
            header 'true',
            inferSchema 'true'
        )
    {% endcall %}
{% endif %}

SELECT *
FROM temp_settlements_source
