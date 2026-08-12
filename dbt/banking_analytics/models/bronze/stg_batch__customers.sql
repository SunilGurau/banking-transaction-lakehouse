{{ config(materialized='table') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set customers_uri = batch_table_uris.get('customers') %}

{% if execute and not customers_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.customers is required') %}
{% endif %}

{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_customers_source
        USING csv
        OPTIONS (
            path '{{customers_uri}}',
            header 'true',
            inferSchema 'true'
        );
    {% endcall %}
{% endif %}

select * from temp_customers_source
