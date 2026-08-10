{{ config(materialized='table', alias='stg_reference_merchant_categories') }}

{% set reference_table_uris = var('reference_table_uris', {}) %}
{% set merchant_categories_uri = reference_table_uris.get('merchant_categories') %}

{% if execute and not merchant_categories_uri %}
    {% do exceptions.raise_compiler_error('reference_table_uris.merchant_categories is required') %}
{% endif %}


{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_merchant_categories_source
        USING csv
        OPTIONS (
            path '{{merchant_categories_uri}}',
            header 'true',
            inferSchema 'true'
        );
    {% endcall %}
{% endif %}

select
    merchant_category_code,
    merchant_category_name,
    risk_category
from temp_merchant_categories_source
