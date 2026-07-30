{{ config(materialized='table', alias='stg_reference_merchant_categories') }}

{% set reference_table_uris = var('reference_table_uris', {}) %}
{% set merchant_categories_uri = reference_table_uris.get('merchant_categories') %}

{% if execute and not merchant_categories_uri %}
    {% do exceptions.raise_compiler_error('reference_table_uris.merchant_categories is required') %}
{% endif %}

select
    merchant_category_code,
    merchant_category_name,
    risk_category
from delta.`{{ merchant_categories_uri }}`
