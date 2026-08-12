{{ config(materialized='table') }}

{% set batch_table_uris = var('batch_table_uris', {}) %}
{% set accounts_uri = batch_table_uris.get('accounts') %}

{% if execute and not accounts_uri %}
    {% do exceptions.raise_compiler_error('batch_table_uris.accounts is required') %}
{% endif %}

{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_accounts_source
        USING csv
        OPTIONS (
            path '{{accounts_uri}}',
            header 'true',
            inferSchema 'true'
        );
    {% endcall %}
{% endif %}

select * from temp_accounts_source
