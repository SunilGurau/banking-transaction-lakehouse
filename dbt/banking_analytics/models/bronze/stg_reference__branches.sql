{{ config(materialized='table', alias='stg_reference_branches') }}

{% set reference_table_uris = var('reference_table_uris', {}) %}
{% set branches_uri = reference_table_uris.get('branches') %}

{% if execute and not branches_uri %}
    {% do exceptions.raise_compiler_error('reference_table_uris.branches is required') %}
{% endif %}


{% if execute %}
    {% call statement('create_temp_view', auto_begin=false) %}
        CREATE OR REPLACE TEMPORARY VIEW temp_branches_source
        USING csv
        OPTIONS (
            path '{{branches_uri}}',
            header 'true',
            inferSchema 'true'
        );
    {% endcall %}
{% endif %}

select *
from temp_branches_source