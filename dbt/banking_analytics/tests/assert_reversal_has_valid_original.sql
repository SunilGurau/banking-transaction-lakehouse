-- dbt/banking_analytics/tests/assert_reversal_has_valid_original.sql
-- Fails if any REVERSED transaction has a missing or non-existent
-- original_transaction_id. dbt tests fail on non-empty result sets.

with reversed as (
    select transaction_id, original_transaction_id
    from {{ ref('stg_batch__transactions') }}
    where status = 'REVERSED'
),

orphans as (
    select r.transaction_id, r.original_transaction_id
    from reversed r
    left join {{ ref('stg_batch__transactions') }} t
        on r.original_transaction_id = t.transaction_id
    where r.original_transaction_id is null
       or r.original_transaction_id = ''
       or t.transaction_id is null
)

select * from orphans