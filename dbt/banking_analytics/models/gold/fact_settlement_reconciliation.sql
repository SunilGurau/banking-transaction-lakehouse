{{ config(materialized='table') }}

/*
    fact_settlement_reconciliation — Reconciliation fact
    Grain: one row per settlement_date / channel / transaction_type_code

    Compares settlement file totals against actual transaction aggregates
    to surface mismatches. The data generator intentionally introduces ~12%
    settlement amount discrepancies for quality-check exercises.
*/

with settlement as (
    select
        settlement_batch_id,
        settlement_date,
        channel,
        transaction_type_code,
        settled_transaction_count,
        settled_gross_amount,
        settled_fee_amount,
        currency
    from {{ ref('silver_settlement') }}
),

-- Aggregate actual transactions for the same grain
transaction_aggregates as (
    select
        transaction_date                                    as settlement_date,
        channel,
        transaction_type_code,
        count(transaction_id)                               as actual_transaction_count,
        round(sum(amount), 2)                               as actual_gross_amount,
        round(sum(fee_amount), 2)                           as actual_fee_amount
    from {{ ref('silver_transaction') }}
    where status = 'SUCCESS'
    group by
        transaction_date,
        channel,
        transaction_type_code
)

select
    -- Keys
    s.settlement_batch_id,
    s.settlement_date,
    s.channel,
    s.transaction_type_code,

    -- Settlement side
    s.settled_transaction_count,
    s.settled_gross_amount,
    s.settled_fee_amount,

    -- Transaction side
    coalesce(t.actual_transaction_count, 0)                 as actual_transaction_count,
    coalesce(t.actual_gross_amount, 0)                      as actual_gross_amount,
    coalesce(t.actual_fee_amount, 0)                        as actual_fee_amount,

    -- Variances
    s.settled_transaction_count
        - coalesce(t.actual_transaction_count, 0)           as count_variance,
    round(s.settled_gross_amount
        - coalesce(t.actual_gross_amount, 0), 2)            as amount_variance,
    case
        when coalesce(t.actual_gross_amount, 0) = 0 then null
        else round(
            (s.settled_gross_amount - t.actual_gross_amount)
            / t.actual_gross_amount * 100, 4
        )
    end                                                     as variance_pct,

    -- Reconciliation status
    case
        when abs(s.settled_gross_amount
                 - coalesce(t.actual_gross_amount, 0)) < 0.01
             and s.settled_transaction_count
                 = coalesce(t.actual_transaction_count, 0)
        then 'MATCHED'
        else 'UNMATCHED'
    end                                                     as reconciliation_status,

    s.currency
from settlement s
left join transaction_aggregates t
    on s.settlement_date = t.settlement_date
    and s.channel = t.channel
    and s.transaction_type_code = t.transaction_type_code
