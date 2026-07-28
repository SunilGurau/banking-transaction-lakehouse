{{ config(materialized='table') }}

with settlement as (
    select * from {{ ref('silver_settlement') }}
),
transactions as (
    select
        cast(transaction_date as date) as settlement_date,
        channel,
        transaction_type,
        sum(amount) as transaction_total_amount,
        count(transaction_id) as transaction_total_count
    from {{ ref('silver_transaction') }}
    where status = 'COMPLETED'
    group by 1, 2, 3
)
select
    s.settlement_date,
    s.channel,
    s.transaction_type,
    s.total_amount as batch_total_amount,
    t.transaction_total_amount as transaction_total_amount,
    s.total_amount - coalesce(t.transaction_total_amount, 0) as variance_amount,
    s.transaction_count as batch_total_count,
    t.transaction_total_count as transaction_total_count,
    s.transaction_count - coalesce(t.transaction_total_count, 0) as variance_count,
    case when s.total_amount = coalesce(t.transaction_total_amount, 0) then 'MATCHED' else 'UNMATCHED' end as status
from settlement s
left join transactions t 
    on s.settlement_date = t.settlement_date
    and s.channel = t.channel
    and s.transaction_type = t.transaction_type
