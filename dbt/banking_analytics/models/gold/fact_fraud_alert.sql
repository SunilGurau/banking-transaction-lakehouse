{{ config(materialized='table') }}

/*
    fact_fraud_alert — Rule-based risk alert fact
    Grain: one row per flagged transaction (a transaction may trigger multiple alerts)

    Rules:
    1. HIGH_VALUE     — Transaction amount exceeds 100,000 NPR
    2. HIGH_RISK_MCC  — Merchant category classified as HIGH risk (Gaming, Money Transfer)
    3. RAPID_ACTIVITY — More than 5 transactions from the same account within the same day
    4. ZERO_AMOUNT    — Transaction with zero amount (potential test/probing)
*/

with transactions as (
    select * from {{ ref('silver_transaction') }}
),

mcc_risk as (
    select
        merchant_category_code,
        risk_category
    from {{ ref('stg_reference__merchant_categories') }}
),

-- Count transactions per account per day for rapid-activity detection
daily_account_counts as (
    select
        account_id,
        transaction_date,
        count(*) as daily_txn_count
    from transactions
    where status = 'SUCCESS'
    group by account_id, transaction_date
),

-- Rule 1: High value transactions
high_value_alerts as (
    select
        transaction_id,
        account_id,
        customer_id,
        branch_id,
        channel,
        transaction_type_code,
        merchant_category_code,
        transaction_date,
        transaction_ts,
        amount,
        'HIGH_VALUE' as alert_type,
        'Transaction amount exceeds 100,000 NPR' as alert_description,
        'HIGH' as alert_severity
    from transactions
    where amount > 100000
      and status = 'SUCCESS'
),

-- Rule 2: High risk MCC
high_risk_mcc_alerts as (
    select
        t.transaction_id,
        t.account_id,
        t.customer_id,
        t.branch_id,
        t.channel,
        t.transaction_type_code,
        t.merchant_category_code,
        t.transaction_date,
        t.transaction_ts,
        t.amount,
        'HIGH_RISK_MCC' as alert_type,
        concat('High-risk merchant category: ', m.risk_category) as alert_description,
        'MEDIUM' as alert_severity
    from transactions t
    inner join mcc_risk m
        on t.merchant_category_code = m.merchant_category_code
        and m.risk_category = 'HIGH'
    where t.status = 'SUCCESS'
),

-- Rule 3: Rapid activity (> 5 txns per account per day)
rapid_activity_alerts as (
    select
        t.transaction_id,
        t.account_id,
        t.customer_id,
        t.branch_id,
        t.channel,
        t.transaction_type_code,
        t.merchant_category_code,
        t.transaction_date,
        t.transaction_ts,
        t.amount,
        'RAPID_ACTIVITY' as alert_type,
        concat('Account had ', d.daily_txn_count, ' transactions in one day') as alert_description,
        'MEDIUM' as alert_severity
    from transactions t
    inner join daily_account_counts d
        on t.account_id = d.account_id
        and t.transaction_date = d.transaction_date
    where d.daily_txn_count > 5
      and t.status = 'SUCCESS'
),

-- Rule 4: Zero amount (probing/testing)
zero_amount_alerts as (
    select
        transaction_id,
        account_id,
        customer_id,
        branch_id,
        channel,
        transaction_type_code,
        merchant_category_code,
        transaction_date,
        transaction_ts,
        amount,
        'ZERO_AMOUNT' as alert_type,
        'Transaction with zero amount detected' as alert_description,
        'LOW' as alert_severity
    from transactions
    where amount = 0
      and status = 'SUCCESS'
),

all_alerts as (
    select * from high_value_alerts
    union all
    select * from high_risk_mcc_alerts
    union all
    select * from rapid_activity_alerts
    union all
    select * from zero_amount_alerts
)

select
    row_number() over (order by transaction_date, transaction_id, alert_type) as alert_key,
    *
from all_alerts
