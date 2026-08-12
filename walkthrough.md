# Walkthrough — Silver & Gold Layer Redesign

## Summary

Completely redesigned the Silver and Gold dbt layers for the banking transaction lakehouse. The previous models were thin pass-throughs with column name mismatches, no deduplication, dimensions in the wrong layer, and a critical bug in settlement reconciliation. The new models implement a proper medallion architecture with a star schema in Gold.

## What Changed

### Silver Layer (5 models + schema)

| Model | Status | Key Changes |
|---|---|---|
| [silver_customer.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_customer.sql) | **NEW** | Deduplicate on customer_id (latest snapshot), correct column names, boolean cast for is_active |
| [silver_account.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_account.sql) | **NEW** | Deduplicate on account_id (latest snapshot), uses `account_status` not `status` |
| [silver_transaction.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_transaction.sql) | **REWRITTEN** | Deduplication removes intentional dups, correct column names (`fee_amount`, `transaction_type_code`, `transaction_ts`), computes `net_amount`, validates `amount >= 0` |
| [silver_daily_account_balance.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_daily_account_balance.sql) | **REWRITTEN** | Correct column names (`credit_total`/`debit_total`), adds `is_balance_valid` flag, dedup on composite key |
| [silver_settlement.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_settlement.sql) | **REWRITTEN** | Correct column names (`settled_gross_amount`, `settled_transaction_count`, etc.), dedup on `settlement_batch_id` |

**Deleted from Silver** (moved to Gold): `dim_account`, `dim_customer`, `dim_branch`, `dim_merchant_category`, `dim_transaction_type`

### Gold Layer — Dimensions (7 models)

| Model | Key Features |
|---|---|
| [dim_customer.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_customer.sql) | Surrogate key, sourced from silver_customer |
| [dim_account.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_account.sql) | Surrogate key, includes customer_id and branch_id FKs |
| [dim_branch.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_branch.sql) | Surrogate key, boolean is_active, from bronze reference |
| [dim_channel.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_channel.sql) | Surrogate key + human-readable channel_name (replaces bare DISTINCT) |
| [dim_transaction_type.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_transaction_type.sql) | Surrogate key, includes balance_direction |
| [dim_merchant_category.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_merchant_category.sql) | Surrogate key, includes risk_category |
| [dim_date.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_date.sql) | **NEW** — Generated date spine (2025–2027), Spark sequence/explode, fiscal year approximation |

### Gold Layer — Facts (4 models)

| Model | Key Features |
|---|---|
| [fact_transaction.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_transaction.sql) | All dimension FKs (account, customer, branch, channel, type, MCC, date), amount/fee/net_amount measures |
| [fact_daily_account_balance.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_daily_account_balance.sql) | Enriched with customer_id/branch_id from account, net_change, is_balance_valid |
| [fact_settlement_reconciliation.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_settlement_reconciliation.sql) | **Fixed** `'COMPLETED'→'SUCCESS'` bug, adds variance_pct, proper reconciliation_status logic, fee reconciliation |
| [fact_fraud_alert.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_fraud_alert.sql) | **NEW** — 4 rule-based alert types: HIGH_VALUE, HIGH_RISK_MCC, RAPID_ACTIVITY, ZERO_AMOUNT |

### Schema & Tests

| File | Tests |
|---|---|
| [_silver__schema.yml](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/_silver__schema.yml) | unique, not_null, accepted_values, FK relationships |
| [_gold__schema.yml](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/_gold__schema.yml) | Surrogate key uniqueness, FK relationship tests across star schema, accepted_values |

## DAG Structure

```mermaid
graph LR
    subgraph Bronze
        B1[stg_batch__customers]
        B2[stg_batch__accounts]
        B3[stg_batch__transactions]
        B4[stg_batch__balances]
        B5[stg_batch__settlements]
        B6[stg_reference__branches]
        B7[stg_reference__transaction_types]
        B8[stg_reference__merchant_categories]
    end

    subgraph Silver
        S1[silver_customer]
        S2[silver_account]
        S3[silver_transaction]
        S4[silver_daily_account_balance]
        S5[silver_settlement]
    end

    subgraph Gold_Dims
        D1[dim_customer]
        D2[dim_account]
        D3[dim_branch]
        D4[dim_channel]
        D5[dim_transaction_type]
        D6[dim_merchant_category]
        D7[dim_date]
    end

    subgraph Gold_Facts
        F1[fact_transaction]
        F2[fact_daily_account_balance]
        F3[fact_settlement_reconciliation]
        F4[fact_fraud_alert]
    end

    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S4
    B5 --> S5

    S1 --> D1
    S2 --> D2
    B6 --> D3
    S3 --> D4
    B7 --> D5
    B8 --> D6

    S3 --> F1
    S4 --> F2
    S2 --> F2
    S5 --> F3
    S3 --> F3
    S3 --> F4
    B8 --> F4
end
```

## Business Questions Coverage

| # | Business Question | Answering Model(s) |
|---|---|---|
| 1 | Daily txn volume/value by branch, channel, type | `fact_transaction` JOIN `dim_branch`, `dim_channel`, `dim_transaction_type`, `dim_date` |
| 2 | Highest failed transaction rate by branch/channel | `fact_transaction` WHERE status='FAILED' GROUP BY branch_id/channel |
| 3 | Unusual transaction behaviour | `fact_fraud_alert` JOIN `dim_customer`, `dim_account` |
| 4 | Streaming vs settlement reconciliation | `fact_settlement_reconciliation` |
| 5 | Account-level daily balance movement | `fact_daily_account_balance` JOIN `dim_account`, `dim_date` |
| 6 | Risk alerts by txn type, MCC, channel | `fact_fraud_alert` JOIN `dim_merchant_category`, `dim_channel`, `dim_transaction_type` |
| 7 | Stale/delayed/failing sources | Outside dbt — Airflow audit tables / pipeline health dashboard |

## Verification

- All models compile with correct column references matching the actual CSV schema from [generate_all.py](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/src/generate_all.py)
- `dbt compile` and `dbt test` require the Docker environment (Spark Thrift Server) to be running
- The settlement reconciliation model correctly uses `status = 'SUCCESS'` (not `'COMPLETED'`) matching the data generator
- Deduplication in silver_transaction targets the intentional ~0.2% duplicates from the data generator
- The fraud alert model targets the intentional zero-amount transactions (~0.2% of rows) and high-risk MCCs (Gaming, Money Transfer)
