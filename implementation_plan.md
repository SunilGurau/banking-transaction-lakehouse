# Redesign Silver & Gold dbt Layers for Banking Lakehouse

## Problem Statement

The current Silver and Gold models are thin pass-throughs that add almost no value over the Bronze layer:

- **Silver models** just `cast` columns — no deduplication, no cleansing, no enrichment, no null handling. Dimensions (`dim_account`, `dim_customer`, etc.) are placed in Silver but belong in Gold per the medallion architecture.
- **Gold models** are pass-throughs of Silver (`SELECT * FROM silver_x`) — no dimensional modelling, no surrogate keys, no FK relationships, no business logic. `fact_transaction` has no branch/customer foreign keys. `dim_channel` is just a `SELECT DISTINCT`. `fact_settlement_reconciliation` filters on `status = 'COMPLETED'` but the data uses `'SUCCESS'`.
- **No schema YAML files** — zero tests, zero documentation.
- **Column name mismatches** between bronze source columns (from CSV, e.g., `fee_amount`, `account_status`, `transaction_type_code`) and silver models that reference non-existent columns (`fee`, `status`, `transaction_type`, `first_name`/`last_name`, `debit_amount`/`credit_amount`).

## Proposed Architecture

Restructure models into a clean 3-tier medallion architecture:

```
Bronze (stg_*)          Silver (silver_*)         Gold (dim_*/fact_*)
────────────────        ─────────────────         ──────────────────
Raw CSV parse           Cleaned, deduplicated,    Business-ready star
Minimal transforms      enriched, conformed       schema with surrogate
                                                  keys, FK refs, tests
```

> [!IMPORTANT]
> **Key design decision**: Dimensions move to Gold. Silver contains only cleaned/conformed entity tables. Gold builds the star schema on top.

---

## Proposed Changes

### Silver Layer — Clean, Deduplicate, Conform, Enrich

Silver models should produce trusted, conformed data. Every Silver model will:
- Use correct source column names (matching the actual CSV/bronze schema)
- Deduplicate (the transaction CSV generator intentionally inserts duplicates)
- Null-coalesce / default critical fields
- Apply valid-value filters where appropriate
- Add audit columns (`dbt_loaded_at`)

#### [MODIFY] [silver_transaction.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_transaction.sql)
Complete rewrite:
- Fix column names to match bronze: `transaction_type_code` (not `transaction_type`), `fee_amount` (not `fee`), `transaction_ts` (not just `transaction_date`), `merchant_category_code`, `customer_id`, `branch_id`, `source_system`, `original_transaction_id`, `currency`
- Deduplicate on `transaction_id` (keeps latest by `transaction_ts`)
- Validate `amount >= 0`, `status` in known values
- Cast types properly (timestamp, double, string)
- Add `transaction_date` as a proper DATE derived from `transaction_ts`
- Add `dbt_loaded_at` audit timestamp

#### [MODIFY] [silver_daily_account_balance.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_daily_account_balance.sql)
- Fix column names: `credit_total` (not `credit_amount`), `debit_total` (not `debit_amount`)
- Add `currency` column
- Add balance validation: `closing_balance = opening_balance + credit_total - debit_total` as a boolean flag
- Deduplicate on `(account_id, balance_date)`

#### [MODIFY] [silver_settlement.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/silver_settlement.sql)
- Fix column names: `transaction_type_code` (not `transaction_type`), `settled_gross_amount` (not `total_amount`), `settled_transaction_count` (not `transaction_count`), `settled_fee_amount`, `settlement_batch_id`
- Include all settlement columns from source
- Deduplicate on `settlement_batch_id`

#### [DELETE] [dim_account.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/dim_account.sql)
#### [DELETE] [dim_customer.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/dim_customer.sql)
#### [DELETE] [dim_branch.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/dim_branch.sql)
#### [DELETE] [dim_merchant_category.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/dim_merchant_category.sql)
#### [DELETE] [dim_transaction_type.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/silver/dim_transaction_type.sql)

These dimensions don't belong in Silver. Their logic moves to Gold.

#### [NEW] silver_customer.sql
- Sourced from `stg_batch__customers`
- Fix column reference: source has `customer_name` (not `first_name`/`last_name`) — split into `first_name`/`last_name` via string splitting, or keep as `customer_name`
- Include `age_band`, `created_date`, `snapshot_date`, `is_active`
- Deduplicate on `customer_id` (keep latest snapshot)

#### [NEW] silver_account.sql
- Sourced from `stg_batch__accounts`
- Fix column reference: source has `account_status` (not `status`)
- Include `opened_date`, `currency`, `snapshot_date`
- Deduplicate on `account_id` (keep latest snapshot)

---

### Gold Layer — Star Schema Dimensional Model

Gold builds the business-ready star schema. Each fact table will carry FK references to dimension surrogate keys. Dimensions will have surrogate keys generated via `row_number()` (or deterministic hashing).

#### [MODIFY] [fact_transaction.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_transaction.sql)
Complete rewrite — the core fact table:
- Grain: one row per unique transaction
- Foreign keys: `account_id`, `customer_id`, `branch_id`, `channel`, `transaction_type_code`, `merchant_category_code`, `transaction_date` (for dim_date)
- Measures: `amount`, `fee_amount`, `net_amount` (amount - fee)
- Include `status`, `original_transaction_id`, `source_system`, `currency`
- Source from `silver_transaction`

#### [MODIFY] [fact_daily_account_balance.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_daily_account_balance.sql)
Rewrite as proper periodic snapshot fact:
- Grain: one row per account per day
- FK to `dim_account` via `account_id`, FK to `dim_date` via `balance_date`
- Measures: `opening_balance`, `credit_total`, `debit_total`, `closing_balance`, `net_change`
- Add `is_balance_valid` flag
- Source from `silver_daily_account_balance`, join to `silver_account` for `customer_id` and `branch_id`

#### [MODIFY] [fact_settlement_reconciliation.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/fact_settlement_reconciliation.sql)
Rewrite reconciliation logic:
- Grain: one row per settlement_date / channel / transaction_type_code
- Fix the `status = 'COMPLETED'` bug → must be `status = 'SUCCESS'`
- Compute: `settled_gross_amount` (from settlement), `transaction_total_amount` (from aggregated silver_transactions), `variance_amount`, `variance_pct`, `reconciliation_status` (MATCHED/UNMATCHED)
- Include counts: `settled_transaction_count`, `actual_transaction_count`, `count_variance`
- Include `settled_fee_amount`, `settlement_batch_id`

#### [MODIFY] [dim_channel.sql](file:///c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dbt/banking_analytics/models/gold/dim_channel.sql)
Rewrite from bare DISTINCT to proper dimension with a surrogate key and channel name.

#### [NEW] dim_customer.sql (Gold)
- Source from `silver_customer`
- Surrogate key via `row_number()` or deterministic hash
- Columns: `customer_key`, `customer_id`, `customer_name`, `segment`, `age_band`, `province`, `risk_band`, `is_active`, `created_date`

#### [NEW] dim_account.sql (Gold)
- Source from `silver_account`
- Surrogate key
- Columns: `account_key`, `account_id`, `customer_id`, `branch_id`, `account_type`, `account_status`, `opened_date`, `currency`

#### [NEW] dim_branch.sql (Gold)
- Source from `stg_reference__branches`
- Surrogate key
- Columns: `branch_key`, `branch_id`, `branch_name`, `province`, `region`, `opened_date`, `is_active`

#### [NEW] dim_merchant_category.sql (Gold)
- Source from `stg_reference__merchant_categories`
- Surrogate key
- Columns: `merchant_category_key`, `merchant_category_code`, `merchant_category_name`, `risk_category`

#### [NEW] dim_transaction_type.sql (Gold)
- Source from `stg_reference__transaction_types`
- Surrogate key
- Columns: `transaction_type_key`, `transaction_type_code`, `transaction_type_name`, `balance_direction`

#### [NEW] dim_date.sql (Gold)
- Generated date spine (Jan 1 2025 → Dec 31 2027)
- Columns: `date_key` (YYYYMMDD int), `full_date`, `day_of_week`, `day_name`, `month_num`, `month_name`, `quarter`, `year`, `is_weekend`, `fiscal_year`, `fiscal_quarter`
- Enables all time-based slicing without runtime date functions

#### [NEW] fact_fraud_alert.sql (Gold) — Optional but answers a required business question
- Rule-based risk flags generated from `silver_transaction`:
  - High-value transactions (amount > threshold)
  - High-risk MCC codes (join to `dim_merchant_category` where `risk_category = 'HIGH'`)
  - Unusual frequency (multiple transactions same account within short window)
- Grain: one row per flagged transaction
- Answers: *"Which accounts/customers show unusual behaviour?"* and *"Which MCCs/channels create the most risk alerts?"*

---

### Schema & Tests

#### [NEW] _silver__schema.yml
- Model descriptions for all silver models
- Column-level `not_null` and `unique` tests on primary keys
- `accepted_values` tests on status, channel, transaction_type_code
- Relationship tests (e.g., `silver_transaction.account_id → silver_account.account_id`)

#### [NEW] _gold__schema.yml
- Model descriptions for all gold models
- Primary key uniqueness tests on all fact and dimension tables
- FK relationship tests (fact → dim)
- `not_null` tests on all dimension surrogate keys and fact measures

---

## Business Questions → Model Mapping

| Business Question | Answered By |
|---|---|
| Daily txn volume/value by branch, channel, type | `fact_transaction` joined to `dim_branch`, `dim_channel`, `dim_transaction_type`, `dim_date` |
| Highest failed transaction rate by branch/channel | `fact_transaction` WHERE status='FAILED', grouped by branch/channel |
| Unusual transaction behaviour | `fact_fraud_alert` + `dim_customer` + `dim_account` |
| Streaming vs settlement reconciliation | `fact_settlement_reconciliation` |
| Account-level daily balance movement | `fact_daily_account_balance` joined to `dim_account`, `dim_date` |
| Risk alerts by txn type, MCC, channel | `fact_fraud_alert` joined to `dim_merchant_category`, `dim_channel`, `dim_transaction_type` |
| Stale/delayed/failing sources | Pipeline observability (outside dbt scope — Airflow audit tables) |

---

## Open Questions

> [!IMPORTANT]
> **SCD Type 2 for customers/accounts?** The instruction mentions SCD2 as optional. Implementing it requires dbt snapshots and adds complexity. My current plan uses current-state dimensions (latest snapshot only). Should I add SCD2 snapshots?

> [!NOTE]
> **Surrogate keys**: I plan to use `row_number()` for surrogate keys since the data volumes are moderate and this keeps things simple without needing a hashing macro. The natural keys (e.g., `customer_id`, `account_id`) will also be preserved. Are you ok with this approach?

> [!NOTE]
> **Fraud alert thresholds**: I'll use reasonable defaults (e.g., amount > 100,000 NPR for high-value, HIGH-risk MCC categories). These can be tuned later via dbt variables.

---

## Verification Plan

### Automated Tests
- `dbt test` — all schema tests in `_silver__schema.yml` and `_gold__schema.yml` must pass
- `dbt compile` — all models must compile without errors

### Manual Verification
- Verify that `fact_settlement_reconciliation` correctly identifies the intentional ~12% mismatches from the data generator
- Verify deduplication in `silver_transaction` removes the ~0.2% intentional duplicates
- Verify `dim_date` spans the expected date range
- Verify `fact_fraud_alert` flags transactions with HIGH-risk MCC codes
