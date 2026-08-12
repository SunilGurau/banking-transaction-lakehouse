# Task Tracker — Silver & Gold Layer Redesign

## Silver Layer
- [x] Delete misplaced dimension models from Silver (5 files)
- [x] Create `silver_customer.sql`
- [x] Create `silver_account.sql`
- [x] Rewrite `silver_transaction.sql`
- [x] Rewrite `silver_daily_account_balance.sql`
- [x] Rewrite `silver_settlement.sql`

## Gold Layer — Dimensions
- [x] Rewrite `dim_channel.sql`
- [x] Create `dim_customer.sql`
- [x] Create `dim_account.sql`
- [x] Create `dim_branch.sql`
- [x] Create `dim_merchant_category.sql`
- [x] Create `dim_transaction_type.sql`
- [x] Create `dim_date.sql`

## Gold Layer — Facts
- [x] Rewrite `fact_transaction.sql`
- [x] Rewrite `fact_daily_account_balance.sql`
- [x] Rewrite `fact_settlement_reconciliation.sql`
- [x] Create `fact_fraud_alert.sql`

## Schema & Tests
- [x] Create `_silver__schema.yml`
- [x] Create `_gold__schema.yml`

## Verification
- [ ] `dbt compile` passes (requires Docker environment)
- [x] Walkthrough artifact
