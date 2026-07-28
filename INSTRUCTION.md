**Batch 14 Final Capstone Briefing**

_Banking Transaction Lakehouse and Fraud Monitoring Platform_

# 1\. Capstone Overview

In this capstone, each team will design and build a local end-to-end data platform for a fictional regional digital bank. The bank receives historical transaction files, customer and account snapshots, daily balance snapshots, settlement files, and near-real-time transaction events. The platform must support transaction analytics, branch and channel reporting, settlement reconciliation, basic fraud monitoring, and pipeline observability.

This is not a simple ETL assignment. Your job is to build a production-like data platform with clear layers, operational controls, data-quality checks, modelling decisions, and a business-facing dashboard.

# 2\. Business Scenario

The fictional bank currently depends on manual CSV reports and inconsistent SQL queries. Operations cannot reliably see daily transaction failures. Finance cannot easily reconcile settlement totals. Risk teams cannot identify suspicious activity quickly. Management wants branch, channel, and customer-segment reporting from a trusted Gold layer.

Your platform should create a controlled path from source data to analytics: raw ingestion, Bronze parsing, Silver cleansing and enrichment, Gold dimensional modelling, data-quality gates, operational audit logs, and dashboards.

# 3\. Required Business Questions

- What is daily transaction volume and value by branch, channel, and transaction type?
- Which branches or channels have the highest failed transaction rate?
- Which accounts or customers show unusual transaction behaviour?
- Are streaming transaction totals reconciling with end-of-day settlement files?
- What is the account-level daily balance movement?
- Which transaction types, merchant categories, or channels create the most risk alerts?
- Which source files, Kafka topics, or tables are stale, delayed, or failing quality checks?

# 4\. Mandatory Technology Scope

| **Technology**                            | **Required Use**                                                                                  |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Python                                    | Synthetic data generation, batch ingestion utilities, validation helpers, Kafka producer support. |
| PostgreSQL                                | Serving layer, operational audit tables, or dashboard backend.                                    |
| Kafka                                     | Streaming transaction events and DLQ or invalid-event topic.                                      |
| Spark                                     | Batch and streaming processing, deduplication, enrichment, lakehouse writes.                      |
| Airflow                                   | Orchestration of ingestion, Spark processing, dbt models, checks, and dashboard refresh.          |
| dbt Core                                  | Staging, intermediate, marts, tests, documentation, and optional snapshots.                       |
| MinIO                                     | Local object storage for landing, Bronze, Silver, and Gold zones.                                 |
| Iceberg or Delta Lake                     | Open table format for lakehouse tables.                                                           |
| Superset, Metabase, Streamlit, or Grafana | Business and operational dashboards.                                                              |
| GitHub                                    | Version control, collaboration, README, issues, and final portfolio submission.                   |

# 5\. Source Datasets

| **Dataset**                | **Type**             | **Description**                                                    | **Expected Use**                                   |
| -------------------------- | -------------------- | ------------------------------------------------------------------ | -------------------------------------------------- |
| branches.csv               | Reference            | Branch ID, region, province, active flag.                          | Branch dimension and branch performance dashboard. |
| customers snapshot         | Master batch         | Synthetic customer profile, segment, risk band, province.          | Customer dimension; optional SCD2.                 |
| accounts snapshot          | Master batch         | Account ID, customer ID, branch ID, type, status.                  | Account dimension and transaction enrichment.      |
| transactions daily CSV     | Batch                | Historical transaction extract with status, channel, amount, type. | Batch fact_transaction load.                       |
| account balances daily CSV | Batch snapshot       | Opening, debit, credit, closing balance by account/day.            | Daily balance snapshot fact.                       |
| settlement daily CSV       | Batch reconciliation | Settlement totals by date/channel/type.                            | Reconciliation fact and finance dashboard.         |
| transaction_events JSONL   | Streaming source     | Kafka-style transaction events with duplicates and late events.    | Streaming Bronze/Silver processing.                |
| malformed_events JSONL     | Bad stream samples   | Invalid or incomplete event records.                               | DLQ and quality-check testing.                     |

# 6\. Target Architecture

Batch path:

Source CSV snapshots -> Python ingestion -> MinIO landing -> Spark batch -> Bronze/Silver lakehouse tables -> dbt Gold -> Trino/PostgreSQL -> dashboard

Streaming path:

Transaction event generator or JSONL replay -> Kafka banking.transactions.raw -> Spark Structured Streaming -> Bronze/Silver -> near-real-time aggregates -> dashboard

Suggested Kafka topics:

- banking.transactions.raw
- banking.transactions.validated
- banking.transactions.dlq
- banking.fraud.alerts optional

Suggested MinIO layout:

landing/source=&lt;source_name&gt;/ingestion_date=YYYY-MM-DD/

bronze/&lt;table_name&gt;/

silver/&lt;table_name&gt;/

gold/&lt;table_name&gt;/

# 7\. Data Layers

| **Layer**   | **Purpose**                                                                 | **Examples**                                                                                |
| ----------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Landing/Raw | Immutable source copy with ingestion metadata. No business transformations. | Original CSV files, raw JSONL/Kafka event dumps, source_manifest.csv.                       |
| Bronze      | Parsed, source-aligned data. Minimal casting and schema checks.             | bronze_transactions, bronze_customers, bronze_accounts, bronze_transaction_events.          |
| Silver      | Cleaned, deduplicated, conformed, enriched data.                            | silver_transactions, silver_accounts, silver_customers, silver_settlements.                 |
| Gold        | Business-ready facts, dimensions, aggregates, and marts.                    | fact_transaction, fact_daily_account_balance, fact_settlement_reconciliation, dim_customer. |

# 8\. Recommended Dimensional Model

| **Table**                      | **Type**               | **Grain**                                | **Notes**                                                          |
| ------------------------------ | ---------------------- | ---------------------------------------- | ------------------------------------------------------------------ |
| fact_transaction               | Transaction fact       | One row per unique transaction           | Core fact. Include amount, fee, status, channel, transaction type. |
| fact_daily_account_balance     | Periodic snapshot fact | One row per account per day              | Opening, credit, debit, closing balances.                          |
| fact_settlement_reconciliation | Reconciliation fact    | One row per date/channel/type            | Batch total, transaction total, variance, status.                  |
| fact_fraud_alert optional      | Event fact             | One row per risk alert                   | Basic rule-based risk flags.                                       |
| dim_customer                   | Dimension              | One row per customer or customer version | Segment, province, risk band. SCD2 optional.                       |
| dim_account                    | Dimension              | One row per account or account version   | Account type, status, branch. SCD2 optional.                       |
| dim_branch                     | Dimension              | One row per branch                       | Region, province, active flag.                                     |
| dim_channel                    | Dimension              | One row per transaction channel          | Branch, ATM, mobile, card, wallet.                                 |
| dim_transaction_type           | Dimension              | One row per transaction type             | Deposit, withdrawal, transfer, POS, fee.                           |
| dim_date                       | Dimension              | One row per date                         | Calendar attributes for analytics.                                 |

# 9\. Required Pipelines

| **Pipeline**                     | **Schedule/Trigger**   | **Expected Output**                                               |
| -------------------------------- | ---------------------- | ----------------------------------------------------------------- |
| Reference ingestion              | One-time or daily      | Branch, transaction type, and merchant category reference tables. |
| Customer/account ingestion       | Daily                  | Bronze and Silver customer/account tables.                        |
| Historical transaction ingestion | Daily/backfill         | Bronze and Silver transaction tables.                             |
| Balance snapshot ingestion       | Daily                  | Daily account balance Silver table.                               |
| Settlement ingestion             | Daily                  | Settlement Silver table.                                          |
| Kafka transaction streaming      | Continuous or replayed | Bronze/Silver transaction event tables.                           |
| Gold dbt build                   | Daily/hourly           | Facts, dimensions, aggregates, tests, docs.                       |
| Reconciliation                   | Daily                  | Settlement variance fact and alert table.                         |
| Data-quality checks              | Every run              | Quality result table and pass/fail gate.                          |
| Dashboard refresh                | Daily/hourly           | Updated business and operational dashboards.                      |

# 10\. Minimum Viable Scope

- At least three batch sources: customers, accounts, transactions.
- At least one settlement or balance source for reconciliation or snapshot modelling.
- At least two Kafka topics: raw transactions and DLQ.
- At least three Airflow DAGs: ingestion, processing, and dbt/quality/dashboard.
- Landing, Bronze, Silver, and Gold layers.
- At least two fact tables and five dimensions.
- At least ten meaningful data-quality checks.
- At least two dashboard pages: business metrics and pipeline/quality health.
- Docker Compose setup with documented commands.
- README, architecture diagram, data dictionary, runbook, and final demo script.

# 11\. Required Data-Quality Checks

| **Category**         | **Example Checks**                                                                          | **Suggested Tool**                           |
| -------------------- | ------------------------------------------------------------------------------------------- | -------------------------------------------- |
| Source/Ingestion     | File exists, file schema valid, duplicate file hash, malformed Kafka events.                | Airflow, Python, Spark.                      |
| Transformation       | Unique transaction_id, non-null keys, valid statuses, account FK exists.                    | dbt tests, Spark, SQL.                       |
| Business             | Settlement variance, balance movement validation, reversed transaction references original. | SQL reconciliation, dbt.                     |
| Freshness/Operations | Kafka lag, stale Silver/Gold table, failed partition, DLQ count.                            | Airflow, monitoring dashboard, audit tables. |

# 12\. Expected Dashboards

| **Dashboard**                  | **Audience**    | **KPIs**                                                          |
| ------------------------------ | --------------- | ----------------------------------------------------------------- |
| Transaction Operations         | Operations team | Transaction count, amount, failure rate, channel mix.             |
| Branch and Channel Performance | Management      | Volume/value by branch, region, and channel.                      |
| Settlement Reconciliation      | Finance         | Stream/batch totals, settlement variance, unmatched records.      |
| Fraud/Risk Monitoring optional | Risk team       | Suspicious transaction count, high-risk MCC, high-value activity. |
| Pipeline Health                | Data team       | Failed DAGs, stale tables, DLQ count, quality failure count.      |

# 13\. Open-Ended Architecture Challenges

- What is the correct partitioning strategy for transactions and events?
- How should late streaming events update daily Gold tables?
- How should duplicates be identified across batch and stream?
- Should customer and account changes use SCD Type 2 or current-state dimensions?
- How should settlement mismatches be detected, stored, and displayed?
- How should the system recover after Kafka, Spark, or Airflow failure?

# 14\. Final Submission Checklist

- GitHub repository with meaningful commits from all team members.
- docker-compose.yml and setup instructions.
- Synthetic dataset generator or documented generated data sample.
- Airflow DAGs for ingestion, processing, quality, and dbt.
- Spark jobs for batch and streaming processing.
- dbt project with models, tests, docs, and marts.
- Gold fact and dimension tables.
- Dashboard screenshots or working dashboard.
- Architecture diagram and data dictionary.
- Runbook explaining failure recovery and rerun process.
- Final presentation explaining business value and technical decisions.