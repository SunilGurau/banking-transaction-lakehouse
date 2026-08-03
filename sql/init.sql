-- =====================================================================
-- Banking Transaction Lakehouse - Postgres init.sql
-- Serving layer + operational/DQ audit tables
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------------
-- Central Data Quality result table. Every layer (Python/Spark bronze
-- checks, dbt silver/gold tests, Airflow freshness checks) writes here.
-- This table is the source for the "Pipeline Health" dashboard.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.dq_check_results (
    check_id        BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(200) NOT NULL,
    dag_id          VARCHAR(100) NOT NULL,
    layer           VARCHAR(20)  NOT NULL,   -- landing | bronze | silver | gold | streaming
    table_name      VARCHAR(100) NOT NULL,
    check_name      VARCHAR(150) NOT NULL,
    check_type      VARCHAR(50)  NOT NULL,   -- schema | null | unique | accepted_values | fk | freshness | reconciliation
    status          VARCHAR(10)  NOT NULL,   -- PASS | FAIL
    row_count       BIGINT DEFAULT 0,
    failed_count    BIGINT DEFAULT 0,
    details         TEXT,
    checked_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_status_time ON audit.dq_check_results (status, checked_at);
CREATE INDEX IF NOT EXISTS idx_dq_table ON audit.dq_check_results (table_name);

-- ---------------------------------------------------------------------
-- Ingestion audit log — one row per file/partition ingested.
-- Feeds "which files landed / were skipped / hashed as duplicate".
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.ingestion_log (
    ingestion_id    BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(200) NOT NULL,
    dag_id          VARCHAR(100) NOT NULL,
    source_name     VARCHAR(100) NOT NULL,   -- accounts | customers | transactions | balances | settlements | streaming
    file_name       VARCHAR(300) NOT NULL,
    file_sha256     VARCHAR(64),
    row_count       BIGINT,
    is_duplicate    BOOLEAN DEFAULT FALSE,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------
-- Settlement reconciliation results (fed by dbt gold model or a Spark job
-- comparing fact_transaction totals against settlement files).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.settlement_variance (
    variance_id             BIGSERIAL PRIMARY KEY,
    settlement_date         DATE NOT NULL,
    channel                 VARCHAR(50) NOT NULL,
    transaction_type_code   VARCHAR(10) NOT NULL,
    settled_transaction_count BIGINT,
    computed_transaction_count BIGINT,
    settled_gross_amount    NUMERIC(18,2),
    computed_gross_amount   NUMERIC(18,2),
    variance_amount         NUMERIC(18,2),
    status                  VARCHAR(20),      -- MATCHED | MINOR_VARIANCE | FAILED
    checked_at              TIMESTAMP DEFAULT NOW()
);

-- ---------------------------------------------------------------------
-- Fraud/Risk alerts - populated by a Kafka consumer reading
-- banking.fraud.alerts (see silver cleaning script's fraud_stream).
-- Feeds the "Fraud/Risk Monitoring" dashboard page.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.fraud_alerts (
    alert_id            BIGSERIAL PRIMARY KEY,
    transaction_id       VARCHAR(100) NOT NULL,
    account_id           VARCHAR(100),
    customer_id          VARCHAR(100),
    channel              VARCHAR(50),
    merchant_category_code VARCHAR(10),
    amount               NUMERIC(18,2),
    status               VARCHAR(20),
    event_ts             TIMESTAMP,
    received_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_time ON audit.fraud_alerts (received_at);

CREATE TABLE IF NOT EXISTS audit.dlq_summary (
    summary_id      BIGSERIAL PRIMARY KEY,
    event_date      DATE NOT NULL,
    topic           VARCHAR(100) NOT NULL,
    dlq_count       BIGINT DEFAULT 0,
    total_count     BIGINT DEFAULT 0,
    recorded_at     TIMESTAMP DEFAULT NOW()
);