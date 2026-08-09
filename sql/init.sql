CREATE SCHEMA IF NOT EXISTS audit;

DROP TABLE IF EXISTS audit.fraud_alerts;

-- Enable UUID extension if using UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE audit.fraud_alerts (
    alert_id VARCHAR(255) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    transaction_id VARCHAR(255),
    account_id VARCHAR(255),
    customer_id VARCHAR(255),
    channel VARCHAR(50),
    merchant_category_code VARCHAR(50),
    amount NUMERIC(12, 2),
    status VARCHAR(50) DEFAULT 'OPEN',
    event_ts TIMESTAMP,
    fraud_type VARCHAR(100),
    risk_score NUMERIC(5, 2),
    rule_triggered VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for storing Data Quality check results
CREATE TABLE IF NOT EXISTS audit.dq_check_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255),
    dag_id VARCHAR(255),
    layer VARCHAR(50),
    table_name VARCHAR(255),
    check_name VARCHAR(255),
    check_type VARCHAR(100),
    status VARCHAR(20),
    row_count BIGINT DEFAULT 0,
    failed_count BIGINT DEFAULT 0,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);