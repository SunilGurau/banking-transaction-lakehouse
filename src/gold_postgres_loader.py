"""
src/gold_postgres_loader.py

Helper module to load Gold Delta Lake tables stored in MinIO into etl-postgres database.
Creates target tables with Primary Keys, Unique Constraints, and Foreign Key References,
and performs batch upserts using psycopg2.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Connection helper
def get_postgres_connection():
    # 1. Try Airflow BaseHook connection if running inside Airflow
    try:
        from airflow.hooks.base import BaseHook
        conn_obj = BaseHook.get_connection("etl_postgres")
        print("conn_object", conn_obj)
        host = conn_obj.host or "etl-postgres"
        print("password", conn_obj.password)
        if host in ("localhost", "127.0.0.1") and os.path.exists("/.dockerenv"):
            host = "etl-postgres"
        print("host", host)
        print("schema", conn_obj.schema)
        print("port", conn_obj.port)
        print("password", conn_obj.password )
        print("db name", conn_obj.schema)
        print("username", conn_obj.login)
        print("port", conn_obj.port)
        logger.info(f"Connecting to Postgres via Airflow Connection etl_postgres at {host}:{conn_obj.port or 5432}/{conn_obj.schema}")
        return psycopg2.connect(
            host=host,
            port=conn_obj.port or 5432,
            dbname=conn_obj.schema or os.environ.get("POSTGRES_DB", "etl_db"),
            user=conn_obj.login or os.environ.get("POSTGRES_USER", "etl_user"),
            password=conn_obj.password or os.environ.get("POSTGRES_PASSWORD", "etl_password"),
        )
    except Exception as exc:
        logger.debug(f"BaseHook etl_postgres resolution failed ({exc}); falling back to env vars.")

    # 2. Fall back to environment variables
    pg_host = os.environ.get("POSTGRES_HOST", "etl-postgres")
    if pg_host in ("localhost", "127.0.0.1") and os.path.exists("/.dockerenv"):
        pg_host = "etl-postgres"

    pg_port = int(os.environ.get("POSTGRES_PORT", 5432))
    pg_db = os.environ.get("POSTGRES_DB", "etl_db")
    pg_user = os.environ.get("POSTGRES_USER", "etl_user")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "etl_password")

    logger.info(f"Connecting to Postgres at {pg_host}:{pg_port}/{pg_db} as {pg_user}")
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
    )

# MinIO storage options for deltalake
def get_minio_storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "AWS_REGION": os.environ.get("MINIO_REGION", "us-east-1"),
    }

# Table Schema Definitions with DDL, Primary Keys, Unique Constraints, and FK References
TABLE_CONFIGS: dict[str, dict[str, Any]] = {
    "dim_customer": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_customer (
                customer_key BIGINT PRIMARY KEY,
                customer_id VARCHAR(255) UNIQUE NOT NULL,
                customer_name VARCHAR(255),
                segment VARCHAR(100),
                age_band VARCHAR(50),
                province VARCHAR(100),
                risk_band VARCHAR(50),
                is_active BOOLEAN,
                created_date DATE
            );
        """,
        "columns": ["customer_key", "customer_id", "customer_name", "segment", "age_band", "province", "risk_band", "is_active", "created_date"],
        "conflict_cols": ["customer_id"],
        "bool_cols": ["is_active"],
    },
    "dim_branch": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_branch (
                branch_key BIGINT PRIMARY KEY,
                branch_id VARCHAR(255) UNIQUE NOT NULL,
                branch_name VARCHAR(255),
                province VARCHAR(100),
                region VARCHAR(100),
                opened_date DATE,
                is_active BOOLEAN
            );
        """,
        "columns": ["branch_key", "branch_id", "branch_name", "province", "region", "opened_date", "is_active"],
        "conflict_cols": ["branch_id"],
        "bool_cols": ["is_active"],
    },
    "dim_channel": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_channel (
                channel_key BIGINT PRIMARY KEY,
                channel_code VARCHAR(50) UNIQUE NOT NULL,
                channel_name VARCHAR(100)
            );
        """,
        "columns": ["channel_key", "channel_code", "channel_name"],
        "conflict_cols": ["channel_code"],
        "bool_cols": [],
    },
    "dim_transaction_type": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_transaction_type (
                transaction_type_key BIGINT PRIMARY KEY,
                transaction_type_code VARCHAR(50) UNIQUE NOT NULL,
                transaction_type_name VARCHAR(100),
                balance_direction VARCHAR(20)
            );
        """,
        "columns": ["transaction_type_key", "transaction_type_code", "transaction_type_name", "balance_direction"],
        "conflict_cols": ["transaction_type_code"],
        "bool_cols": [],
    },
    "dim_merchant_category": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_merchant_category (
                merchant_category_key BIGINT PRIMARY KEY,
                merchant_category_code VARCHAR(50) UNIQUE NOT NULL,
                merchant_category_name VARCHAR(255),
                risk_category VARCHAR(50)
            );
        """,
        "columns": ["merchant_category_key", "merchant_category_code", "merchant_category_name", "risk_category"],
        "conflict_cols": ["merchant_category_code"],
        "bool_cols": [],
    },
    "dim_date": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE UNIQUE NOT NULL,
                day_of_week INT,
                day_name VARCHAR(20),
                month_num INT,
                month_name VARCHAR(20),
                quarter_num INT,
                year_num INT,
                is_weekend BOOLEAN,
                fiscal_year INT,
                fiscal_quarter INT
            );
        """,
        "columns": ["date_key", "full_date", "day_of_week", "day_name", "month_num", "month_name", "quarter_num", "year_num", "is_weekend", "fiscal_year", "fiscal_quarter"],
        "conflict_cols": ["date_key"],
        "bool_cols": ["is_weekend"],
    },
    "dim_account": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.dim_account (
                account_key BIGINT PRIMARY KEY,
                account_id VARCHAR(255) UNIQUE NOT NULL,
                customer_id VARCHAR(255) NOT NULL REFERENCES public.dim_customer(customer_id) ON DELETE CASCADE,
                branch_id VARCHAR(255) REFERENCES public.dim_branch(branch_id) ON DELETE SET NULL,
                account_type VARCHAR(50),
                account_status VARCHAR(50),
                opened_date DATE,
                currency VARCHAR(10)
            );
        """,
        "columns": ["account_key", "account_id", "customer_id", "branch_id", "account_type", "account_status", "opened_date", "currency"],
        "conflict_cols": ["account_id"],
        "bool_cols": [],
    },
    "fact_transaction": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.fact_transaction (
                transaction_id VARCHAR(255) PRIMARY KEY,
                account_id VARCHAR(255) NOT NULL REFERENCES public.dim_account(account_id) ON DELETE CASCADE,
                customer_id VARCHAR(255) NOT NULL REFERENCES public.dim_customer(customer_id) ON DELETE CASCADE,
                branch_id VARCHAR(255) REFERENCES public.dim_branch(branch_id) ON DELETE SET NULL,
                channel VARCHAR(50) REFERENCES public.dim_channel(channel_code) ON DELETE SET NULL,
                transaction_type_code VARCHAR(50) REFERENCES public.dim_transaction_type(transaction_type_code) ON DELETE SET NULL,
                merchant_category_code VARCHAR(50) REFERENCES public.dim_merchant_category(merchant_category_code) ON DELETE SET NULL,
                transaction_date DATE REFERENCES public.dim_date(full_date) ON DELETE SET NULL,
                transaction_ts TIMESTAMP,
                amount NUMERIC(15,2),
                fee_amount NUMERIC(15,2),
                net_amount NUMERIC(15,2),
                status VARCHAR(50),
                currency VARCHAR(10),
                original_transaction_id VARCHAR(255),
                source_system VARCHAR(50)
            );
        """,
        "columns": ["transaction_id", "account_id", "customer_id", "branch_id", "channel", "transaction_type_code", "merchant_category_code", "transaction_date", "transaction_ts", "amount", "fee_amount", "net_amount", "status", "currency", "original_transaction_id", "source_system"],
        "conflict_cols": ["transaction_id"],
        "bool_cols": [],
    },
    "fact_daily_account_balance": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.fact_daily_account_balance (
                account_id VARCHAR(255) NOT NULL REFERENCES public.dim_account(account_id) ON DELETE CASCADE,
                customer_id VARCHAR(255) REFERENCES public.dim_customer(customer_id) ON DELETE CASCADE,
                branch_id VARCHAR(255) REFERENCES public.dim_branch(branch_id) ON DELETE SET NULL,
                balance_date DATE NOT NULL REFERENCES public.dim_date(full_date) ON DELETE CASCADE,
                opening_balance NUMERIC(15,2),
                credit_total NUMERIC(15,2),
                debit_total NUMERIC(15,2),
                closing_balance NUMERIC(15,2),
                net_change NUMERIC(15,2),
                is_balance_valid BOOLEAN,
                currency VARCHAR(10),
                PRIMARY KEY (account_id, balance_date)
            );
        """,
        "columns": ["account_id", "customer_id", "branch_id", "balance_date", "opening_balance", "credit_total", "debit_total", "closing_balance", "net_change", "is_balance_valid", "currency"],
        "conflict_cols": ["account_id", "balance_date"],
        "bool_cols": ["is_balance_valid"],
    },
    "fact_settlement_reconciliation": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.fact_settlement_reconciliation (
                settlement_batch_id VARCHAR(255) PRIMARY KEY,
                settlement_date DATE REFERENCES public.dim_date(full_date) ON DELETE SET NULL,
                channel VARCHAR(50) REFERENCES public.dim_channel(channel_code) ON DELETE SET NULL,
                transaction_type_code VARCHAR(50) REFERENCES public.dim_transaction_type(transaction_type_code) ON DELETE SET NULL,
                settled_transaction_count BIGINT,
                settled_gross_amount NUMERIC(15,2),
                settled_fee_amount NUMERIC(15,2),
                actual_transaction_count BIGINT,
                actual_gross_amount NUMERIC(15,2),
                actual_fee_amount NUMERIC(15,2),
                count_variance BIGINT,
                amount_variance NUMERIC(15,2),
                variance_pct NUMERIC(10,4),
                reconciliation_status VARCHAR(50),
                currency VARCHAR(10)
            );
        """,
        "columns": ["settlement_batch_id", "settlement_date", "channel", "transaction_type_code", "settled_transaction_count", "settled_gross_amount", "settled_fee_amount", "actual_transaction_count", "actual_gross_amount", "actual_fee_amount", "count_variance", "amount_variance", "variance_pct", "reconciliation_status", "currency"],
        "conflict_cols": ["settlement_batch_id"],
    },
    "fact_fraud_alert": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.fact_fraud_alert (
                alert_key BIGINT PRIMARY KEY,
                transaction_id VARCHAR(255) REFERENCES public.fact_transaction(transaction_id) ON DELETE CASCADE,
                account_id VARCHAR(255) REFERENCES public.dim_account(account_id) ON DELETE CASCADE,
                customer_id VARCHAR(255) REFERENCES public.dim_customer(customer_id) ON DELETE CASCADE,
                branch_id VARCHAR(255) REFERENCES public.dim_branch(branch_id) ON DELETE SET NULL,
                channel VARCHAR(50) REFERENCES public.dim_channel(channel_code) ON DELETE SET NULL,
                transaction_type_code VARCHAR(50) REFERENCES public.dim_transaction_type(transaction_type_code) ON DELETE SET NULL,
                merchant_category_code VARCHAR(50) REFERENCES public.dim_merchant_category(merchant_category_code) ON DELETE SET NULL,
                transaction_date DATE REFERENCES public.dim_date(full_date) ON DELETE SET NULL,
                transaction_ts TIMESTAMP,
                amount NUMERIC(15,2),
                alert_type VARCHAR(50),
                alert_description TEXT,
                alert_severity VARCHAR(20)
            );
        """,
        "columns": ["alert_key", "transaction_id", "account_id", "customer_id", "branch_id", "channel", "transaction_type_code", "merchant_category_code", "transaction_date", "transaction_ts", "amount", "alert_type", "alert_description", "alert_severity"],
        "conflict_cols": ["alert_key"],
    },
}

# Order of table creation for DDL setup
TABLE_CREATION_ORDER = [
    "dim_customer",
    "dim_branch",
    "dim_channel",
    "dim_transaction_type",
    "dim_merchant_category",
    "dim_date",
    "dim_account",
    "fact_transaction",
    "fact_daily_account_balance",
    "fact_settlement_reconciliation",
    "fact_fraud_alert",
]

def ensure_tables_exist():
    """Creates all Postgres Gold tables if they do not already exist."""
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            for table_name in TABLE_CREATION_ORDER:
                config = TABLE_CONFIGS[table_name]
                logger.info(f"Ensuring table 'public.{table_name}' exists...")
                cur.execute(config["ddl"])
        conn.commit()
        logger.info("All target Postgres Gold tables created/verified successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error ensuring Postgres tables exist: {e}")
        raise
    finally:
        conn.close()

def _sanitize_row_value(val, is_bool_col: bool = False):
    if pd.isna(val) or val is None or val is np.nan:
        return None
    if is_bool_col:
        if isinstance(val, (bool, np.bool_)):
            return bool(val)
        if isinstance(val, (int, np.integer, float, np.floating)):
            return bool(val != 0)
        if isinstance(val, str):
            return val.strip().lower() in ("true", "1", "t", "yes")
        return bool(val)
    if isinstance(val, (bool, np.bool_)):
        return bool(val)
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        return float(val)
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        return str(val)
    return str(val)

def load_gold_table_to_postgres(table_name: str) -> int:
    """
    Reads a Gold Delta Lake table from MinIO (s3://gold/<table_name>)
    and upserts its contents into PostgreSQL (public.<table_name>).
    """
    if table_name not in TABLE_CONFIGS:
        raise ValueError(f"Unknown Gold table name: {table_name}")

    ensure_tables_exist()

    config = TABLE_CONFIGS[table_name]
    columns = config["columns"]
    conflict_cols = config["conflict_cols"]
    bool_cols = set(config.get("bool_cols", []))

    table_uri = f"s3://gold/{table_name}"
    storage_options = get_minio_storage_options()

    logger.info(f"Reading Delta Lake table from {table_uri}...")
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
    except Exception as exc:
        logger.warning(f"Could not read Delta table {table_uri}: {exc}")
        logger.info(f"Skipping load for {table_name} as source table was not found or is empty.")
        return 0

    if df.empty:
        logger.info(f"Delta table {table_name} is empty. 0 rows loaded.")
        return 0

    # Ensure all required columns exist in df
    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]

    # Convert DataFrame to list of tuples for psycopg2
    data_tuples = [
        tuple(
            _sanitize_row_value(val, is_bool_col=(col_name in bool_cols))
            for col_name, val in zip(columns, row)
        )
        for row in df.itertuples(index=False)
    ]

    col_names_str = ", ".join(columns)
    conflict_str = ", ".join(conflict_cols)

    non_conflict_cols = [c for c in columns if c not in conflict_cols]
    if non_conflict_cols:
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_conflict_cols])
        on_conflict_clause = f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
    else:
        on_conflict_clause = f"ON CONFLICT ({conflict_str}) DO NOTHING"

    insert_query = f"""
        INSERT INTO public.{table_name} ({col_names_str})
        VALUES %s
        {on_conflict_clause}
    """

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            logger.info(f"Upserting {len(data_tuples):,} rows into public.{table_name}...")
            execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()
        logger.info(f"Successfully loaded {len(data_tuples):,} rows into public.{table_name}.")
        return len(data_tuples)
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert data into public.{table_name}: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    table_to_load = sys.argv[1] if len(sys.argv) > 1 else "all"
    if table_to_load == "all":
        ensure_tables_exist()
        for tbl in TABLE_CREATION_ORDER:
            load_gold_table_to_postgres(tbl)
    else:
        load_gold_table_to_postgres(table_to_load)
