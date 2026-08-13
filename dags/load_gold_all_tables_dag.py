from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gold_postgres_loader import load_gold_table_to_postgres, ensure_tables_exist


@dag(
    dag_id="load_gold_all_tables",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold", "postgres", "load", "pipeline"],
)
def load_gold_all_tables():
    """
    Master DAG to load all 11 Gold Delta Lake tables from MinIO into ETL Postgres
    in strict dependency order to satisfy foreign key constraints.
    """

    @task
    def create_schema():
        ensure_tables_exist()

    @task
    def load_customer():
        load_gold_table_to_postgres("dim_customer")

    @task
    def load_branch():
        load_gold_table_to_postgres("dim_branch")

    @task
    def load_channel():
        load_gold_table_to_postgres("dim_channel")

    @task
    def load_transaction_type():
        load_gold_table_to_postgres("dim_transaction_type")

    @task
    def load_merchant_category():
        load_gold_table_to_postgres("dim_merchant_category")

    @task
    def load_date():
        load_gold_table_to_postgres("dim_date")

    @task
    def load_account():
        load_gold_table_to_postgres("dim_account")

    @task
    def load_fact_txn():
        load_gold_table_to_postgres("fact_transaction")

    @task
    def load_fact_balance():
        load_gold_table_to_postgres("fact_daily_account_balance")

    @task
    def load_fact_settlement():
        load_gold_table_to_postgres("fact_settlement_reconciliation")

    @task
    def load_fact_fraud():
        load_gold_table_to_postgres("fact_fraud_alert")

    init = create_schema()
    cust = load_customer()
    branch = load_branch()
    channel = load_channel()
    txn_type = load_transaction_type()
    mcc = load_merchant_category()
    dt = load_date()

    init >> [cust, branch, channel, txn_type, mcc, dt]

    acc = load_account()
    [cust, branch] >> acc

    fact_txn = load_fact_txn()
    [acc, cust, branch, channel, txn_type, mcc, dt] >> fact_txn

    fact_bal = load_fact_balance()
    [acc, cust, branch, dt] >> fact_bal

    fact_set = load_fact_settlement()
    [dt, channel, txn_type] >> fact_set

    fact_fraud = load_fact_fraud()
    [fact_txn, acc, cust, branch, channel, txn_type, mcc, dt] >> fact_fraud


load_gold_all_tables_dag = load_gold_all_tables()
