from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gold_postgres_loader import load_gold_table_to_postgres


@dag(
    dag_id="load_gold_fact_transaction",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold", "postgres", "load", "fact_transaction"],
)
def load_gold_fact_transaction():
    """Load fact_transaction Delta Lake table from MinIO into ETL Postgres."""

    @task
    def run_load():
        load_gold_table_to_postgres("fact_transaction")

    run_load()


load_gold_fact_transaction_dag = load_gold_fact_transaction()
