from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dag(
    dag_id="balances_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=True,
    tags=["batch", "ingestion", "balances"],
    max_active_runs=1,
)
def balances_ingestion():
    @task
    def load_balances(**kwargs):
        from reference_minio_loader import load_reference_csvs_to_minio

        logical_date = kwargs["logical_date"].date()
        print(f"Loading balances for logical date: {logical_date}")

        folder = Path("/opt/airflow/data/batch/balances")
        file_path = folder / f"account_balances_{logical_date}.csv"
        print(f"Loading balances from file: {file_path}")

        return load_reference_csvs_to_minio(file_path, prefix="balances")

    loaded_files = load_balances()


balances_ingestion_dag = balances_ingestion()
