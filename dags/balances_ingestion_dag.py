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
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "balances"],
)
def balances_ingestion():
    @task
    def load_balances():
        from reference_minio_loader import load_reference_csvs_to_minio

        folder = Path("/opt/airflow/data/batch/balances")
        results = []
        for csv_file in sorted(folder.glob("*.csv")):
            results.append(load_reference_csvs_to_minio(csv_file, prefix="balances"))
        return results

    load_balances()


balances_ingestion_dag = balances_ingestion()
