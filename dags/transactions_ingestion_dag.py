from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dag(
    dag_id="transactions_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "transactions"],
)
def transactions_ingestion():
    @task
    def load_transactions():
        from reference_minio_loader import load_reference_csvs_to_minio

        folder = Path("/opt/airflow/data/batch/transactions")
        results = []
        for csv_file in sorted(folder.glob("*.csv")):
            results.append(
                load_reference_csvs_to_minio(csv_file, prefix="transactions")
            )
        return results

    loaded_files = load_transactions()


transactions_ingestion_dag = transactions_ingestion()
