from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dag(
    dag_id="accounts_customers_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "accounts", "customers"],
)
def accounts_customers_ingestion():
    @task
    def load_accounts_and_customers():
        from reference_minio_loader import load_reference_csvs_to_minio

        source_folders = [
            (Path("/opt/airflow/data/batch/accounts"), "accounts"),
            (Path("/opt/airflow/data/batch/customers"), "customers"),
        ]

        results = []
        for folder, prefix in source_folders:
            for csv_file in sorted(folder.glob("*.csv")):
                results.append(load_reference_csvs_to_minio(csv_file, prefix=prefix))
        return results

    load_accounts_and_customers()


accounts_customers_ingestion_dag = accounts_customers_ingestion()
