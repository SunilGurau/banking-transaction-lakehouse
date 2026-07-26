from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dag(
    dag_id="reference_to_minio_delta",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def reference_to_minio_delta():
    @task
    def load_reference_data():
        from reference_minio_loader import load_reference_csvs_to_minio

        # Iterate and load the reference CSV files to MinIO
        reference_files = Path("/opt/airflow/data/reference").glob("*.csv")
        for file in reference_files:
            load_reference_csvs_to_minio(file)

    load_reference_data()


reference_to_minio_delta_dag = reference_to_minio_delta()
