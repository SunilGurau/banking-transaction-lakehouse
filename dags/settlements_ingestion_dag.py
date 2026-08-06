from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dag(
    dag_id="settlements_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=True,
    tags=["batch", "ingestion", "settlements"],
    max_active_runs=1,
)
def settlements_ingestion():
    @task
    def load_settlements(**kwargs):
        from reference_minio_loader import load_reference_csvs_to_minio

        logical_date = kwargs["logical_date"].date()
        print(f"Loading settlements for logical date: {logical_date}")

        folder = Path("/opt/airflow/data/batch/settlements")
        file_path = folder / f"settlement_{logical_date}.csv"
        print(f"Loading settlements from file: {file_path}")

        return load_reference_csvs_to_minio(file_path, prefix="settlements")

    loaded_files = load_settlements()


settlements_ingestion_dag = settlements_ingestion()
