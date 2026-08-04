from __future__ import annotations

import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

REFERENCE_STAGE_PREFIX = "reference"


@dag(
    dag_id="reference_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "reference"],
)
def reference_ingestion():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def discover_reference_files() -> list[str]:
        reference_directory = Path("/opt/airflow/data/reference")
        return [
            str(file_path) for file_path in sorted(reference_directory.glob("*.csv"))
        ]

    @task
    def load_reference_data(reference_files: list[str]):
        from reference_minio_loader import load_reference_csvs_to_minio

        for file_path in reference_files:
            load_reference_csvs_to_minio(Path(file_path), prefix=REFERENCE_STAGE_PREFIX)
        return reference_files

    reference_files = discover_reference_files()
    loaded_files = load_reference_data(reference_files)

    start >> reference_files >> loaded_files >> end


reference_ingestion_dag = reference_ingestion()
