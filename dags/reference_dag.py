from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

LAKEHOUSE_BUCKET = "landing"
REFERENCE_STAGE_PREFIX = "reference"
DBT_EXECUTABLE = Path(
    os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
)
DBT_PROJECT_DIR = Path(
    os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics")
)
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")


def _latest_reference_delta_uri(table_name: str) -> str:
    hook = S3Hook(aws_conn_id="minio")
    prefix = f"{REFERENCE_STAGE_PREFIX}/{table_name}/"
    keys = hook.list_keys(bucket_name=LAKEHOUSE_BUCKET, prefix=prefix) or []

    timestamps = {
        key[len(prefix) :].split("/", 1)[0]
        for key in keys
        if key.startswith(prefix) and key[len(prefix) :]
    }
    if not timestamps:
        raise FileNotFoundError(
            f"No reference Delta table found for {table_name} under {prefix}"
        )

    latest_timestamp = max(timestamps)
    return f"s3a://{LAKEHOUSE_BUCKET}/{prefix}{latest_timestamp}"


@dag(
    dag_id="reference_to_minio_delta",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def reference_to_minio_delta():
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

    @task
    def resolve_latest_reference_tables(reference_files: list[str]) -> dict[str, str]:
        reference_tables: dict[str, str] = {}
        for file_path in reference_files:
            table_name = Path(file_path).stem
            reference_tables[table_name] = _latest_reference_delta_uri(table_name)
        return reference_tables

    dbt_run_stage = BashOperator(
        task_id="run_reference_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--vars '{{\"reference_table_uris\": {json.dumps(resolve_latest_reference_tables.output)}}}'"
        ),
    )

    reference_files = discover_reference_files()
    loaded_files = load_reference_data(reference_files)
    reference_tables = resolve_latest_reference_tables(loaded_files)

    (reference_files >> loaded_files >> reference_tables >> dbt_run_stage)
