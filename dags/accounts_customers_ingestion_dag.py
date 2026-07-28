from __future__ import annotations

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
DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")

def _latest_delta_uri(prefix_str: str, table_name: str) -> str:
    hook = S3Hook(aws_conn_id="minio")
    prefix = f"{prefix_str}/{table_name}/"
    keys = hook.list_keys(bucket_name=LAKEHOUSE_BUCKET, prefix=prefix) or []

    timestamps = {
        key[len(prefix) :].split("/", 1)[0]
        for key in keys
        if key.startswith(prefix) and key[len(prefix) :]
    }
    if not timestamps:
        raise FileNotFoundError(f"No reference Delta table found for {table_name} under {prefix}")

    latest_timestamp = max(timestamps)
    return f"s3a://{LAKEHOUSE_BUCKET}/{prefix}{latest_timestamp}"

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

    @task
    def resolve_latest_tables(dummy) -> dict[str, str]:
        return {
            "accounts": _latest_delta_uri("accounts", "accounts"),
            "customers": _latest_delta_uri("customers", "customers"),
        }

    dbt_run_stage = BashOperator(
        task_id="run_accounts_customers_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__customers stg_batch__accounts dim_customer dim_account "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}'"
        ),
    )

    loaded_files = load_accounts_and_customers()
    resolved_tables = resolve_latest_tables(loaded_files)
    resolved_tables >> dbt_run_stage

accounts_customers_ingestion_dag = accounts_customers_ingestion()
