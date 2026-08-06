from __future__ import annotations

import os
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils import latest_incremental_file_uri

DBT_EXECUTABLE = Path(
    os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
)
DBT_PROJECT_DIR = Path(
    os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics")
)
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")
DBT_VARS_TEMPLATE = '{{ {"batch_table_uris": {"settlements": ti.xcom_pull(task_ids="resolve_settlement_file")}} | tojson }}'


@dag(
    dag_id="settlements_transformation",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True,
    tags=["batch", "transformation", "settlements", "dbt"],
    max_active_runs=1,
)
def settlements_transformation():
    @task
    def resolve_settlement_file(**kwargs) -> str:
        logical_date = kwargs["logical_date"].date()
        filename = f"settlement_{logical_date}.csv"
        return latest_incremental_file_uri("settlements", filename)

    dbt_run_stage = BashOperator(
        task_id="run_settlements_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__settlements silver_settlement "
            f"--vars '{DBT_VARS_TEMPLATE}'"
        ),
    )

    settlement_file = resolve_settlement_file()
    settlement_file >> dbt_run_stage


settlements_transformation_dag = settlements_transformation()
