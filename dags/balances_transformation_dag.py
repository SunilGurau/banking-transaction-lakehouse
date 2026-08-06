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
DBT_VARS_TEMPLATE = '{{ {"batch_table_uris": {"balances": ti.xcom_pull(task_ids="resolve_balance_file")}} | tojson }}'


@dag(
    dag_id="balances_transformation",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True,
    tags=["batch", "transformation", "balances", "dbt"],
    max_active_runs=1,
)
def balances_transformation():
    @task
    def resolve_balance_file(**kwargs) -> str:
        logical_date = kwargs["logical_date"].date()
        filename = f"account_balances_{logical_date}.csv"
        return latest_incremental_file_uri("balances", filename)

    dbt_run_stage = BashOperator(
        task_id="run_balances_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__balances silver_daily_account_balance "
            f"--vars '{DBT_VARS_TEMPLATE}'"
        ),
    )

    balance_file = resolve_balance_file()
    balance_file >> dbt_run_stage


balances_transformation_dag = balances_transformation()
