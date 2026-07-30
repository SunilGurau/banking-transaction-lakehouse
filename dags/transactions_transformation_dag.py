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

from utils import latest_delta_uri

DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")


@dag(
    dag_id="transactions_transformation",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "transformation", "transactions", "dbt"],
)
def transactions_transformation():
    @task
    def resolve_latest_tables() -> dict[str, str]:
        return {"transactions": latest_delta_uri("", "transactions")}

    dbt_run_stage = BashOperator(
        task_id="run_transactions_dbt",
        bash_command=(
            'echo \'{{ {"batch_table_uris": ti.xcom_pull(task_ids="resolve_latest_tables")} | tojson }}\' && '
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__transactions silver_transaction "
            f'--vars \'{{{{ {{"batch_table_uris": ti.xcom_pull(task_ids="resolve_latest_tables")}} | tojson }}}}\''
        ),
    )

    resolved_tables = resolve_latest_tables()
    resolved_tables >> dbt_run_stage

transactions_transformation_dag = transactions_transformation()
