from __future__ import annotations

import os
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


DBT_EXECUTABLE = Path(
    os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
)
DBT_PROJECT_DIR = Path(
    os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics")
)
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")


@dag(
    dag_id="reference_transformation",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "transformation", "reference", "dbt"],
)
def reference_transformation():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def resolve_latest_reference_tables() -> dict[str, str]:
        return {
            "branches": "s3a://landing-zone/reference/branches.csv",
            "merchant_categories": "s3a://landing-zone/reference/merchant_categories.csv",
            "transaction_types": "s3a://landing-zone/reference/transaction_types.csv",
        }

    dbt_run_stage = BashOperator(
        task_id="run_reference_dbt",
        bash_command=(
            'echo \'{{ {"reference_table_uris": ti.xcom_pull(task_ids="resolve_latest_reference_tables")} | tojson }}\' && '
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            # f"--select stg_reference__branches+ stg_reference__merchant_categories+ stg_reference__transaction_types+ "
            # f"--select stg_reference_transaction_types+ "
            f"--select stg_reference__transaction_types+ "
            f'--vars \'{{{{ {{"reference_table_uris": ti.xcom_pull(task_ids="resolve_latest_reference_tables")}} | tojson }}}}\''
        ),
    )

    reference_tables = resolve_latest_reference_tables()

    start >> reference_tables >> dbt_run_stage >> end


reference_transformation_dag = reference_transformation()
