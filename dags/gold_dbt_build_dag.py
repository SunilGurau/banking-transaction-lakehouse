from __future__ import annotations

import os
import sys
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")


@dag(
    dag_id="gold_dbt_build",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["batch", "dbt", "gold"],
)
def gold_dbt_build():
    BashOperator(
        task_id="run_gold_models_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select path:models/gold"
        ),
    )

gold_dbt_build_dag = gold_dbt_build()
