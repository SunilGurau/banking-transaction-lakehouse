from __future__ import annotations

import os
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.trigger_rule import TriggerRule

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/banking_analytics"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"))
DBT_TARGET = os.environ.get("DBT_TARGET", "spark")


@dag(
    dag_id="dashboard_sync",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["batch", "dbt", "quality", "dashboard"],
)
def dashboard_sync():
    # dbt exits non-zero on any failing test -- that's fine, we still want
    # the failures recorded in dq_check_results, so downstream tasks use
    # TriggerRule.ALL_DONE instead of the default ALL_SUCCESS.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def load_dq_results():
        from dq_results_loader import load_dq_results

        load_dq_results()

    @task
    def load_business_metrics():
        from business_metrics_loader import sync_business_metrics

        sync_business_metrics()

    run_dbt_tests >> load_dq_results() >> load_business_metrics()


dashboard_sync_dag = dashboard_sync()