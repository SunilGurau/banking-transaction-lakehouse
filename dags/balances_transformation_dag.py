from __future__ import annotations

import os
import sys
from pathlib import Path
import json
import subprocess

from airflow.decorators import dag, task
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
        print(f"Resolving balance file for logical date: {logical_date}")
        filename = f"account_balances_{logical_date}.csv"
        
        # FIXED: Added target_date so it knows exactly which historical folder to pull from
        return latest_incremental_file_uri("balances", filename)

    @task
    def run_balances_dbt(balance_uri: str) -> None:
        print(f"Running dbt with balance URI: {balance_uri}")
        
        var_payload = {"batch_table_uris": {"balances": balance_uri}}
        vars_str = json.dumps(var_payload)

        cmd = [
            str(DBT_EXECUTABLE),
            "run",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
            "--target", DBT_TARGET,
            "--select", "stg_batch__balances",
            "--vars", vars_str
        ]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)
        
        if result.returncode != 0:
            print("ERROR OUTPUT:")
            print(result.stderr)
            raise RuntimeError("dbt balances model execution failed")

    # Clean functional data dependency mapping
    balance_file = resolve_balance_file()
    run_balances_dbt(balance_file)


balances_transformation_dag = balances_transformation()