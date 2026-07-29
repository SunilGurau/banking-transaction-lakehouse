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

# NEW: Postgres connection string for the DQ audit database.
# Move this to an Airflow Connection (e.g. "audit_postgres") rather than
# hardcoding once you're past local dev.
DQ_PG_CONN = os.environ.get(
    "DQ_PG_CONN", "postgresql://postgres:postgres@postgres:5432/banking"
)
RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"


def _latest_delta_uri(prefix_str: str) -> str:
    hook = S3Hook(aws_conn_id="minio")
    root_prefix = f"{prefix_str}/"
    keys = hook.list_keys(bucket_name=LAKEHOUSE_BUCKET, prefix=root_prefix) or []

    entries = set()
    for key in keys:
        remainder = key[len(root_prefix):]
        parts = remainder.split("/")
        if len(parts) >= 2 and parts[0] and parts[1]:
            entries.add((parts[0], parts[1]))

    if not entries:
        raise FileNotFoundError(f"No Delta table found under {root_prefix}")

    latest_stem, latest_timestamp = max(entries)
    return f"s3a://{LAKEHOUSE_BUCKET}/{root_prefix}{latest_stem}/{latest_timestamp}"


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
            "accounts": _latest_delta_uri("accounts"),
            "customers": _latest_delta_uri("customers"),
        }

    dbt_run_stage = BashOperator(
        task_id="run_accounts_customers_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__customers stg_batch__accounts dim_customer dim_account "
            "--vars '{{ {\"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\")} | tojson }}'"
        ),
    )

    # NEW: run dbt tests against the same model selection. `--select` mirrors
    # the run stage's selection plus the "+" to catch any downstream deps.
    # dbt writes target/run_results.json regardless of pass/fail, and this
    # BashOperator is intentionally NOT set to fail the DAG on test failure
    # (dbt exits non-zero on test fail) - we want the publish task below to
    # run either way and log the failures, then decide whether to raise.
    dbt_test_stage = BashOperator(
        task_id="test_accounts_customers_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__customers stg_batch__accounts dim_customer dim_account "
            "--vars '{{ {\"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\")} | tojson }}' "
            "|| true"  # don't let Airflow mark this failed; let the publish task decide
        ),
    )

    # NEW: parse run_results.json and push rows into audit.dq_check_results.
    # This task raises (failing the DAG) if any dbt test actually failed,
    # which is the point where you want visibility/alerting to trigger.
    @task
    def publish_dbt_dq_results(**context):
        from dbt_dq_publisher import publish_dbt_test_results

        run_id = context["run_id"]
        any_failed = publish_dbt_test_results(
            run_results_path=str(RUN_RESULTS_PATH),
            pg_conn_str=DQ_PG_CONN,
            run_id=run_id,
            dag_id="accounts_customers_ingestion",
        )
        if any_failed:
            raise ValueError(
                "One or more dbt tests failed for accounts/customers - "
                "see audit.dq_check_results for details"
            )

    loaded_files = load_accounts_and_customers()
    resolved_tables = resolve_latest_tables(loaded_files)
    resolved_tables >> dbt_run_stage >> dbt_test_stage >> publish_dbt_dq_results()


accounts_customers_ingestion_dag = accounts_customers_ingestion()