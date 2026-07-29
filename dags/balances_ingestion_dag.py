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

DQ_PG_CONN = os.environ.get("DQ_PG_CONN", "postgresql://postgres:postgres@postgres:5432/banking")
RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"


def _latest_delta_uri(prefix_str: str, table_name: str) -> str:
    hook = S3Hook(aws_conn_id="minio")
    prefix = f"{prefix_str}/{table_name}/"
    keys = hook.list_keys(bucket_name=LAKEHOUSE_BUCKET, prefix=prefix) or []

    timestamps = {
        key[len(prefix):].split("/", 1)[0]
        for key in keys
        if key.startswith(prefix) and key[len(prefix):]
    }
    if not timestamps:
        raise FileNotFoundError(f"No reference Delta table found for {table_name} under {prefix}")

    latest_timestamp = max(timestamps)
    return f"s3a://{LAKEHOUSE_BUCKET}/{prefix}{latest_timestamp}"


@dag(
    dag_id="balances_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "balances"],
)
def balances_ingestion():
    @task
    def load_balances():
        from reference_minio_loader import load_reference_csvs_to_minio

        folder = Path("/opt/airflow/data/batch/balances")
        results = []
        for csv_file in sorted(folder.glob("*.csv")):
            results.append(load_reference_csvs_to_minio(csv_file, prefix="balances"))
        return results

    # FIX: this task was previously missing entirely - the DAG had no way to
    # find the freshly-loaded Delta table's timestamped path.
    @task
    def resolve_latest_tables(dummy) -> dict[str, str]:
        return {"balances": _latest_delta_uri("balances", "balances")}

    # NEW: bronze DQ, including the balance-equation check specific to this
    # dataset - opening + credit - debit must equal closing (within rounding).
    @task
    def run_bronze_dq_checks(resolved_tables: dict[str, str], **context):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from dq_checks import DQChecker

        spark = SparkSession.builder.appName("bronze_dq_balances").getOrCreate()
        df = spark.read.format("delta").load(resolved_tables["balances"])

        dq = DQChecker(DQ_PG_CONN, run_id=context["run_id"], dag_id="balances_ingestion")
        dq.check_schema(df, "bronze", "bronze_balances", [
            "balance_date", "account_id", "opening_balance",
            "credit_total", "debit_total", "closing_balance", "currency",
        ])
        dq.check_row_count(df, "bronze", "bronze_balances")
        dq.check_not_null(df, "bronze", "bronze_balances", ["account_id", "balance_date"])
        dq.check_unique(df, "bronze", "bronze_balances", "account_id")  # one row/account/day expected

        # Balance equation check - not in dq_checks.py's generic methods, done inline.
        total = df.count()
        mismatch = df.filter(
            F.abs(F.col("closing_balance") - (F.col("opening_balance") + F.col("credit_total") - F.col("debit_total"))) > 0.01
        ).count()
        dq._log("bronze", "bronze_balances", "balance_equation_holds", "business",
                passed=mismatch == 0, row_count=total, failed_count=mismatch)
        dq.flush()

    dbt_run_stage = BashOperator(
        task_id="run_balances_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__balances silver_balance "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}'"
        ),
    )

    dbt_test_stage = BashOperator(
        task_id="test_balances_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__balances silver_balance "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}' "
            "|| true"
        ),
    )

    @task
    def publish_dbt_dq_results(**context):
        from dbt_dq_publisher import publish_dbt_test_results

        any_failed = publish_dbt_test_results(
            run_results_path=str(RUN_RESULTS_PATH),
            pg_conn_str=DQ_PG_CONN,
            run_id=context["run_id"],
            dag_id="balances_ingestion",
        )
        if any_failed:
            raise ValueError(
                "One or more dbt tests failed for balances - "
                "see audit.dq_check_results for details"
            )

    loaded_files = load_balances()
    resolved_tables = resolve_latest_tables(loaded_files)
    bronze_dq = run_bronze_dq_checks(resolved_tables)

    bronze_dq >> dbt_run_stage >> dbt_test_stage >> publish_dbt_dq_results()


balances_ingestion_dag = balances_ingestion()