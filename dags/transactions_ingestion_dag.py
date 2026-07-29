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
    dag_id="transactions_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "transactions"],
)
def transactions_ingestion():
    @task
    def load_transactions():
        from reference_minio_loader import load_reference_csvs_to_minio

        folder = Path("/opt/airflow/data/batch/transactions")
        results = []
        for csv_file in sorted(folder.glob("*.csv")):
            results.append(load_reference_csvs_to_minio(csv_file, prefix="transactions"))
        return results

    @task
    def resolve_latest_tables(dummy) -> dict[str, str]:
        return {"transactions": _latest_delta_uri("transactions", "transactions")}

    # NEW: bronze DQ, run against the just-resolved Delta table before dbt touches it.
    @task
    def run_bronze_dq_checks(resolved_tables: dict[str, str], **context):
        from pyspark.sql import SparkSession
        from dq_checks import DQChecker

        spark = SparkSession.builder.appName("bronze_dq_transactions").getOrCreate()
        df = spark.read.format("delta").load(resolved_tables["transactions"])

        dq = DQChecker(DQ_PG_CONN, run_id=context["run_id"], dag_id="transactions_ingestion")
        dq.check_schema(df, "bronze", "bronze_transactions", [
            "transaction_id", "account_id", "customer_id", "branch_id",
            "transaction_ts", "transaction_date", "transaction_type_code",
            "channel", "merchant_category_code", "amount", "fee_amount",
            "status", "currency", "original_transaction_id", "source_system",
        ])
        dq.check_row_count(df, "bronze", "bronze_transactions")
        dq.check_not_null(df, "bronze", "bronze_transactions",
                           ["transaction_id", "account_id", "customer_id", "branch_id"])
        # NOT gating on this - generator injects duplicate transaction_ids on
        # purpose; dedup + real uniqueness enforcement happens in silver_transaction.
        dq.check_unique(df, "bronze", "bronze_transactions", "transaction_id")
        dq.check_accepted_values(df, "bronze", "bronze_transactions", "status",
                                  ["SUCCESS", "FAILED", "REVERSED", "PENDING"])
        dq.check_positive_amount(df, "bronze", "bronze_transactions")
        dq.check_reversal_reference(df, "bronze", "bronze_transactions")
        dq.flush()

    dbt_run_stage = BashOperator(
        task_id="run_transactions_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__transactions silver_transaction "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}'"
        ),
    )

    # NEW: dbt test, non-blocking so the publish task always runs and logs results.
    dbt_test_stage = BashOperator(
        task_id="test_transactions_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__transactions silver_transaction "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}' "
            "|| true"
        ),
    )

    # NEW: parse run_results.json into audit.dq_check_results, raise if any failed.
    @task
    def publish_dbt_dq_results(**context):
        from dbt_dq_publisher import publish_dbt_test_results

        any_failed = publish_dbt_test_results(
            run_results_path=str(RUN_RESULTS_PATH),
            pg_conn_str=DQ_PG_CONN,
            run_id=context["run_id"],
            dag_id="transactions_ingestion",
        )
        if any_failed:
            raise ValueError(
                "One or more dbt tests failed for transactions - "
                "see audit.dq_check_results for details"
            )

    loaded_files = load_transactions()
    resolved_tables = resolve_latest_tables(loaded_files)
    bronze_dq = run_bronze_dq_checks(resolved_tables)

    bronze_dq >> dbt_run_stage >> dbt_test_stage >> publish_dbt_dq_results()


transactions_ingestion_dag = transactions_ingestion()