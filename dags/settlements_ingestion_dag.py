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
    dag_id="settlements_ingestion",
    start_date=timezone.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion", "settlements"],
)
def settlements_ingestion():
    @task
    def load_settlements():
        from reference_minio_loader import load_reference_csvs_to_minio

        folder = Path("/opt/airflow/data/batch/settlements")
        results = []
        for csv_file in sorted(folder.glob("*.csv")):
            results.append(load_reference_csvs_to_minio(csv_file, prefix="settlements"))
        return results

    @task
    def resolve_latest_tables(dummy) -> dict[str, str]:
        return {"settlements": _latest_delta_uri("settlements", "settlements")}

    # NEW: bronze DQ before dbt runs.
    @task
    def run_bronze_dq_checks(resolved_tables: dict[str, str], **context):
        from pyspark.sql import SparkSession
        from dq_checks import DQChecker

        spark = SparkSession.builder.appName("bronze_dq_settlements").getOrCreate()
        df = spark.read.format("delta").load(resolved_tables["settlements"])

        dq = DQChecker(DQ_PG_CONN, run_id=context["run_id"], dag_id="settlements_ingestion")
        dq.check_schema(df, "bronze", "bronze_settlements", [
            "settlement_date", "channel", "transaction_type_code",
            "settled_transaction_count", "settled_gross_amount",
            "settled_fee_amount", "currency", "settlement_batch_id",
        ])
        dq.check_row_count(df, "bronze", "bronze_settlements")
        dq.check_not_null(df, "bronze", "bronze_settlements",
                           ["settlement_date", "channel", "transaction_type_code"])
        dq.check_unique(df, "bronze", "bronze_settlements", "settlement_batch_id")
        dq.flush()

    dbt_run_stage = BashOperator(
        task_id="run_settlements_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__settlements silver_settlement "
            f"--vars '{{{{ \"batch_table_uris\": ti.xcom_pull(task_ids=\"resolve_latest_tables\") | tojson }}}}'"
        ),
    )

    dbt_test_stage = BashOperator(
        task_id="test_settlements_dbt",
        bash_command=(
            f"{DBT_EXECUTABLE} test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target {DBT_TARGET} "
            f"--select stg_batch__settlements silver_settlement "
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
            dag_id="settlements_ingestion",
        )
        if any_failed:
            raise ValueError(
                "One or more dbt tests failed for settlements - "
                "see audit.dq_check_results for details"
            )

    # NEW: settlement reconciliation - runs LAST since it needs both
    # silver_settlement (from this DAG) and silver_transaction (from
    # transactions_ingestion) to already exist. If transactions_ingestion
    # hasn't run yet for the same date, this will show variances for
    # everything - that's expected, not a bug, on first run.
    @task
    def run_settlement_reconciliation():
        import psycopg2
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.appName("settlement_reconciliation").getOrCreate()

        fact_txn = spark.sql("SELECT * FROM analytics.silver_transaction")
        settlements = spark.sql("SELECT * FROM analytics.silver_settlement")

        computed = (
            fact_txn.filter(F.col("status") == "SUCCESS")
            .groupBy("transaction_date", "channel", "transaction_type_code")
            .agg(
                F.count("*").alias("computed_transaction_count"),
                F.sum("amount").alias("computed_gross_amount"),
            )
        )

        joined = settlements.join(
            computed,
            (settlements.settlement_date == computed.transaction_date)
            & (settlements.channel == computed.channel)
            & (settlements.transaction_type_code == computed.transaction_type_code),
            "left",
        )

        rows = joined.select(
            "settlement_date", settlements.channel, settlements.transaction_type_code,
            "settled_transaction_count", "computed_transaction_count",
            "settled_gross_amount", "computed_gross_amount",
        ).collect()

        conn = psycopg2.connect(DQ_PG_CONN)
        cur = conn.cursor()
        for r in rows:
            settled_amt = float(r["settled_gross_amount"] or 0)
            computed_amt = float(r["computed_gross_amount"] or 0)
            variance = round(settled_amt - computed_amt, 2)
            status = "MATCHED" if abs(variance) < 1.0 else (
                "MINOR_VARIANCE" if abs(variance) < 500 else "FAILED"
            )
            cur.execute("""
                INSERT INTO audit.settlement_variance
                (settlement_date, channel, transaction_type_code,
                 settled_transaction_count, computed_transaction_count,
                 settled_gross_amount, computed_gross_amount, variance_amount, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                r["settlement_date"], r["channel"], r["transaction_type_code"],
                r["settled_transaction_count"], r["computed_transaction_count"] or 0,
                settled_amt, computed_amt, variance, status,
            ))
        conn.commit()
        cur.close()
        conn.close()

    loaded_files = load_settlements()
    resolved_tables = resolve_latest_tables(loaded_files)
    bronze_dq = run_bronze_dq_checks(resolved_tables)
    published = publish_dbt_dq_results()

    bronze_dq >> dbt_run_stage >> dbt_test_stage >> published >> run_settlement_reconciliation()


settlements_ingestion_dag = settlements_ingestion()