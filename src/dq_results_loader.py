"""Load dbt test results into etl-postgres for the Pipeline Health dashboard.

Reads target/run_results.json, which dbt writes after every `dbt run` or
`dbt test`, and inserts one row per test node into dq_check_results.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

RUN_RESULTS_PATH = Path(
    os.environ.get(
        "DBT_RUN_RESULTS_PATH",
        "/opt/airflow/dbt/banking_analytics/target/run_results.json",
    )
)

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "etl-postgres"),
    port=int(os.environ.get("POSTGRES_PORT", 5432)),
    dbname=os.environ.get("POSTGRES_DB", "etl_db"),
    user=os.environ.get("POSTGRES_USER", "etl_user"),
    password=os.environ.get("POSTGRES_PASSWORD", "etl_password"),
)


def load_dq_results(run_results_path: Path = RUN_RESULTS_PATH) -> int:
    if not run_results_path.exists():
        raise FileNotFoundError(
            f"dbt run_results.json not found at {run_results_path}. "
            "Run `dbt test` before calling this loader."
        )

    with run_results_path.open() as f:
        run_results = json.load(f)

    generated_at = run_results.get("metadata", {}).get("generated_at")
    run_at = (
        datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        if generated_at
        else datetime.now(timezone.utc)
    )

    rows = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue  # skip model-run results, only dbt tests feed this table

        check_name = unique_id.split(".")[-1]
        status = result.get("status", "unknown")
        execution_time = result.get("execution_time")
        failures = result.get("failures") or 0

        rows.append((run_at, check_name, status, execution_time, failures))

    if not rows:
        print("No dbt test results found in run_results.json; nothing to load.")
        return 0

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dq_check_results
                    (run_at, check_name, status, execution_time, failures)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
    finally:
        conn.close()

    print(f"Loaded {len(rows)} dq_check_results rows for run_at={run_at}")
    return len(rows)


if __name__ == "__main__":
    load_dq_results()