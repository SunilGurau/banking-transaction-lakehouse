"""
src/dbt_dq_publisher.py

Reads dbt's target/run_results.json (produced automatically after any
`dbt test` or `dbt build` invocation) and writes each test result into
audit.dq_check_results, using the same schema as the Python/Spark bronze
checks in dq_checks.py. This lets one query/dashboard cover both.

dbt result statuses map as:
    pass  -> PASS
    warn  -> PASS   (treated as pass but noted in details)
    fail  -> FAIL
    error -> FAIL
    skipped -> skipped entirely (not logged)
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg2

STATUS_MAP = {
    "pass": "PASS",
    "warn": "PASS",
    "fail": "FAIL",
    "error": "FAIL",
}

# dbt model name -> (layer, table_name) for cleaner rows in the audit table.
# Extend this as you add more models.
MODEL_LAYER_MAP = {
    "stg_batch__transactions": ("silver", "stg_batch__transactions"),
    "stg_batch__accounts": ("silver", "stg_batch__accounts"),
    "stg_batch__customers": ("silver", "stg_batch__customers"),
    "stg_batch__balances": ("silver", "stg_batch__balances"),
    "stg_batch__settlements": ("silver", "stg_batch__settlements"),
    "bronze_transactions": ("bronze", "bronze_transactions"),
    "bronze_accounts": ("bronze", "bronze_accounts"),
    "bronze_customers": ("bronze", "bronze_customers"),
    "bronze_transaction_events": ("bronze", "bronze_transaction_events"),
}


def _resolve_layer_and_table(node_name: str, unique_id: str) -> tuple[str, str]:
    """
    dbt test unique_ids look like:
      test.banking_analytics.not_null_stg_batch__transactions_transaction_id.<hash>
      test.banking_analytics.relationships_stg_batch__transactions_account_id__account_id__ref_stg_batch__accounts_.<hash>
    We match on any known model name appearing in the unique_id; fall back to "unknown".
    """
    for model_name, (layer, table) in MODEL_LAYER_MAP.items():
        if model_name in unique_id:
            return layer, table
    return "unknown", node_name


def publish_dbt_test_results(run_results_path: str, pg_conn_str: str,
                              run_id: str, dag_id: str) -> bool:
    """
    Parses run_results_path (target/run_results.json) and inserts one row
    per test into audit.dq_check_results.

    Returns True if any test failed (so the caller can raise/fail the DAG).
    """
    path = Path(run_results_path)
    if not path.exists():
        raise FileNotFoundError(f"dbt run_results.json not found at {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    rows: list[tuple] = []
    any_failed = False

    for result in data.get("results", []):
        if result.get("status") == "skipped":
            continue

        unique_id = result.get("unique_id", "")
        # only process test nodes, skip model run results if run_results.json
        # came from `dbt build` rather than `dbt test`
        if not unique_id.startswith("test."):
            continue

        node_name = unique_id.split(".")[2] if len(unique_id.split(".")) > 2 else unique_id
        layer, table_name = _resolve_layer_and_table(node_name, unique_id)

        dbt_status = result.get("status", "error")
        status = STATUS_MAP.get(dbt_status, "FAIL")
        if status == "FAIL":
            any_failed = True

        failures = result.get("failures") or 0
        message = result.get("message") or ""

        rows.append((
            run_id,
            dag_id,
            layer,
            table_name,
            node_name,
            "dbt_test",
            status,
            0,               # row_count not available from run_results.json
            failures,
            f"dbt_status={dbt_status}; message={message}"[:2000],
        ))

    if rows:
        conn = psycopg2.connect(pg_conn_str)
        try:
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO audit.dq_check_results
                (run_id, dag_id, layer, table_name, check_name, check_type,
                 status, row_count, failed_count, details)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            conn.commit()
            cur.close()
        finally:
            conn.close()

    return any_failed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Publish dbt test results to audit.dq_check_results")
    parser.add_argument("--run-results", required=True, help="Path to target/run_results.json")
    parser.add_argument("--pg-conn", required=True, help="Postgres connection string")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dag-id", required=True)
    args = parser.parse_args()

    failed = publish_dbt_test_results(args.run_results, args.pg_conn, args.run_id, args.dag_id)
    if failed:
        raise SystemExit("One or more dbt tests FAILED - see audit.dq_check_results")
    print("All dbt tests passed and were logged to audit.dq_check_results")