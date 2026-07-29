"""
test_dq_local.py

Standalone test harness for dq_checks.py. Run this against the output of
generate_all.py directly, using local Spark - no Airflow, MinIO, or Kafka
required. Postgres is optional: pass --pg-conn to actually write results
to audit.dq_check_results, or omit it to just print results to console.

Usage:
    python test_dq_local.py --data-dir test_output
    python test_dq_local.py --data-dir test_output --pg-conn "postgresql://postgres:postgres@localhost:5432/banking"

Requires: pyspark, pandas (optional pretty printing), psycopg2 (only if --pg-conn given)
"""
from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

print("DEBUG: script started", flush=True)

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
print(f"DEBUG: sys.path updated, python={sys.executable}", flush=True)


def print_results(dq) -> None:
    print("\n" + "=" * 90)
    print(f"{'LAYER':<10}{'TABLE':<25}{'CHECK':<35}{'STATUS':<8}{'FAILED/TOTAL'}")
    print("=" * 90)
    for r in dq.results:
        # r = (run_id, dag_id, layer, table, check_name, check_type, status, row_count, failed_count, details)
        layer, table, check_name, _, status, row_count, failed_count, details = (
            r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]
        )
        marker = "OK " if status == "PASS" else "FAIL"
        print(f"{layer:<10}{table:<25}{check_name:<35}{marker:<8}{failed_count}/{row_count}")
        if status == "FAIL" and details:
            print(f"          -> {details}")
    print("=" * 90)
    total = len(dq.results)
    failed = sum(1 for r in dq.results if r[6] == "FAIL")
    print(f"Total checks: {total}   Passed: {total - failed}   Failed: {failed}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local DQ test harness")
    parser.add_argument("--data-dir", required=True, help="Output dir from generate_all.py")
    parser.add_argument("--pg-conn", default=None,
                         help="Optional Postgres conn string. If omitted, results print to console only.")
    parser.add_argument("--date", default=None,
                         help="Which day's transactions/balances/settlements to test (YYYY-MM-DD). "
                              "Defaults to the earliest file found.")
    args = parser.parse_args()

    print("DEBUG: importing pyspark...", flush=True)
    from pyspark.sql import SparkSession
    from dq_checks import DQChecker
    print("DEBUG: pyspark imported OK, creating SparkSession...", flush=True)

    spark = (
        SparkSession.builder
        .appName("local_dq_test")
        .master("local[*]")
        .getOrCreate()
    )
    print("DEBUG: SparkSession created OK", flush=True)
    spark.sparkContext.setLogLevel("WARN")

    base = Path(args.data_dir)

    # ---- resolve which day's files to use ----
    txn_files = sorted((base / "batch" / "transactions").glob("transactions_*.csv"))
    if not txn_files:
        raise SystemExit(f"No transaction files found under {base / 'batch' / 'transactions'}")
    if args.date:
        txn_path = base / "batch" / "transactions" / f"transactions_{args.date}.csv"
        if not txn_path.exists():
            raise SystemExit(f"No file for date {args.date}: {txn_path}")
    else:
        txn_path = txn_files[0]
    day_str = txn_path.stem.replace("transactions_", "")
    print(f"Testing DQ checks against day: {day_str}")

    accounts_files = sorted((base / "batch" / "accounts").glob("*.csv"))
    customers_files = sorted((base / "batch" / "customers").glob("*.csv"))
    balances_path = base / "batch" / "balances" / f"account_balances_{day_str}.csv"
    settlements_path = base / "batch" / "settlements" / f"settlement_{day_str}.csv"

    dq = DQChecker(args.pg_conn or "unused", run_id=f"local_test__{day_str}", dag_id="local_dq_test")

    # ---- TRANSACTIONS ----
    txn_df = spark.read.option("header", True).option("inferSchema", True).csv(str(txn_path))
    dq.check_schema(txn_df, "bronze", "bronze_transactions", [
        "transaction_id", "account_id", "customer_id", "branch_id",
        "transaction_ts", "transaction_date", "transaction_type_code",
        "channel", "merchant_category_code", "amount", "fee_amount",
        "status", "currency", "original_transaction_id", "source_system",
    ])
    dq.check_row_count(txn_df, "bronze", "bronze_transactions")
    dq.check_not_null(txn_df, "bronze", "bronze_transactions",
                       ["transaction_id", "account_id", "customer_id", "branch_id"])
    dq.check_unique(txn_df, "bronze", "bronze_transactions", "transaction_id")  # EXPECT FAIL (dupes are intentional)
    dq.check_accepted_values(txn_df, "bronze", "bronze_transactions", "status",
                              ["SUCCESS", "FAILED", "REVERSED", "PENDING"])
    dq.check_positive_amount(txn_df, "bronze", "bronze_transactions")
    dq.check_reversal_reference(txn_df, "bronze", "bronze_transactions")

    # ---- ACCOUNTS ----
    if accounts_files:
        acc_df = spark.read.option("header", True).option("inferSchema", True).csv(str(accounts_files[-1]))
        dq.check_row_count(acc_df, "bronze", "bronze_accounts")
        dq.check_not_null(acc_df, "bronze", "bronze_accounts", ["account_id", "customer_id"])
        dq.check_unique(acc_df, "bronze", "bronze_accounts", "account_id")
        dq.check_fk_exists(txn_df, acc_df, "bronze", "bronze_transactions", "account_id", "account_id")

    # ---- CUSTOMERS ----
    if customers_files:
        cust_df = spark.read.option("header", True).option("inferSchema", True).csv(str(customers_files[-1]))
        dq.check_unique(cust_df, "bronze", "bronze_customers", "customer_id")

    # ---- BALANCES (equation check) ----
    if balances_path.exists():
        from pyspark.sql import functions as F
        bal_df = spark.read.option("header", True).option("inferSchema", True).csv(str(balances_path))
        total = bal_df.count()
        mismatch = bal_df.filter(
            F.abs(F.col("closing_balance") - (F.col("opening_balance") + F.col("credit_total") - F.col("debit_total"))) > 0.01
        ).count()
        dq._log("bronze", "bronze_balances", "balance_equation_holds", "business",
                passed=mismatch == 0, row_count=total, failed_count=mismatch)

    # ---- SETTLEMENTS (basic checks; full reconciliation needs both silver tables) ----
    if settlements_path.exists():
        settle_df = spark.read.option("header", True).option("inferSchema", True).csv(str(settlements_path))
        dq.check_unique(settle_df, "bronze", "bronze_settlements", "settlement_batch_id")
        dq.check_not_null(settle_df, "bronze", "bronze_settlements", ["settlement_date", "channel"])

    print_results(dq)

    if args.pg_conn:
        dq.flush()
        print(f"Results written to audit.dq_check_results via {args.pg_conn}")
    else:
        print("No --pg-conn given, so nothing was written to Postgres. Add --pg-conn to test the DB write path.")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
        print("DEBUG: main() completed successfully", flush=True)
    except SystemExit as e:
        print(f"DEBUG: SystemExit raised: {e}", flush=True)
        raise
    except Exception:
        print("DEBUG: an exception was raised - full traceback below:", flush=True)
        traceback.print_exc()
        sys.exit(1)