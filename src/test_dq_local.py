"""
src/test_dq_local.py

Combined local test harness and DQ engine runner for the banking lakehouse.
Reads batch CSVs from --data-dir, executes quality checks, and flushes
the results directly to Postgres audit.dq_check_results.
"""
from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

# Ensure src/ is on Python path regardless of execution directory
SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dq_checks import DQChecker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def print_results(dq: DQChecker) -> None:
    print("\n" + "=" * 90)
    print(f"{'LAYER':<10}{'TABLE':<25}{'CHECK':<35}{'STATUS':<8}{'FAILED/TOTAL'}")
    print("=" * 90)
    for r in dq.results:
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
    print(f"Total checks: {total}    Passed: {total - failed}    Failed: {failed}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local DQ test harness")
    parser.add_argument("--data-dir", default="data", help="Path to batch data directory")
    parser.add_argument(
        "--pg-conn",
        default="postgresql://etl_user:etl_password@etl-postgres:5432/etl_db",
        help="Postgres connection string for audit logging",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date string (YYYY-MM-DD) to test. Defaults to earliest file.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("local_dq_test")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = Path(args.data_dir)

    # ---- Resolve batch files ----
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

    dq = DQChecker(args.pg_conn, run_id=f"local_test__{day_str}", dag_id="local_dq_test")

    # ---- 1. TRANSACTIONS CHECKS ----
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
    dq.check_unique(txn_df, "bronze", "bronze_transactions", "transaction_id")
    dq.check_accepted_values(txn_df, "bronze", "bronze_transactions", "status",
                             ["SUCCESS", "FAILED", "REVERSED", "PENDING"])
    dq.check_positive_amount(txn_df, "bronze", "bronze_transactions")
    dq.check_reversal_reference(txn_df, "bronze", "bronze_transactions")

    # ---- 2. ACCOUNTS CHECKS ----
    if accounts_files:
        acc_df = spark.read.option("header", True).option("inferSchema", True).csv(str(accounts_files[-1]))
        dq.check_row_count(acc_df, "bronze", "bronze_accounts")
        dq.check_not_null(acc_df, "bronze", "bronze_accounts", ["account_id", "customer_id"])
        dq.check_unique(acc_df, "bronze", "bronze_accounts", "account_id")
        dq.check_fk_exists(txn_df, acc_df, "bronze", "bronze_transactions", "account_id", "account_id")

    # ---- 3. CUSTOMERS CHECKS ----
    if customers_files:
        cust_df = spark.read.option("header", True).option("inferSchema", True).csv(str(customers_files[-1]))
        dq.check_unique(cust_df, "bronze", "bronze_customers", "customer_id")

    # ---- 4. BALANCES CHECKS ----
    if balances_path.exists():
        bal_df = spark.read.option("header", True).option("inferSchema", True).csv(str(balances_path))
        total = bal_df.count()
        mismatch = bal_df.filter(
            F.abs(F.col("closing_balance") - (F.col("opening_balance") + F.col("credit_total") - F.col("debit_total"))) > 0.01
        ).count()
        dq._log("bronze", "bronze_balances", "balance_equation_holds", "business",
                passed=mismatch == 0, row_count=total, failed_count=mismatch)

    # ---- 5. SETTLEMENTS CHECKS ----
    if settlements_path.exists():
        settle_df = spark.read.option("header", True).option("inferSchema", True).csv(str(settlements_path))
        dq.check_unique(settle_df, "bronze", "bronze_settlements", "settlement_batch_id")
        dq.check_not_null(settle_df, "bronze", "bronze_settlements", ["settlement_date", "channel"])

    # Print summary table to console
    print_results(dq)

    # Write audit log to Postgres
    dq.flush()
    print(f"Successfully written {len(dq.results)} check results to audit.dq_check_results in Postgres!")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)