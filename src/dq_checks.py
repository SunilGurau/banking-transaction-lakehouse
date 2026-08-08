"""
src/dq_checks.py

Reusable Bronze/Silver data-quality checker for the banking lakehouse.
Call this from Spark ingestion jobs (batch or streaming) and it logs every
check result to Postgres audit.dq_check_results.

Column names below match generate_all.py exactly - update here if the
generator output changes.
"""
from __future__ import annotations

from pyspark.sql import DataFrame


class DQChecker:
    def __init__(self, pg_conn_str: str, run_id: str, dag_id: str):
        self.pg_conn_str = pg_conn_str
        self.run_id = run_id
        self.dag_id = dag_id
        self.results: list[tuple] = []

    # ---- individual checks -------------------------------------------------

    def check_schema(self, df: DataFrame, layer: str, table_name: str, expected_cols: list[str]) -> None:
        actual = set(df.columns)
        missing = set(expected_cols) - actual
        self._log(layer, table_name, "schema_columns", "schema",
                   passed=len(missing) == 0, details=f"missing={sorted(missing)}")

    def check_row_count(self, df: DataFrame, layer: str, table_name: str, min_rows: int = 1) -> None:
        count = df.count()
        self._log(layer, table_name, "row_count_min", "schema",
                   passed=count >= min_rows, row_count=count,
                   details=f"expected>={min_rows}")

    def check_not_null(self, df: DataFrame, layer: str, table_name: str, columns: list[str]) -> None:
        total = df.count()
        dtypes = dict(df.dtypes)
        for c in columns:
            condition = df[c].isNull()
            if dtypes.get(c) == "string":
                condition = condition | (df[c] == "")
            null_count = df.filter(condition).count()
            self._log(layer, table_name, f"not_null_{c}", "null",
                    passed=null_count == 0, row_count=total, failed_count=null_count)

    def check_unique(self, df: DataFrame, layer: str, table_name: str, key_col: str) -> None:
        total = df.count()
        distinct = df.select(key_col).distinct().count()
        self._log(layer, table_name, f"unique_{key_col}", "unique",
                   passed=total == distinct, row_count=total, failed_count=total - distinct,
                   details="generator intentionally injects duplicate transaction_ids - expect FAIL pre-dedup")

    def check_accepted_values(self, df: DataFrame, layer: str, table_name: str, column: str, allowed: list[str]) -> None:
        total = df.count()
        bad = df.filter(~df[column].isin(allowed)).count()
        self._log(layer, table_name, f"accepted_values_{column}", "accepted_values",
                   passed=bad == 0, row_count=total, failed_count=bad,
                   details=f"allowed={allowed}")

    def check_positive_amount(self, df: DataFrame, layer: str, table_name: str, column: str = "amount") -> None:
        total = df.count()
        # generator injects ~0.2% zero-amount rows on purpose
        negative = df.filter(df[column] < 0).count()
        zero = df.filter(df[column] == 0).count()
        self._log(layer, table_name, f"non_negative_{column}", "range",
                   passed=negative == 0, row_count=total, failed_count=negative,
                   details=f"zero_amount_rows={zero} (expected, generator injects these)")

    def check_fk_exists(self, child_df: DataFrame, parent_df: DataFrame, layer: str,
                         table_name: str, fk_col: str, parent_key_col: str) -> None:
        total = child_df.count()
        orphans = child_df.join(parent_df.select(parent_key_col).distinct(),
                                 child_df[fk_col] == parent_df[parent_key_col], "left_anti").count()
        self._log(layer, table_name, f"fk_{fk_col}_exists", "fk",
                   passed=orphans == 0, row_count=total, failed_count=orphans,
                   details=f"references {parent_key_col}")

    def check_reversal_reference(self, df: DataFrame, layer: str, table_name: str) -> None:
        """REVERSED transactions must carry a non-empty original_transaction_id."""
        reversed_df = df.filter(df["status"] == "REVERSED")
        total = reversed_df.count()
        missing_ref = reversed_df.filter(
            (df["original_transaction_id"].isNull()) | (df["original_transaction_id"] == "")
        ).count()
        self._log(layer, table_name, "reversal_has_original_txn_id", "business",
                   passed=missing_ref == 0, row_count=total, failed_count=missing_ref)

    # ---- persistence --------------------------------------------------------

    def _log(self, layer, table_name, check_name, check_type, passed,
             row_count=0, failed_count=0, details="") -> None:
        status = "PASS" if passed else "FAIL"
        self.results.append((
            self.run_id, self.dag_id, layer, table_name, check_name,
            check_type, status, row_count, failed_count, details
        ))

    def flush(self) -> None:
        if not self.results:
            return
        import psycopg2  # imported here so DQChecker can be used standalone
                          # (e.g. in test_dq_local.py without --pg-conn) without
                          # requiring psycopg2 to be installed at all.
        conn = psycopg2.connect(self.pg_conn_str)
        try:
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO audit.dq_check_results
                (run_id, dag_id, layer, table_name, check_name, check_type,
                 status, row_count, failed_count, details)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, self.results)
            conn.commit()
            cur.close()
        finally:
            conn.close()

    def any_failed(self) -> bool:
        return any(r[6] == "FAIL" for r in self.results)


# ==========================================================================
# Example: wiring into your transactions bronze load
# ==========================================================================
if __name__ == "__main__":
    """
    Example usage - adapt paths/conn string to your environment.
    Run this after Spark reads transactions_<date>.csv into bronze_transactions_df,
    and accounts into bronze_accounts_df.
    """
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.getOrCreate()
    # bronze_transactions_df = spark.read.option("header", True).csv("s3a://bronze/transactions/...")
    # bronze_accounts_df = spark.read.option("header", True).csv("s3a://bronze/accounts/...")

    PG_CONN = "postgresql://postgres:postgres@localhost:5432/banking"

    EXPECTED_TXN_COLS = [
        "transaction_id", "account_id", "customer_id", "branch_id",
        "transaction_ts", "transaction_date", "transaction_type_code",
        "channel", "merchant_category_code", "amount", "fee_amount",
        "status", "currency", "original_transaction_id", "source_system",
    ]

    # dq = DQChecker(PG_CONN, run_id="manual__2026-07-29", dag_id="transactions_ingestion")
    # dq.check_schema(bronze_transactions_df, "bronze", "bronze_transactions", EXPECTED_TXN_COLS)
    # dq.check_row_count(bronze_transactions_df, "bronze", "bronze_transactions")
    # dq.check_not_null(bronze_transactions_df, "bronze", "bronze_transactions",
    #                    ["transaction_id", "account_id", "customer_id", "branch_id"])
    # dq.check_unique(bronze_transactions_df, "bronze", "bronze_transactions", "transaction_id")
    # dq.check_accepted_values(bronze_transactions_df, "bronze", "bronze_transactions", "status",
    #                            ["SUCCESS", "FAILED", "REVERSED", "PENDING"])
    # dq.check_positive_amount(bronze_transactions_df, "bronze", "bronze_transactions")
    # dq.check_reversal_reference(bronze_transactions_df, "bronze", "bronze_transactions")
    # dq.check_fk_exists(bronze_transactions_df, bronze_accounts_df, "bronze", "bronze_transactions",
    #                      "account_id", "account_id")
    # dq.flush()
    # if dq.any_failed():
    #     raise ValueError("Bronze DQ checks failed for transactions - see audit.dq_check_results")
    pass