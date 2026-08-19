"""Sync Gold-layer transaction facts into etl-postgres for the Business
Metrics dashboard page.

Reads fact_transaction (gold) + dim_account/dim_branch (silver) straight
from MinIO via delta-rs (the `deltalake` package, no Spark needed), rolls
them up to one row per transaction_date/branch/channel/status, and upserts
into business_metrics_daily.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import psycopg2
from deltalake import DeltaTable
from psycopg2.extras import execute_values

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_REGION": os.environ.get("MINIO_REGION", "us-east-1"),
}

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "etl-postgres"),
    port=int(os.environ.get("POSTGRES_PORT", 5432)),
    dbname=os.environ.get("POSTGRES_DB", "etl_db"),
    user=os.environ.get("POSTGRES_USER", "etl_user"),
    password=os.environ.get("POSTGRES_PASSWORD", "etl_password"),
)


def _read_delta(bucket: str, table: str) -> pd.DataFrame:
    uri = f"s3://{bucket}/{table}"
    return DeltaTable(uri, storage_options=STORAGE_OPTIONS).to_pandas()


def build_business_metrics_daily(lookback_days: int = 30) -> pd.DataFrame:
    fact_transaction = _read_delta("gold", "fact_transaction")
    dim_account = _read_delta("silver", "dim_account")
    dim_branch = _read_delta("silver", "dim_branch")

    fact_transaction["transaction_date"] = pd.to_datetime(
        fact_transaction["transaction_date"]
    ).dt.date

    cutoff = date.today() - timedelta(days=lookback_days)
    fact_transaction = fact_transaction[fact_transaction["transaction_date"] >= cutoff]

    enriched = fact_transaction.merge(
        dim_account[["account_id", "branch_id"]], on="account_id", how="left"
    ).merge(dim_branch[["branch_id", "branch_name"]], on="branch_id", how="left")
    enriched["branch_name"] = enriched["branch_name"].fillna("UNKNOWN")

    grouped = (
        enriched.groupby(
            ["transaction_date", "branch_name", "channel", "status"], dropna=False
        )
        .agg(
            transaction_count=("transaction_id", "count"),
            total_amount=("amount", "sum"),
        )
        .reset_index()
    )
    return grouped


def upsert_business_metrics(df: pd.DataFrame) -> int:
    if df.empty:
        print("No business metrics rows to load.")
        return 0

    records = list(
        df[
            [
                "transaction_date",
                "branch_name",
                "channel",
                "status",
                "transaction_count",
                "total_amount",
            ]
        ].itertuples(index=False, name=None)
    )

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO business_metrics_daily
                    (transaction_date, branch_name, channel, status,
                     transaction_count, total_amount)
                VALUES %s
                ON CONFLICT (transaction_date, branch_name, channel, status)
                DO UPDATE SET
                    transaction_count = EXCLUDED.transaction_count,
                    total_amount = EXCLUDED.total_amount
                """,
                records,
            )
    finally:
        conn.close()

    print(f"Upserted {len(records)} business_metrics_daily rows.")
    return len(records)


def sync_business_metrics(lookback_days: int = 30) -> int:
    df = build_business_metrics_daily(lookback_days=lookback_days)
    return upsert_business_metrics(df)


if __name__ == "__main__":
    sync_business_metrics()