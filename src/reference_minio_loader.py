# # from __future__ import annotations

# # import os
# # import time
# # from pathlib import Path
# # from typing import Any

# # import pandas as pd
# # from airflow.hooks.base import BaseHook
# # from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# # from botocore.exceptions import ClientError, EndpointConnectionError
# # from deltalake import write_deltalake

# # DEFAULT_BUCKET = "banking-lakehouse"
# # DEFAULT_REGION = "us-east-1"
# # DEFAULT_ENDPOINT = "http://minio:9000"
# # DEFAULT_ACCESS_KEY = "minioadmin"
# # DEFAULT_SECRET_KEY = "minioadmin123"
# # DEFAULT_CONN_ID = "minio"
# # DEFAULT_REFERENCE_PREFIX = "raw/reference"


# # def _resolve_value(value: str | None, env_name: str, fallback: str) -> str:
# #     if value:
# #         return value
# #     return os.environ.get(env_name, fallback)


# # def _s3_hook(conn_id: str) -> S3Hook:
# #     return S3Hook(aws_conn_id=conn_id)


# # def _connection_details(conn_id: str) -> dict[str, str]:
# #     connection = BaseHook.get_connection(conn_id)
# #     extras = connection.extra_dejson
# #     endpoint_url = extras.get("endpoint_url") or DEFAULT_ENDPOINT
# #     region = extras.get("region_name") or os.environ.get("MINIO_REGION", DEFAULT_REGION)
# #     access_key = connection.login or os.environ.get(
# #         "MINIO_ROOT_USER", DEFAULT_ACCESS_KEY
# #     )
# #     secret_key = connection.password or os.environ.get(
# #         "MINIO_ROOT_PASSWORD", DEFAULT_SECRET_KEY
# #     )
# #     return {
# #         "endpoint_url": endpoint_url,
# #         "region": region,
# #         "access_key": access_key,
# #         "secret_key": secret_key,
# #     }


# # def _wait_for_minio(client, attempts: int = 10, delay_seconds: float = 2.0) -> None:
# #     last_error: Exception | None = None
# #     for _ in range(attempts):
# #         try:
# #             client.list_buckets()
# #             return
# #         except (ClientError, EndpointConnectionError, OSError) as exc:
# #             last_error = exc
# #             time.sleep(delay_seconds)
# #     raise RuntimeError("MinIO is not reachable") from last_error


# # def _ensure_bucket(client, bucket_name: str) -> None:
# #     try:
# #         client.head_bucket(Bucket=bucket_name)
# #     except ClientError:
# #         client.create_bucket(Bucket=bucket_name)


# # def _storage_options(
# #     endpoint_url: str, access_key: str, secret_key: str, region: str
# # ) -> dict[str, str]:
# #     return {
# #         "AWS_ACCESS_KEY_ID": access_key,
# #         "AWS_SECRET_ACCESS_KEY": secret_key,
# #         "AWS_REGION": region,
# #         "AWS_ENDPOINT_URL": endpoint_url,
# #         "AWS_ALLOW_HTTP": "true",
# #         "AWS_S3_ADDRESSING_STYLE": "path",
# #     }


# # def load_reference_csvs_to_minio(
# #     file_path: str | Path,
# #     bucket_name: str | None = None,
# #     prefix: str | None = None,
# #     conn_id: str | None = None,
# # ) -> dict[str, Any]:
# #     csv_path = Path(file_path)
# #     if not csv_path.exists():
# #         raise FileNotFoundError(f"CSV file not found: {csv_path}")

# #     resolved_bucket = _resolve_value(bucket_name, "MINIO_BUCKET", DEFAULT_BUCKET)
# #     resolved_prefix = _resolve_value(
# #         prefix, "MINIO_REFERENCE_PREFIX", DEFAULT_REFERENCE_PREFIX
# #     ).strip("/")
# #     resolved_conn_id = _resolve_value(conn_id, "MINIO_AWS_CONN_ID", DEFAULT_CONN_ID)
# #     connection_details = _connection_details(resolved_conn_id)

# #     hook = _s3_hook(resolved_conn_id)
# #     client = hook.get_conn()
# #     _wait_for_minio(client)
# #     _ensure_bucket(client, resolved_bucket)

# #     frame = pd.read_csv(csv_path)
# #     storage_options = _storage_options(
# #         endpoint_url=connection_details["endpoint_url"],
# #         access_key=connection_details["access_key"],
# #         secret_key=connection_details["secret_key"],
# #         region=connection_details["region"],
# #     )

# #     raw_key = f"{resolved_prefix}/csv/{csv_path.name}"
# #     with csv_path.open("rb") as fileobj:
# #         client.upload_fileobj(Fileobj=fileobj, Bucket=resolved_bucket, Key=raw_key)

# #     table_name = csv_path.stem
# #     table_uri = f"s3://{resolved_bucket}/{resolved_prefix}/delta/{table_name}"
# #     write_deltalake(
# #         table_uri,
# #         frame,
# #         mode="overwrite",
# #         storage_options=storage_options,
# #     )

# #     return {
# #         "bucket": resolved_bucket,
# #         "prefix": resolved_prefix,
# #         "file": csv_path.name,
# #         "rows": len(frame),
# #         "delta_table": table_uri,
# #         "raw_key": raw_key,
# #     }


# # if __name__ == "__main__":
# #     reference_dir = Path(__file__).resolve().parents[1] / "data" / "reference"
# #     results = []
# #     for csv_path in sorted(reference_dir.glob("*.csv")):
# #         results.append(load_reference_csvs_to_minio(csv_path))
# #     print(results)


# from datetime import datetime

# import pandas as pd
# import pyarrow as pa
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from deltalake import write_deltalake

# hook = S3Hook(aws_conn_id="minio")


# def load_reference_csvs_to_minio(file: str, prefix: str) -> None:
#     if not hook.check_for_bucket("landing"):
#         hook.create_bucket(bucket_name="landing")

#     timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#     print("reading csv")
#     df = pd.read_csv(file)
#     print("completed reading csv")

#     print("converting to pyarrow table")
#     table = pa.Table.from_pandas(df)
#     print("completed converting to pyarrow table")

#     print("writing to delta")
#     write_deltalake(
#         f"s3://landing/{prefix}/{file.name.split('.')[0]}/{timestamp}",
#         table,
#         mode="overwrite",
#         storage_options={
#             "AWS_ENDPOINT_URL": "http://minio:9000",
#             "AWS_ACCESS_KEY_ID": "minioadmin",
#             "AWS_SECRET_ACCESS_KEY": "minioadmin123",
#             "AWS_ALLOW_HTTP": "true",
#             "AWS_S3_ADDRESSING_STYLE": "path",
#         },
#     )
#     print("completed writing to delta")



from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from deltalake import write_deltalake
from pyarrow import csv


def load_reference_csvs_to_minio(file, prefix: str) -> None:
    hook = S3Hook(aws_conn_id="minio")

    if not hook.check_for_bucket("landing"):
        hook.create_bucket(bucket_name="landing")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    print("reading csv")
    table = csv.read_csv(file)
    print(f"completed reading csv ({table.num_rows:,} rows)")

    print("writing to delta")
    write_deltalake(
        f"s3://landing/{prefix}/{file.stem}/{timestamp}",
        table,
        mode="overwrite",
        storage_options={
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin123",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
        },
    )
    print("completed writing to delta")