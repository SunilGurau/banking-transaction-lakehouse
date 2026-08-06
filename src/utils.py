import re

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

LAKEHOUSE_BUCKET = "landing"


def list_subfolders(
    bucket_name: str,
    prefix: str,
    aws_conn_id: str = "minio",
) -> list[str]:
    """
    Return the immediate subfolders under a given prefix.
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []

    subfolders = sorted(
        {
            key[len(prefix) :].split("/", 1)[0]
            for key in keys
            if key.startswith(prefix) and "/" in key[len(prefix) :]
        }
    )

    return subfolders


def latest_delta_uri(prefix_str: str, table_name: str) -> str:
    hook = S3Hook(aws_conn_id="minio")
    prefix = f"{prefix_str}/{table_name}" if prefix_str else table_name

    keys = hook.list_keys(bucket_name=LAKEHOUSE_BUCKET, prefix=prefix) or []
    for key in keys:
        print(key)

    timestamps = {key.removeprefix(prefix + "/").split("/", 1)[0] for key in keys}
    print(timestamps)
    if not timestamps:
        raise FileNotFoundError(
            f"No reference Delta table found for {table_name} under {prefix}"
        )

    latest_timestamp = max(timestamps)
    print(f"s3a://{LAKEHOUSE_BUCKET}/{prefix}/{latest_timestamp}")
    return f"s3a://{LAKEHOUSE_BUCKET}/{prefix}/{latest_timestamp}"


def latest_snapshot_file_uri(
    prefix_str: str,
    filename_prefix: str,
    bucket_name: str = LAKEHOUSE_BUCKET,
    aws_conn_id: str = "minio",
) -> str:
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix_str) or []
    pattern = re.compile(
        rf"^{re.escape(filename_prefix)}(?P<snapshot_date>\d{{4}}-\d{{2}}-\d{{2}})\.csv$"
    )
    matching_keys = [
        (match.group("snapshot_date"), key)
        for key in keys
        if (match := pattern.match(key.rsplit("/", 1)[-1]))
    ]

    if not matching_keys:
        raise FileNotFoundError(
            f"No snapshot file starting with {filename_prefix} found under s3a://{bucket_name}/{prefix_str}"
        )

    latest_snapshot_date = max(snapshot_date for snapshot_date, _ in matching_keys)
    latest_keys = [
        key
        for snapshot_date, key in matching_keys
        if snapshot_date == latest_snapshot_date
    ]

    return f"s3a://{bucket_name}/{max(latest_keys)}"


def latest_incremental_file_uri(
    prefix_str: str,
    filename: str,
    bucket_name: str = LAKEHOUSE_BUCKET,
    aws_conn_id: str = "minio",
) -> str:
    hook = S3Hook(aws_conn_id=aws_conn_id)

    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix_str) or []
    matching_keys = [key for key in keys if key.endswith(f"/{filename}")]

    if not matching_keys:
        raise FileNotFoundError(
            f"No file named {filename} found under s3a://{bucket_name}/{prefix_str}"
        )

    latest_ingestion_date = max(key.split("/")[-2] for key in matching_keys)

    return f"s3a://{bucket_name}/{prefix_str}/{latest_ingestion_date}/{filename}"
