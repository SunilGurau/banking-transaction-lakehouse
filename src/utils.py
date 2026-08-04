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
