from datetime import datetime, timedelta


def partition_for(data_interval_end):
    """Returns the date of the previous calendar day as YYYY-MM-DD."""
    if data_interval_end is None:
        raise ValueError("data_interval_end is required")
    return (data_interval_end - timedelta(days=1)).strftime("%Y-%m-%d")


def find_partition_prefix(bucket, base_prefix, snapshot_date, keys):
    datetime.strptime(snapshot_date, "%Y-%m-%d")
    prefix = f"{base_prefix}/dataflow_date={snapshot_date}/"
    data_keys = [key for key in keys if key.startswith(prefix) and key.endswith(".orc")]
    if not data_keys:
        raise FileNotFoundError(f"No ORC files for partition {snapshot_date}")
    return f"s3a://{bucket}/{prefix}"
