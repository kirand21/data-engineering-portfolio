from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from price_load.partitions import find_partition_prefix, partition_for

S3_CONN_ID = "s3_snapshots"
PG_CONN_ID = "pg_target"
BASE_PREFIX = "warehouse/prices/current_price"
JOB_PATH = "/opt/airflow/jobs/price_job.py"
PACKAGE_ZIP = "/opt/airflow/jobs/price_load.zip"  # built by scripts/build_package.py

with DAG(
    dag_id="load_current_prices",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Moscow"),
    schedule="20 7 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
) as dag:

    @task
    def resolve_partition(data_interval_end=None):
        """Finds the snapshot of the previous calendar day and checks that it has ORC files."""
        from airflow.models import Variable

        bucket = Variable.get("snapshot_bucket")
        snapshot_date = partition_for(data_interval_end)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        keys = hook.list_keys(
            bucket_name=bucket,
            prefix=f"{BASE_PREFIX}/dataflow_date={snapshot_date}/",
        ) or []
        return {
            "path": find_partition_prefix(bucket, BASE_PREFIX, snapshot_date, keys),
            "date": snapshot_date,
        }

    snapshot = resolve_partition()

    # Makes reruns safe: remove rows of this snapshot date before inserting them again.
    clear_snapshot_date = PostgresOperator(
        task_id="clear_snapshot_date",
        postgres_conn_id=PG_CONN_ID,
        sql="DELETE FROM ref.current_prices "
            "WHERE load_date = '{{ ti.xcom_pull(task_ids='resolve_partition')['date'] }}'",
    )

    load_prices = SparkSubmitOperator(
        task_id="load_prices",
        conn_id="spark_default",
        application=JOB_PATH,
        py_files=PACKAGE_ZIP,
        application_args=[
            "--source-path", snapshot["path"],
            "--snapshot-date", snapshot["date"],
            "--target-table", "ref.current_prices",
        ],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "{{ conn.s3_snapshots.extra_dejson.endpoint_url }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider",
            "spark.executorEnv.AWS_ACCESS_KEY_ID": "{{ conn.s3_snapshots.login }}",
            "spark.executorEnv.AWS_SECRET_ACCESS_KEY": "{{ conn.s3_snapshots.password }}",
        },
        # Credentials stay in Airflow Connections and are rendered at run time.
        env_vars={
            "AWS_ACCESS_KEY_ID": "{{ conn.s3_snapshots.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.s3_snapshots.password }}",
            "PG_HOST": "{{ conn.pg_target.host }}",
            "PG_PORT": "{{ conn.pg_target.port }}",
            "PG_DB": "{{ conn.pg_target.schema }}",
            "PG_USER": "{{ conn.pg_target.login }}",
            "PG_PASSWORD": "{{ conn.pg_target.password }}",
        },
    )

    snapshot >> clear_snapshot_date >> load_prices
