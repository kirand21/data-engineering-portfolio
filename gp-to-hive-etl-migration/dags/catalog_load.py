from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

SPARK_CONN_ID = "spark_default"
PG_CONN_ID = "pg_buffer"
JOB_PATH = "/opt/airflow/jobs/catalog_job.py"
PACKAGE_ZIP = "/opt/airflow/jobs/catalog_etl.zip"  # built by scripts/build_package.py

with DAG(
    dag_id="update_catalog_items",
    start_date=datetime(2026, 1, 1),
    schedule="20 7 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    # Makes reruns safe: remove rows of this run date before inserting them again.
    clear_run_date = PostgresOperator(
        task_id="clear_run_date",
        postgres_conn_id=PG_CONN_ID,
        sql="DELETE FROM buf.catalog_items WHERE load_date = %(run_date)s",
        parameters={"run_date": "{{ ds }}"},
    )

    load_items = SparkSubmitOperator(
        task_id="load_items",
        conn_id=SPARK_CONN_ID,
        application=JOB_PATH,
        py_files=PACKAGE_ZIP,
        application_args=["--source-table", "catalog.dim_item",
                          "--target-table", "buf.catalog_items",
                          "--run-date", "{{ ds }}"],
        # Credentials stay in the Airflow Connection and are rendered at run time.
        env_vars={"PG_HOST": "{{ conn.pg_buffer.host }}",
                  "PG_PORT": "{{ conn.pg_buffer.port }}",
                  "PG_DB": "{{ conn.pg_buffer.schema }}",
                  "PG_USER": "{{ conn.pg_buffer.login }}",
                  "PG_PASSWORD": "{{ conn.pg_buffer.password }}"},
    )

    run_downstream = TriggerDagRunOperator(
        task_id="run_downstream",
        trigger_dag_id="build_catalog_marts",
        wait_for_completion=False,
    )

    clear_run_date >> load_items >> run_downstream
