from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from pipeline_tasks import refresh_snowflake_raw, run_dbt_build

default_args = {
    "owner": "yousef",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_end_to_end_pipeline",
    default_args=default_args,
    description="Healthcare IoT Lakehouse: S3, Glue/PySpark, Snowflake, dbt",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["healthcare", "s3", "snowflake", "dbt"],
) as dag:

    task_generate_batch = BashOperator(
        task_id="generate_batch_data",
        bash_command="python3 /opt/airflow/scripts/generate_batch_data_v2.py",
    )

    task_upload_batch = BashOperator(
        task_id="upload_batch_to_s3",
        bash_command="python3 /opt/airflow/scripts/upload_batch_to_s3.py",
        env={
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_default.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_default.password }}",
            "AWS_DEFAULT_REGION": "{{ conn.aws_default.extra_dejson.get('region_name', 'us-east-1') }}",
        },
    )

    task_wait_streaming = S3KeySensor(
        task_id="wait_for_streaming_data_in_s3",
        bucket_name="healthcare-iot-lake",
        bucket_key="raw/streaming/vital-readings/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
        mode="reschedule",
    )

    # 4. Triggers AWS Glue Job and polls until Succeeded
    task_process_layers = BashOperator(
        task_id="process_silver_and_gold_layers",
        bash_command="python3 /opt/airflow/scripts/trigger_glue_job.py",
        env={
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_default.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_default.password }}",
            "AWS_DEFAULT_REGION": "{{ conn.aws_default.extra_dejson.get('region_name', 'us-east-1') }}",
            "GLUE_JOB_NAME": "healthcare_iot_bronze_to_gold",
        },
    )

    task_load_snowflake = PythonOperator(
        task_id="refresh_snowflake_raw",
        python_callable=refresh_snowflake_raw,
        op_kwargs={"snowflake_conn_id": "snowflake_default"},
    )

    task_run_dbt = PythonOperator(
        task_id="run_dbt_transformations",
        python_callable=run_dbt_build,
        op_kwargs={
            "snowflake_conn_id": "snowflake_default",
            "project_dir": "/opt/airflow/iomt_clinical_warehouse",
        },
    )

    task_bi_ready = BashOperator(
        task_id="notify_bi_dashboard",
        bash_command="echo 'Lakehouse refresh complete. BI reporting layer ready.'",
    )

    task_generate_batch >> task_upload_batch
    [task_upload_batch, task_wait_streaming] >> task_process_layers >> task_load_snowflake >> task_run_dbt >> task_bi_ready
