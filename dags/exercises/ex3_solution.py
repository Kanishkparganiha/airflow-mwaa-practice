from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor  # swap for S3KeySensor on MWAA

# On MWAA:
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# sensor = S3KeySensor(
#     bucket_name="my-bucket",
#     bucket_key="input/{{ ds_nodash }}/data.parquet",
#     ...
# )

default_args = {"owner": "kanishk", "retries": 1}

def process(**kwargs):
    print(f"Processing data for {kwargs['ds']}")

with DAG(
    dag_id="sensor_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
) as dag:

    # Local filesystem sensor for local dev — same API as S3KeySensor
    wait = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/airflow-practice/{{ ds_nodash }}/data.parquet",
        mode="reschedule",        # releases worker slot between pokes
        poke_interval=120,        # 2 minutes
        timeout=4 * 3600,         # 4 hours
        soft_fail=True,           # SKIPPED on timeout instead of FAILED
    )

    process_task = PythonOperator(
        task_id="process",
        python_callable=process,
        trigger_rule="none_failed_min_one_success",  # runs even if sensor was soft_fail skipped? No —
        # Actually: soft_fail marks sensor SKIPPED, so process won't run with default all_success.
        # To run after skip too, use trigger_rule="all_done" — depends on your requirement.
    )

    wait >> process_task
