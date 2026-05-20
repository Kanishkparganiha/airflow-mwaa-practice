from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "kanishk",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def extract(ds, **kwargs):
    print(f"Extracting for {ds}")

def validate(**kwargs):
    row_count = 50_000
    print(f"Validated {row_count} rows")

def transform(**kwargs):
    print("Transforming")

def load(**kwargs):
    print("Loading to Redshift")

with DAG(
    dag_id="asin_daily_pipeline",
    default_args=default_args,
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exercise"],
) as dag:

    t_extract   = PythonOperator(task_id="extract",   python_callable=extract)
    t_validate  = PythonOperator(task_id="validate",  python_callable=validate)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load      = PythonOperator(task_id="load",      python_callable=load)

    t_extract >> t_validate >> t_transform >> t_load
