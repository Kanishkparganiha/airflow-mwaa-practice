from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "kanishk", "retries": 1}

def count_rows(**kwargs):
    return 75_000   # auto-pushed to XCom as return_value

def check_threshold(**kwargs):
    count = kwargs["ti"].xcom_pull(task_ids="count_rows")
    return "process_full" if count > 50_000 else "process_sample"

def process_full(**kwargs):
    print("Running full load")

def process_sample(**kwargs):
    print("Running sample load")

with DAG(
    dag_id="xcom_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
) as dag:

    t_count  = PythonOperator(task_id="count_rows",  python_callable=count_rows)

    t_branch = BranchPythonOperator(task_id="check_threshold", python_callable=check_threshold)

    t_full   = PythonOperator(task_id="process_full",   python_callable=process_full)
    t_sample = PythonOperator(task_id="process_sample", python_callable=process_sample)

    # none_failed_min_one_success — runs after either branch, not both
    t_done   = EmptyOperator(
        task_id="complete",
        trigger_rule="none_failed_min_one_success",
    )

    t_count >> t_branch >> [t_full, t_sample] >> t_done
