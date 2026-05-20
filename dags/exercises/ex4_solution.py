from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# On MWAA, swap PythonOperator for GlueJobOperator:
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {"owner": "kanishk", "retries": 2, "retry_delay": timedelta(minutes=5)}

REGIONS = ["na", "eu", "fe"]

def run_glue_job(region, ds, **kwargs):
    # Simulates GlueJobOperator locally
    print(f"Running asin-processing-{region} for date {ds}")

def aggregate(**kwargs):
    print("Aggregating results from all regions")

with DAG(
    dag_id="regional_glue_pipeline",
    default_args=default_args,
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exercise"],
) as dag:

    regional_tasks = [
        PythonOperator(
            task_id=f"process_{region}",
            python_callable=run_glue_job,
            op_kwargs={"region": region},
            # On MWAA:
            # GlueJobOperator(
            #     job_name=f"asin-processing-{region}",
            #     script_args={"--date": "{{ ds }}", "--region": region},
            # )
        )
        for region in REGIONS
    ]

    t_aggregate = PythonOperator(task_id="aggregate", python_callable=aggregate)

    regional_tasks >> t_aggregate   # all 3 run in parallel, then aggregate
