from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# TaskFlow API — XCom is implicit, no xcom_push/pull needed
# Return values are automatically pushed; function args are automatically pulled

@dag(
    dag_id="xcom_pipeline_taskflow",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
)
def xcom_pipeline():

    @task
    def count_rows() -> int:
        return 75_000   # automatically pushed to XCom

    @task.branch          # branch version of @task
    def check_threshold(count: int) -> str:
        return "process_full" if count > 50_000 else "process_sample"

    @task
    def process_full():
        print("Running full load")

    @task
    def process_sample():
        print("Running sample load")

    # EmptyOperator still needed for the join — no @task equivalent with trigger_rule
    complete = EmptyOperator(
        task_id="complete",
        trigger_rule="none_failed_min_one_success",
    )

    count = count_rows()
    branch = check_threshold(count)

    full   = process_full()
    sample = process_sample()

    [full, sample] >> complete

    # Wire branch to both options
    branch >> [full, sample]

xcom_pipeline()
