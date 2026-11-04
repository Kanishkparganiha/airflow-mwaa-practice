"""
Exercise 5 v2 — TaskFlow API
Topic: @dag, @task, implicit XCom, @task.branch
Reference: ex5_solution.py

SCENARIO: End-of-week merchant scorecard pipeline using the modern TaskFlow API.

PIPELINE:
  fetch_orders(ds) → dict with keys: gmv, order_count, cancellation_rate
       ↓
  classify_merchant(metrics) → @task.branch
      cancellation_rate > 0.15  → "flag_for_review"
      else                      → "generate_scorecard"
       ↓              ↓
  flag_for_review   generate_scorecard
       \                /
         notify_merchant   ← EmptyOperator, trigger_rule=none_failed_min_one_success

REQUIREMENTS:
  - Use @dag and @task decorators throughout
  - fetch_orders must accept ds as a parameter (Airflow injects it automatically)
  - classify_merchant receives the dict returned by fetch_orders (implicit XCom)
  - flag_for_review prints: "FLAGGED: cancellation rate {rate}"
  - generate_scorecard prints: "Scorecard: GMV={gmv}, Orders={count}"
  - Schedule: "0 9 * * 1"  (9am every Monday)

BONUS: make fetch_orders return random data so both branches get triggered
       across multiple manual runs.
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id="ex5_v2_merchant_scorecard",
    schedule="0 9 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
)
def merchant_scorecard():

    @task
    def fetch_orders(ds: str) -> dict:
        # TODO: return a dict with gmv, order_count, cancellation_rate
        ...

    @task.branch
    def classify_merchant(metrics: dict) -> str:
        # TODO: return task_id string based on cancellation_rate
        ...

    @task
    def flag_for_review(metrics: dict):
        # TODO
        ...

    @task
    def generate_scorecard(metrics: dict):
        # TODO
        ...

    notify = EmptyOperator(
        task_id="notify_merchant",
        trigger_rule="none_failed_min_one_success",
    )

    # TODO: wire up the pipeline

merchant_scorecard()
