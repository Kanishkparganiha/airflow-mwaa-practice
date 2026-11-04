"""
Exercise 7 — Dynamic Task Mapping (.expand)
============================================
Airflow 2.3+ feature. Lets you create N task instances at runtime
based on data — you don't know N at DAG parse time.

DoorDash equivalent: run one Glue job per merchant_category, but
the list of categories comes from a database query at runtime.

YOUR TASK:
Build a pipeline:
  1. get_categories() → returns ["food", "grocery", "alcohol", "pets"]
  2. process_category(category) — runs once PER item in that list (dynamic fan-out)
  3. summarise() — runs after ALL dynamic instances finish

Rules:
  - Use @task and .expand() — NOT a Python loop like ex4
  - process_category must receive each category as a plain argument
  - The number of process_category instances is determined at runtime

Key difference from ex4 (loop):
  - ex4 creates tasks at PARSE time (fixed N in code)
  - .expand() creates instances at RUN time (N from upstream data)

Hint:
  categories = get_categories()
  process_category.expand(category=categories)

Bonus: add .expand_kwargs() to pass both category AND a date:
  process_category.expand_kwargs([
      {"category": c, "ds": "{{ ds }}"} for c in categories
  ])
  — but note: expand_kwargs takes a list, not an XCom ref directly.
    For a pure runtime list, chain it through a @task that returns the list of dicts.
"""

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="ex7_dynamic_mapping",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
)
def dynamic_mapping_pipeline():

    @task
    def get_categories() -> list[str]:
        # TODO: return the list (in prod this would query a DB)
        ...

    @task
    def process_category(category: str) -> str:
        # TODO: print/process and return a result string
        ...

    @task
    def summarise(results: list[str]):
        # TODO: print all results
        ...

    # TODO: wire up with .expand() so process_category runs once per category
    # TODO: pass all results to summarise


dynamic_mapping_pipeline()
