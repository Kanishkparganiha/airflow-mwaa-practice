"""
Exercise 6 — TaskGroups
=======================
Organise a multi-step pipeline into logical groups so the Airflow UI
shows a collapsed view instead of a flat mess of tasks.

YOUR TASK:
Build a DAG that processes orders for 3 regions: na, eu, fe.
Each region needs 3 steps: validate → transform → load.
Without TaskGroups that's 9 loose tasks. Wrap each region's
3 tasks in a TaskGroup so the UI shows 3 groups instead.

Expected graph:
  [na: validate → transform → load]
       ↓
  [eu: validate → transform → load]  (all 3 run in parallel)
       ↓
  [fe: validate → transform → load]
       ↓
  send_report

Actually make na/eu/fe run in PARALLEL, then send_report after all 3 finish.

Hint: from airflow.utils.task_group import TaskGroup
      with TaskGroup(group_id="na") as tg_na:
          ...
      [tg_na, tg_eu, tg_fe] >> send_report

Bonus: use a loop to create the 3 groups dynamically (don't repeat code 3 times).
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

REGIONS = ["na", "eu", "fe"]

# TODO: implement these callables
def validate(region, **kwargs): ...
def transform(region, **kwargs): ...
def load(region, **kwargs): ...
def send_report(**kwargs): ...


with DAG(
    dag_id="ex6_task_groups",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise"],
) as dag:

    # TODO: create a TaskGroup per region with validate → transform → load
    # TODO: run all 3 groups in parallel
    # TODO: send_report after all groups finish
    pass
