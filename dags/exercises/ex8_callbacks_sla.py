"""
Exercise 8 — on_failure_callback + SLA miss
============================================
Production DAGs need to alert when things go wrong.
Two separate mechanisms:

  on_failure_callback  — fires when a TASK fails after all retries
  sla_miss_callback    — fires when a DAG takes longer than expected

DoorDash equivalent: MWAA DAGs alert to Slack/PagerDuty on failure.
In interviews: "how did you handle failures in your MWAA pipelines?"

YOUR TASK:
1. Write an on_failure_callback function that prints:
      ALERT: task <task_id> failed on DAG <dag_id> at <execution_date>
   (in prod you'd call a Slack webhook or SNS here)

2. Wire it to default_args so ALL tasks in the DAG inherit it.
   Also add: retries=2, retry_exponential_backoff=True, retry_delay=timedelta(seconds=5)

3. Write an sla_miss_callback that prints:
      SLA MISSED: <dag_id> run <run_id> exceeded its SLA
   Wire it to the DAG with sla=timedelta(minutes=10)

4. Add a task called "flaky_api_call" that:
   - Raises an Exception 70% of the time (use random.random())
   - Succeeds 30% of the time
   - Watch it retry in the Airflow UI

Hint: context dict in callbacks has keys:
  context["task_instance"].task_id
  context["task_instance"].dag_id
  context["execution_date"]
  context["run_id"]
"""

import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# TODO: implement on_failure_callback(context)
def on_failure_callback(context):
    ...

# TODO: implement sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    ...

# TODO: implement flaky_api_call — fails 70% of the time
def flaky_api_call(**kwargs):
    ...

# TODO: build the DAG wiring everything together
