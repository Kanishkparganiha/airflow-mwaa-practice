"""
Exercise 9 — TriggerDagRunOperator + ExternalTaskSensor
========================================================
At scale (DoorDash, MWAA) you split large pipelines across multiple DAGs
and chain them. Two patterns:

  TriggerDagRunOperator  — DAG A actively fires DAG B
  ExternalTaskSensor     — DAG B waits until a task in DAG A is done

YOUR TASK — build TWO DAGs:

--- DAG 1: upstream_pipeline ---
  extract → transform → [TriggerDagRunOperator → triggers downstream_pipeline]

--- DAG 2: downstream_pipeline ---
  [ExternalTaskSensor waits for upstream_pipeline.transform to be SUCCESS]
  → aggregate → publish

Rules:
  - upstream runs on schedule="@daily"
  - downstream has schedule=None (only runs when triggered)
  - TriggerDagRunOperator should pass { "run_date": "{{ ds }}" } as conf to downstream
  - ExternalTaskSensor: external_dag_id="upstream_pipeline", external_task_id="transform"
  - In downstream, read conf["run_date"] inside aggregate() via kwargs["dag_run"].conf

Interview question this prepares:
  "How do you handle dependencies between DAGs?"
  Answer: TriggerDagRunOperator for fire-and-forget, ExternalTaskSensor for
  blocking wait. Use sensor when downstream needs upstream state to be confirmed
  in the metadata DB (not just triggered).

Hint:
  from airflow.operators.trigger_dagrun import TriggerDagRunOperator
  from airflow.sensors.external_task import ExternalTaskSensor
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor


# ── DAG 1: upstream ───────────────────────────────────────────────────────────

def extract(**kwargs): print("upstream: extract")
def transform(**kwargs): print("upstream: transform")

# TODO: build upstream_pipeline DAG
#   extract → transform → TriggerDagRunOperator(trigger_dag_id="downstream_pipeline")


# ── DAG 2: downstream ─────────────────────────────────────────────────────────

def aggregate(**kwargs):
    # TODO: read and print kwargs["dag_run"].conf.get("run_date")
    ...

def publish(**kwargs): print("downstream: publish")

# TODO: build downstream_pipeline DAG (schedule=None)
#   ExternalTaskSensor(external_dag_id="upstream_pipeline", external_task_id="transform")
#   → aggregate → publish
