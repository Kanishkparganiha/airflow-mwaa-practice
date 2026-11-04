"""
Exercise 3 v2 — Sensor
Topic: FileSensor, poke vs reschedule mode, soft_fail, timeout
Reference: ex3_solution.py

SCENARIO: A upstream team drops a merchant_metrics CSV to a shared folder
every morning. Your DAG must wait for it before running.

PIPELINE:
  wait_for_merchant_file   ← FileSensor
          ↓
  validate_schema          ← checks expected columns exist
          ↓
  compute_rankings         ← ranks merchants by GMV

REQUIREMENTS:
  - FileSensor watches: /tmp/merchant_metrics/{{ ds_nodash }}.csv
  - mode="reschedule"  (do NOT use mode="poke" — explain why in a comment)
  - poke_interval=60 seconds
  - timeout=2 hours, soft_fail=True
  - validate_schema trigger_rule must handle the case where sensor was skipped
  - Schedule: @daily, catchup=False

KEY QUESTION to answer in a comment:
  Why use mode="reschedule" instead of mode="poke"?

BONUS: create the file manually to unblock the sensor:
  mkdir -p /tmp/merchant_metrics
  touch /tmp/merchant_metrics/$(date +%Y%m%d).csv
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# TODO: implement validate_schema and compute_rankings callables

# TODO: build the DAG — pay attention to trigger_rule on the tasks after the sensor
