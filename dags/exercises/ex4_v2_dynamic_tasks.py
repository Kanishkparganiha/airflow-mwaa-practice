"""
Exercise 4 v2 — Dynamic task generation (parse-time loop)
Topic: creating N tasks from a list at DAG parse time
Reference: ex4_solution.py

SCENARIO: Run data quality checks for each table in a DoorDash-style schema.
The list of tables is known at write time (fixed), so a loop at parse time is fine.
(Compare with ex7 where the list comes from runtime data.)

TABLES = ["orders", "dashers", "consumers", "merchants", "deliveries"]

PIPELINE:
  For each table in TABLES (in parallel):
      check_row_count_{table}    ← fails if count == 0
      check_nulls_{table}        ← fails if null % > 5%
  All checks done →
      send_dq_report             ← summarises pass/fail per table

REQUIREMENTS:
  - Use a loop to generate tasks — do NOT write 10 task definitions by hand
  - check_row_count and check_nulls for same table must run in sequence
  - All table groups run in parallel
  - send_dq_report runs after every check task finishes
  - Schedule: @daily, catchup=False

HINT: collect all final tasks in a list, then list >> send_dq_report
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

TABLES = ["orders", "dashers", "consumers", "merchants", "deliveries"]

def check_row_count(table, **kwargs):
    # TODO: simulate — print and pretend it passes
    ...

def check_nulls(table, **kwargs):
    # TODO: simulate — print and pretend it passes
    ...

def send_dq_report(**kwargs):
    # TODO: print a summary
    ...

# TODO: build the DAG with the loop
