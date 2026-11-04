"""
Exercise 1 v2 — Linear Pipeline
Topic: DAG anatomy, task chaining, default_args
Reference: ex1_solution.py

SCENARIO: Daily dasher payout pipeline
  1. fetch_completed_deliveries  — pulls delivered orders for {{ ds }}
  2. calculate_earnings          — computes base pay + tips + bonuses
  3. apply_deductions            — deducts equipment rental, violations
  4. write_to_payment_ledger     — writes final amounts (simulated)

REQUIREMENTS:
  - Schedule: 2am daily (dashers get paid overnight)
  - retries=3, retry_delay=10 minutes, retry_exponential_backoff=True
  - catchup=False, max_active_runs=1
  - Each task must print {{ ds }} so you can see which date it ran for
  - Tags: ["payments", "dasher"]

BONUS: add a 5th task notify_dashers that runs only after write_to_payment_ledger
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# TODO: write the 4 callables

# TODO: build the DAG with the correct default_args and chain
