"""
Exercise 2 v2 — XCom + Branching
Topic: BranchPythonOperator, XCom push/pull, trigger_rule
Reference: ex2_solution.py

SCENARIO: Decide whether to run a full recompute or an incremental update
based on how many new orders arrived since the last run.

PIPELINE:
  count_new_orders
       ↓
  decide_strategy    ← BranchPythonOperator
     /        \
full_recompute  incremental_update
     \        /
    update_dashboard   ← must run after EITHER branch

REQUIREMENTS:
  - count_new_orders returns an integer via XCom (simulate: return 150_000)
  - decide_strategy pulls that count:
      count > 100_000  → "full_recompute"
      else             → "incremental_update"
  - update_dashboard needs the right trigger_rule to run after either branch
  - Use classic API (PythonOperator + BranchPythonOperator), NOT TaskFlow

BONUS: make count_new_orders randomly return a value between 50k and 200k
       so you can trigger the DAG multiple times and see both branches fire.
       import random; return random.randint(50_000, 200_000)
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# TODO: implement callables

# TODO: build the DAG
