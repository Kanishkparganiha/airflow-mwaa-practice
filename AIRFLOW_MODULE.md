# Airflow / MWAA — L5 DE Interview Module

You've used MWAA in production on A2H, UIL, LML. This module sharpens the concepts interviewers probe,
fills gaps between "I've used it" and "I can explain every decision", and gives you code exercises to write.

---

## Setup — Docker (no local install)

Airflow runs in Docker. Your `dags/` folder is mounted into the container — edit DAGs locally, they appear in the UI instantly.

Uses your existing `adventureworks` postgres:16 container — no separate Postgres needed.

**Start:**
```bash
cd /Users/kanishk/Downloads/WorkingDir/airflow-practice

# 1. Start your existing Postgres
docker start adventureworks

# 2. Create airflow DB (one-time only)
docker exec adventureworks psql -U postgres -c "CREATE DATABASE airflow;"

# 3. Init Airflow schema + admin user (one-time only)
docker compose run --rm airflow-init

# 4. Start webserver + scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

UI: http://localhost:8085 — login: `admin` / `admin`

**Stop:**
```bash
docker compose down        # stops Airflow containers, adventureworks keeps running
```

**Memory (Mac mini 16 GB):**
| Container          | Cap   |
|--------------------|-------|
| adventureworks     | (existing, shared) |
| airflow-webserver  | 512m  |
| airflow-scheduler  | 512m  |
| **Total new**      | **~1 GB** |

**Writing DAGs:**
- Edit files in `dags/` locally — mounted at `/opt/airflow/dags` inside the container
- Scheduler picks up changes within ~30s (default `dag_dir_list_interval`)
- Notebooks are for reading/understanding code only — no need to run pip install cells

---

## Module 1 — DAG Anatomy

### The skeleton every DAG has

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "kanishk",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5m, 10m, 20m...
    "email_on_failure": True,
    "email": ["kanishkparganiha@gmail.com"],
}

with DAG(
    dag_id="example_dag",
    default_args=default_args,
    schedule="0 6 * * *",       # 6am UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,               # ALWAYS False in prod unless backfill needed
    max_active_runs=1,           # prevents overlapping runs — critical for MWAA
    tags=["etl", "asin"],
) as dag:
    pass
```

### What interviewers ask about default_args

**Q: What happens if you set retries=3 on default_args AND retries=1 on an individual task?**
→ Task-level config wins. default_args are just defaults.

**Q: catchup=True and you set start_date 30 days ago — what happens?**
→ Airflow creates 30 backfill DAG runs immediately on first activation. Usually bad. Always catchup=False unless you explicitly want backfill.

**Q: max_active_runs=1 vs depends_on_past=True — difference?**
→ `max_active_runs=1`: only 1 DAG run at a time (globally). `depends_on_past=True`: a TASK won't run unless the same task in the previous DAG run succeeded. Use max_active_runs=1 for stateful pipelines, depends_on_past for incremental logic.

---

## Module 2 — Operators

### PythonOperator

```python
from airflow.operators.python import PythonOperator

def process_data(ds, **kwargs):         # ds = execution date as string "2024-01-15"
    print(f"Processing for date: {ds}")
    # ds_nodash = "20240115"
    # kwargs["execution_date"] = pendulum datetime object
    return "done"

task = PythonOperator(
    task_id="process",
    python_callable=process_data,
    op_kwargs={"extra_param": "value"},  # passed to function
)
```

### BranchPythonOperator

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def choose_branch(ds, **kwargs):
    # Return the task_id of the branch to take
    if some_condition:
        return "process_full"
    return "process_incremental"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_branch,
)

full = EmptyOperator(task_id="process_full")
incremental = EmptyOperator(task_id="process_incremental")
join = EmptyOperator(
    task_id="join",
    trigger_rule="none_failed_min_one_success",  # KEY — explained below
)

branch >> [full, incremental] >> join
```

### Trigger Rules — the thing people get wrong

| Rule | When task runs |
|---|---|
| `all_success` (default) | All upstream tasks succeeded |
| `all_failed` | All upstream failed |
| `all_done` | All upstream finished (any state) |
| `one_failed` | At least one upstream failed |
| `one_success` | At least one upstream succeeded |
| `none_failed` | No upstream failed (success or skipped) |
| `none_failed_min_one_success` | No failures, at least one success |

**Branch + join pattern:** The join task after a branch MUST use `none_failed_min_one_success`
otherwise it waits for both branches and fails because one is always skipped.

### AWS Operators (what you use on MWAA)

```python
# Glue
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

glue_task = GlueJobOperator(
    task_id="run_glue_job",
    job_name="my-glue-job",
    script_args={"--ds": "{{ ds }}", "--env": "prod"},
    aws_conn_id="aws_default",
    wait_for_completion=True,
)

# Batch
from airflow.providers.amazon.aws.operators.batch import BatchOperator

batch_task = BatchOperator(
    task_id="run_batch_job",
    job_name="feature-extraction",
    job_definition="my-job-def:3",
    job_queue="ml-queue",
    overrides={
        "command": ["python", "main.py", "--date", "{{ ds }}"],
        "environment": [{"name": "ENV", "value": "prod"}],
    },
    aws_conn_id="aws_default",
)

# S3 Sensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_file = S3KeySensor(
    task_id="wait_for_input",
    bucket_name="my-bucket",
    bucket_key="input/{{ ds_nodash }}/data.parquet",
    aws_conn_id="aws_default",
    poke_interval=60,    # check every 60s
    timeout=3600,        # fail after 1 hour
    mode="reschedule",   # releases worker slot between pokes (use this, not "poke")
)
```

---

## Module 3 — XCom

XCom = cross-task communication. Stored in Airflow metadata DB. **Small values only.**

### Push / Pull (explicit)

```python
def push_value(**kwargs):
    kwargs["ti"].xcom_push(key="row_count", value=42_000)

def pull_value(**kwargs):
    count = kwargs["ti"].xcom_pull(task_ids="upstream_task", key="row_count")
    print(f"Rows: {count}")
```

### Implicit push (return value)

```python
def compute():
    return {"status": "ok", "rows": 42_000}   # auto-pushed as key="return_value"

def use_result(**kwargs):
    result = kwargs["ti"].xcom_pull(task_ids="compute_task")  # gets return_value
    print(result["rows"])
```

### TaskFlow API (@task decorator — cleaner XCom)

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def my_pipeline():

    @task
    def extract() -> dict:
        return {"s3_path": "s3://bucket/file.parquet", "row_count": 50_000}

    @task
    def transform(data: dict) -> str:
        print(f"Processing {data['row_count']} rows from {data['s3_path']}")
        return "s3://bucket/output.parquet"

    @task
    def load(output_path: str):
        print(f"Loading from {output_path}")

    result = extract()
    output = transform(result)   # XCom wired automatically
    load(output)

my_pipeline()
```

### XCom rules for interviews

1. **Never push DataFrames** — XCom is stored in the metadata DB (SQLite/Postgres). Large payloads crash it.
2. **Pattern for large data:** Push the S3 path as XCom, not the data itself.
3. **XCom across DAGs:** Use `ExternalTaskSensor` + a shared S3 path, not XCom (XCom is DAG-scoped).
4. **MWAA XCom limit:** Practically, keep under 48KB. Use S3 for anything bigger.

---

## Module 4 — Sensors

### S3KeySensor — mode matters

```python
# BAD — mode="poke" (default)
# Holds a worker slot the ENTIRE time it's waiting
# With 60s poke_interval and 1hr timeout = 1 worker blocked for an hour

# GOOD — mode="reschedule"
# Returns the worker slot between checks, rescheduled by scheduler
wait = S3KeySensor(
    task_id="wait_for_upstream",
    bucket_name="my-bucket",
    bucket_key="data/{{ ds }}/manifest.json",
    mode="reschedule",      # always use this for long waits
    poke_interval=120,
    timeout=7200,
    soft_fail=True,         # marks task as SKIPPED instead of FAILED on timeout
)
```

### ExternalTaskSensor — wait for another DAG

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_ingestion_dag",
    external_dag_id="ingestion_pipeline",
    external_task_id="final_task",       # None = wait for the whole DAG
    execution_delta=timedelta(hours=1),  # upstream runs 1hr before this dag
    mode="reschedule",
    timeout=3600,
)
```

---

## Module 5 — Scheduling & Templates

### Cron + Airflow presets

```
@hourly     = "0 * * * *"
@daily      = "0 0 * * *"
@weekly     = "0 0 * * 0"
@monthly    = "0 0 1 * *"
None        = manual trigger only
```

### Jinja templates — the ones you use

```python
# In operator arguments (string fields only, not Python code)
"{{ ds }}"           # execution date: "2024-01-15"
"{{ ds_nodash }}"    # "20240115"
"{{ ts }}"           # ISO timestamp: "2024-01-15T06:00:00+00:00"
"{{ prev_ds }}"      # previous execution date
"{{ next_ds }}"      # next execution date
"{{ dag.dag_id }}"   # DAG id
"{{ run_id }}"       # unique run identifier

# In practice (A2H pattern):
GlueJobOperator(
    job_name="asin-ingestion",
    script_args={"--date": "{{ ds }}", "--env": "{{ var.value.environment }}"},
)
```

### Variables vs Connections

```python
from airflow.models import Variable

# Variables — for config values, stored in metadata DB or Secrets Manager
env = Variable.get("environment", default_var="dev")
config = Variable.get("pipeline_config", deserialize_json=True)

# Connections — for credentials/endpoints
# Set in UI or Secrets Manager, referenced by conn_id in operators
```

---

## Module 6 — Error Handling & Retries

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,  # 2m, 4m, 8m
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=2),  # kills the task if it hangs
    "on_failure_callback": alert_slack,       # called on final failure
    "on_retry_callback": log_retry,           # called on each retry
}

def alert_slack(context):
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    exec_date = context["ds"]
    exception = context.get("exception")
    # send to Slack/PagerDuty

# SLA — different from timeout
# SLA miss = task didn't COMPLETE by this time after DAG start
# Does NOT kill the task — just fires sla_miss_callback
task = PythonOperator(
    task_id="critical_task",
    python_callable=fn,
    sla=timedelta(hours=1),    # alert if not done within 1hr of DAG start
    execution_timeout=timedelta(hours=2),  # actually kill it at 2hr
)
```

**SLA vs execution_timeout — interview answer:**
> "SLA fires an alert if the task isn't done in time but doesn't stop it. execution_timeout actually kills the task. I use execution_timeout to prevent runaway jobs from blocking the queue, and SLA for business-level alerting — e.g., 'this must complete before the 9am report'."

---

## Module 7 — Dynamic DAGs & TaskGroups

### Dynamic tasks (same operator, variable inputs)

```python
from airflow.utils.task_group import TaskGroup

REGIONS = ["us-east-1", "us-west-2", "eu-west-1"]

with dag:
    with TaskGroup("regional_processing") as regional_group:
        for region in REGIONS:
            PythonOperator(
                task_id=f"process_{region.replace('-', '_')}",
                python_callable=process_region,
                op_kwargs={"region": region},
            )

    final = PythonOperator(task_id="aggregate", python_callable=aggregate)
    regional_group >> final
```

### Dynamic task mapping (Airflow 2.3+)

```python
@task
def get_files() -> list[str]:
    return ["file_a.parquet", "file_b.parquet", "file_c.parquet"]

@task
def process_file(filename: str):
    print(f"Processing {filename}")

files = get_files()
process_file.expand(filename=files)   # creates one task per file dynamically
```

---

## Module 8 — MWAA-Specific Patterns

### What's different on MWAA vs local Airflow

| Thing | Local | MWAA |
|---|---|---|
| DAG deployment | Copy files to dags/ folder | Upload to S3, syncs every ~30s |
| Dependencies | pip install locally | requirements.txt in S3 |
| Secrets | airflow.cfg | AWS Secrets Manager (`/airflow/connections/`, `/airflow/variables/`) |
| Connections | UI or env vars | Secrets Manager path convention |
| Scaling | Manual | Managed — set min/max workers |
| Logs | Local files | CloudWatch Logs |

### Secrets Manager convention (MWAA)

```
/airflow/connections/aws_default     → Airflow connection
/airflow/connections/redshift_conn   → Redshift connection
/airflow/variables/environment       → Airflow variable
/airflow/variables/pipeline_config   → JSON config variable
```

### Your A2H pattern (what to say in interview)

> "In A2H we used MWAA with max_active_runs=1 because the pipeline is stateful — if two runs overlap,
> the Glue job writes to the same S3 prefix and corrupts the delta. We migrated from Step Functions
> because MWAA gave us the 'Clear' button — on failures we could rerun from any task without
> retouching upstream, which cut recovery time from 45 minutes to 8 minutes."

---

## Exercises

### Exercise 1 — Basic DAG with dependencies

**Task:** Write a DAG called `asin_daily_pipeline` that:
- Runs daily at 7am UTC
- Has 4 tasks: `extract` → `validate` → `transform` → `load`
- extract: prints "extracting for {{ ds }}"
- validate: prints row count (hardcode 50000)
- transform: prints "transforming"
- load: prints "loading to redshift"
- retries=2, retry_delay=5min on all tasks

→ Solution: `dags/exercises/ex1_solution.py`

---

### Exercise 2 — XCom pipeline

**Task:** Write a DAG called `xcom_pipeline` where:
- `count_rows` returns an integer (hardcode 75000)
- `check_threshold` pulls that count — if > 50000 return "process_full", else "process_sample"
- Use BranchPythonOperator
- Both branches end at a `complete` task
- `complete` must run regardless of which branch was taken

→ Solution: `dags/exercises/ex2_solution.py`

---

### Exercise 3 — Sensor + downstream

**Task:** Write a DAG called `sensor_pipeline` that:
- Waits for `s3://my-bucket/input/{{ ds_nodash }}/data.parquet` to exist
- On arrival, runs a `process` task
- Sensor uses reschedule mode, pokes every 2 minutes, times out after 4 hours
- On sensor timeout, mark as skipped (not failed)

→ Solution: `dags/exercises/ex3_solution.py`

---

### Exercise 4 — Dynamic regional DAG

**Task:** Write a DAG called `regional_glue_pipeline` that:
- Runs a GlueJobOperator for each of: ["na", "eu", "fe"]
- Job name: `asin-processing-{region}`
- Script args: --date {{ ds }}, --region {region}
- All 3 run in parallel
- After all complete, run a single `aggregate` PythonOperator

→ Solution: `dags/exercises/ex4_solution.py`

---

### Exercise 5 — TaskFlow API (write the whole thing)

**Task:** Rewrite Exercise 2 using the @dag and @task decorators.
- Same logic: count_rows → check_threshold → process_full or process_sample → complete
- No BranchPythonOperator — use @task_group or just @task with conditional return
- XCom must be implicit (no xcom_push/pull calls)

→ Solution: `dags/exercises/ex5_solution.py`

---

## Interview Questions — Answer These Yourself First

1. You have a DAG that runs hourly. One run is stuck at a Glue task for 3 hours. What happens to subsequent runs?

2. A task needs data from a previous day's DAG run. How do you get it?

3. Your MWAA pipeline processes S3 files. Files sometimes arrive 2 hours late. How do you handle it?

4. A downstream task should run whether the upstream succeeded OR failed (e.g., cleanup). What trigger_rule?

5. You want to pass a 500MB DataFrame between tasks. How do you do it?

6. What's the difference between clearing a task and marking it as success?

7. You have 10 regions to process. How do you avoid writing 10 identical task definitions?

8. Your pipeline's SLA is 8am but it's running at 8:30am. How do you investigate in MWAA?
