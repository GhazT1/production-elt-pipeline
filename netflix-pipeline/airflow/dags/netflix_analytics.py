"""
Netflix Data Analytics Pipeline
================================
Orchestrates: S3 file detection → Snowflake raw load → dbt transforms → alerting

Architecture:
    S3 (raw CSVs) → Airflow S3KeySensor → Snowflake (DBT_RAW)
                 → dbt stage models → dbt fact/dimension models
                 → dbt tests → Slack success alert

Failure path:
    Any task failure → on_failure_callback → Slack + SNS (parallel)
"""

import logging
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from alerting.slack_alert import build_failure_alert, build_success_alert
from source_load.data_load import run_snowflake_load

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment-driven configuration — no values are hardcoded
# ---------------------------------------------------------------------------
ENV = os.environ["PIPELINE_ENV"]  # "dev" | "staging" | "prod"
DBT_PROJECT_DIR = os.environ["DBT_PROJECT_DIR"]
DBT_PROFILES_DIR = os.environ["DBT_PROFILES_DIR"]
DBT_VENV = os.environ.get("DBT_VENV_BIN", "/home/airflow/dbt-env/bin")
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_FAILURE_TOPIC_ARN"]

DBT_CMD = f"{DBT_VENV}/dbt"
DBT_BASE = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target {ENV}"


def notify_sns_failure(context: dict) -> None:
    """
    SNS failure callback — fires on any task failure.
    Sends structured metadata so downstream subscribers can parse it.
    """
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context["execution_date"]

    message = (
        f"PIPELINE FAILURE\n"
        f"DAG:        {dag_id}\n"
        f"Task:       {task_id}\n"
        f"Env:        {ENV}\n"
        f"Run time:   {execution_date.isoformat()}\n"
        f"Log:        {ti.log_url}\n"
    )
    try:
        sns = boto3.client("sns", region_name=os.environ["AWS_REGION"])
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=f"[{ENV.upper()}] Pipeline failure: {dag_id}")
        logger.info("SNS failure notification sent for task %s", task_id)
    except Exception as exc:
        # Never let alerting crash the task callback chain
        logger.error("SNS notification failed: %s", exc)


def on_failure(context: dict) -> None:
    """Combined failure handler: Slack + SNS in sequence."""
    build_failure_alert(context).execute(context=context)
    notify_sns_failure(context)


# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime.today() - timedelta(days=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="netflix_analytics",
    default_args=default_args,
    description="Netflix dataset ELT: S3 → Snowflake raw → dbt → Slack",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "elt", ENV],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ------------------------------------------------------------------
    # 1. S3 sensors — wait for both source files to arrive
    #    mode='reschedule' frees the worker slot between pokes
    # ------------------------------------------------------------------
    wait_for_credits = S3KeySensor(
        task_id="wait_for_credits_csv",
        bucket_name=S3_BUCKET,
        bucket_key="raw_files/credits.csv",
        wildcard_match=False,
        aws_conn_id="aws_default",
        poke_interval=60 * 5,       # check every 5 minutes
        timeout=60 * 60 * 6,        # fail after 6 hours (SLA)
        mode="reschedule",           # non-blocking: frees worker slot
        soft_fail=False,
    )

    wait_for_titles = S3KeySensor(
        task_id="wait_for_titles_csv",
        bucket_name=S3_BUCKET,
        bucket_key="raw_files/titles.csv",
        wildcard_match=False,
        aws_conn_id="aws_default",
        poke_interval=60 * 5,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=False,
    )

    # ------------------------------------------------------------------
    # 2. Load raw CSVs from S3 into Snowflake DBT_RAW schema
    # ------------------------------------------------------------------
    load_raw = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=run_snowflake_load,
        op_kwargs={"env": ENV},
        execution_timeout=timedelta(minutes=30),
    )

    # ------------------------------------------------------------------
    # 3. dbt staging models (DIMENSION tag)
    # ------------------------------------------------------------------
    run_stage = BashOperator(
        task_id="dbt_run_stage_models",
        bash_command=f"{DBT_CMD} run --select tag:STAGE {DBT_BASE}",
        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------
    # 4. dbt fact & dimension models (FACT tag)
    # ------------------------------------------------------------------
    run_facts = BashOperator(
        task_id="dbt_run_fact_models",
        bash_command=f"{DBT_CMD} run --select tag:FACT {DBT_BASE}",
        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------
    # 5. dbt tests — gate before success notification
    # ------------------------------------------------------------------
    run_tests = BashOperator(
        task_id="dbt_run_tests",
        bash_command=f"{DBT_CMD} test {DBT_BASE} --store-failures",
        execution_timeout=timedelta(minutes=15),
    )

    # ------------------------------------------------------------------
    # 6. Slack success notification
    # ------------------------------------------------------------------
    slack_success = build_success_alert(dag=dag)

    # ------------------------------------------------------------------
    # Task dependency graph
    # ------------------------------------------------------------------
    start >> [wait_for_credits, wait_for_titles] >> load_raw
    load_raw >> run_stage >> run_facts >> run_tests >> slack_success >> end
