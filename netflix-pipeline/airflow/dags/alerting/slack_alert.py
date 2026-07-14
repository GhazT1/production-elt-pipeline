"""
alerting/slack_alert.py
=======================
Slack alerting for Airflow DAG success and failure events.

Uses the Airflow 2.x provider path (airflow.providers.slack).
Connection credentials are stored in Airflow Connections (id: slack_default),
not hardcoded anywhere.
"""

import logging
import os

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

logger = logging.getLogger(__name__)

SLACK_CONN_ID = os.environ.get("SLACK_CONN_ID", "slack_default")
ENV = os.environ.get("PIPELINE_ENV", "unknown")

_ENV_EMOJI = {
    "prod": ":rotating_light:",
    "staging": ":large_yellow_circle:",
    "dev": ":white_circle:",
}


def _env_emoji() -> str:
    return _ENV_EMOJI.get(ENV, ":white_circle:")


# ---------------------------------------------------------------------------
# Failure alert — used as on_failure_callback
# ---------------------------------------------------------------------------
def build_failure_alert(context: dict) -> SlackWebhookOperator:
    """
    Build and return a SlackWebhookOperator for a task failure.

    This is a factory, not an executor — it returns the operator so the
    caller can chain it with other callbacks (e.g. SNS).

    Args:
        context: Airflow task context dict injected by on_failure_callback.

    Returns:
        SlackWebhookOperator ready to be .execute(context=context)'d.
    """
    ti = context["task_instance"]

    try:
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except Exception as exc:
        logger.error("Could not retrieve Slack connection '%s': %s", SLACK_CONN_ID, exc)
        raise

    message = (
        f":red_circle: *Pipeline Failure* {_env_emoji()}\n"
        f">*DAG:* `{ti.dag_id}`\n"
        f">*Task:* `{ti.task_id}`\n"
        f">*Env:* `{ENV}`\n"
        f">*Run:* `{context['execution_date'].isoformat()}`\n"
        f">*Log:* <{ti.log_url}|View logs>"
    )

    return SlackWebhookOperator(
        task_id="slack_failure_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_token,
        message=message,
        on_failure_callback=None,   # prevent recursive alerts
    )


# ---------------------------------------------------------------------------
# Success alert — used as a DAG task (not a callback)
# ---------------------------------------------------------------------------
def build_success_alert(dag: DAG) -> SlackWebhookOperator:
    """
    Build a SlackWebhookOperator DAG task for pipeline success.

    This is wired into the DAG task graph (not a callback), so it fires
    only after all upstream tasks succeed.

    Args:
        dag: The DAG instance to attach this task to.

    Returns:
        SlackWebhookOperator as a DAG task node.
    """
    try:
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except Exception as exc:
        logger.error("Could not retrieve Slack connection '%s': %s", SLACK_CONN_ID, exc)
        raise

    message = (
        ":large_green_circle: *Pipeline Success*\n"
        ">*DAG:* `{{ task_instance.dag_id }}`\n"
        ">*Env:* `" + ENV + "`\n"
        ">*Completed:* `{{ execution_date }}`\n"
        ">All dbt tests passed. Data is ready."
    )

    return SlackWebhookOperator(
        task_id="slack_success_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_token,
        message=message,
        on_failure_callback=None,
        dag=dag,
    )
