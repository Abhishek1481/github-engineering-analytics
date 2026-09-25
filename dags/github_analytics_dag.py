"""Airflow DAG: GitHub engineering activity analytics.

    extract -> validate -> transform -> load -> quality_check -> analytics_report

Each task calls one function in ``github_analytics.pipeline`` and passes only
a small summary through XCom; data moves through the shared DATA_DIR volume
(raw JSONL, staged Parquet) and PostgreSQL.

Configuration (no secrets in this file):
  * GITHUB_TOKEN, ANALYTICS_DATABASE_URL          environment of the Airflow workers
  * GITHUB_REPOSITORIES / GITHUB_OWNER, ...         environment or config/pipeline.yml
  * GITHUB_ANALYTICS_SCHEDULE                       cron / preset, default "@daily"
  * DAG param ``repositories``                      per-run override from the trigger form
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

SCHEDULE = os.environ.get("GITHUB_ANALYTICS_SCHEDULE", "@daily")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": 2.0,  # Airflow 3.3: multiplier; delay doubles per retry
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}


def _settings() -> Any:
    from github_analytics.utils.config import Settings

    context = get_current_context()
    override = (context["params"].get("repositories") or "").strip()
    if override:
        return Settings.load(repositories=[r.strip() for r in override.split(",") if r.strip()])
    return Settings.load()


def _run_id() -> str:
    from github_analytics.pipeline import safe_run_id

    return str(safe_run_id(get_current_context()["run_id"]))


def _on_failure(context: dict[str, Any]) -> None:
    """Record the failure in etl.pipeline_runs so the run log is complete."""
    from github_analytics.pipeline import mark_failed, safe_run_id
    from github_analytics.utils.config import Settings

    run_id = context.get("run_id") or context["dag_run"].run_id
    reason = context.get("exception") or context.get("reason") or "task failed"
    mark_failed(Settings.load(), safe_run_id(run_id), str(reason))


@dag(
    dag_id="github_engineering_analytics",
    description="GitHub REST API -> PostgreSQL star schema -> engineering metrics",
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # watermarks assume runs do not overlap
    default_args=DEFAULT_ARGS,
    on_failure_callback=_on_failure,
    params={
        "repositories": Param(
            "",
            type="string",
            description="Optional comma-separated owner/repo list overriding configuration",
        ),
    },
    tags=["github", "analytics", "portfolio"],
)
def github_engineering_analytics() -> None:
    @task
    def extract() -> dict[str, Any]:
        from github_analytics import pipeline
        from github_analytics.extract.client import NotFoundError

        try:
            return dict(pipeline.extract(_settings(), _run_id()))
        except NotFoundError as exc:
            # A missing repository will not fix itself on retry.
            raise AirflowFailException(str(exc)) from exc

    @task
    def validate(_extracted: dict[str, Any]) -> dict[str, Any]:
        from github_analytics import pipeline
        from github_analytics.transform.validate import RawValidationError

        try:
            return dict(pipeline.validate(_settings(), _run_id()))
        except RawValidationError as exc:
            raise AirflowFailException(str(exc)) from exc  # contract break: do not retry

    @task
    def transform(_validated: dict[str, Any]) -> dict[str, int]:
        from github_analytics import pipeline

        return dict(pipeline.transform(_settings(), _run_id()))

    @task
    def load(_transformed: dict[str, int]) -> dict[str, Any]:
        from github_analytics import pipeline

        return dict(pipeline.load(_settings(), _run_id()))

    @task
    def quality_check(_loaded: dict[str, Any]) -> list[dict[str, Any]]:
        from github_analytics import pipeline
        from github_analytics.load.quality import QualityCheckError

        try:
            return list(pipeline.quality(_settings(), _run_id()))
        except QualityCheckError as exc:
            raise AirflowFailException(str(exc)) from exc

    @task
    def analytics_report(_checks: list[dict[str, Any]]) -> str:
        from github_analytics import pipeline

        return str(pipeline.report(_settings(), _run_id()))

    analytics_report(quality_check(load(transform(validate(extract())))))


dag_object = github_engineering_analytics()
