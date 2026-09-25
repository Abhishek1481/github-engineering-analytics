"""Pipeline steps shared by the Airflow DAG and the ``gh-analytics`` CLI.

Each step takes ``(settings, run_id)``, reads its inputs from the run's
directory or the database, and returns a small JSON-serialisable summary
(safe to pass through Airflow XCom). Data itself never travels via XCom.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from github_analytics.analytics.report import write_report
from github_analytics.extract.client import GitHubClient
from github_analytics.extract.extractors import ExtractionConfig, Extractor, Watermarks
from github_analytics.extract.raw_store import RawStore
from github_analytics.load.postgres import PostgresLoader
from github_analytics.load.quality import run_quality_checks
from github_analytics.transform import transformers as tf
from github_analytics.transform.validate import is_valid, validate_run
from github_analytics.utils.config import ConfigError, Settings

logger = logging.getLogger(__name__)


def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+'; make them filesystem-safe."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def new_run_id() -> str:
    return datetime.now(UTC).strftime("manual__%Y%m%dT%H%M%SZ")


def _staged_dir(settings: Settings, run_id: str) -> Path:
    return settings.data_dir / "staged" / run_id


def build_client(settings: Settings) -> GitHubClient:
    return GitHubClient(
        token=settings.github_token,
        base_url=settings.github_api_url,
        timeout=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        max_rate_limit_wait_seconds=settings.max_rate_limit_wait_seconds,
        max_pages=settings.max_pages,
    )


# ------------------------------------------------------------------- steps
def extract(settings: Settings, run_id: str, client: GitHubClient | None = None) -> dict[str, Any]:
    client = client or build_client(settings)
    watermarks: Watermarks = {}
    if settings.database_url:
        loader = PostgresLoader(settings.database_url)
        loader.init_schema()
        watermarks = loader.read_watermarks()
    extractor = Extractor(
        client,
        RawStore(settings.data_dir, run_id),
        watermarks,
        ExtractionConfig(
            settings.initial_lookback_days,
            settings.watermark_overlap_minutes,
            settings.include_forks,
        ),
    )
    repos = settings.repositories
    if not repos:
        if not settings.discover_owner:
            raise ConfigError("Set GITHUB_REPOSITORIES or GITHUB_OWNER")
        repos = extractor.discover_repositories(
            settings.discover_owner, settings.discover_owner_type
        )
    manifest = extractor.run(repos)
    if settings.database_url:
        PostgresLoader(settings.database_url).record_run(
            run_id, "running", repositories=repos, extracted=manifest["record_counts"]
        )
    return {
        "run_id": run_id,
        "repositories": repos,
        "record_counts": manifest["record_counts"],
        "api_requests": manifest["request_stats"]["requests"],
    }


def validate(settings: Settings, run_id: str) -> dict[str, Any]:
    store = RawStore(settings.data_dir, run_id)
    summary = validate_run(store, settings.max_invalid_ratio)
    manifest = store.read_manifest()
    manifest["validation"] = summary
    store.write_manifest(manifest)
    return summary


def transform(settings: Settings, run_id: str) -> dict[str, int]:
    store = RawStore(settings.data_dir, run_id)

    def valid(entity: str) -> Any:
        return (r for r in store.read(entity) if is_valid(r))

    commits, people_c = tf.transform_commits(valid("commits"))
    pulls, people_p = tf.transform_pull_requests(valid("pull_requests"))
    reviews, people_r = tf.transform_reviews(valid("reviews"))
    issues, people_i = tf.transform_issues(valid("issues"))
    tables = {
        "repositories": tf.transform_repositories(valid("repositories")),
        "contributors": tf.build_contributors([*people_c, *people_p, *people_r, *people_i]),
        "commits": commits,
        "pull_requests": pulls,
        "reviews": reviews,
        "issues": issues,
    }
    out = _staged_dir(settings, run_id)
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_parquet(out / f"{name}.parquet", index=False)
    counts = {name: len(df) for name, df in tables.items()}
    logger.info("transform complete", extra={"run_id": run_id, **counts})
    return counts


def load(settings: Settings, run_id: str) -> dict[str, Any]:
    staged = _staged_dir(settings, run_id)
    tables = {name: pd.read_parquet(staged / f"{name}.parquet") for name in tf.COLUMNS}
    manifest = RawStore(settings.data_dir, run_id).read_manifest()
    loader = PostgresLoader(settings.require_database())
    result = loader.load(tables, manifest)
    loader.record_run(run_id, "running", loaded=result)
    return result


def quality(settings: Settings, run_id: str) -> list[dict[str, Any]]:
    dsn = settings.require_database()
    loader = PostgresLoader(dsn)
    try:
        results = run_quality_checks(dsn)
    except Exception as exc:
        loader.record_run(run_id, "failed", error_message=str(exc))
        raise
    loader.record_run(run_id, "running", quality_checks=results)
    return results


def report(settings: Settings, run_id: str) -> str:
    dsn = settings.require_database()
    path = write_report(dsn, settings.reports_dir, run_id)
    PostgresLoader(dsn).record_run(run_id, "succeeded")
    return str(path)


def mark_failed(settings: Settings, run_id: str, error: str) -> None:
    if settings.database_url:
        PostgresLoader(settings.database_url).record_run(run_id, "failed", error_message=error)


def run_all(settings: Settings, run_id: str | None = None) -> dict[str, Any]:
    run_id = safe_run_id(run_id or new_run_id())
    logger.info("pipeline started", extra={"run_id": run_id})
    try:
        summary: dict[str, Any] = {"run_id": run_id}
        summary["extract"] = extract(settings, run_id)
        summary["validate"] = validate(settings, run_id)
        summary["transform"] = transform(settings, run_id)
        summary["load"] = load(settings, run_id)
        summary["quality"] = [
            {k: r[k] for k in ("name", "severity", "violations")} for r in quality(settings, run_id)
        ]
        summary["report"] = report(settings, run_id)
    except Exception as exc:
        logger.exception("pipeline failed", extra={"run_id": run_id})
        mark_failed(settings, run_id, str(exc))
        raise
    logger.info("pipeline completed", extra={"run_id": run_id})
    return summary
