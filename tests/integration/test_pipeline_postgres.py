"""End-to-end pipeline against a real PostgreSQL (TEST_POSTGRES_DSN), GitHub mocked.

Verifies the star-schema DDL, dimension/fact upserts, watermarks, quality
checks, analytics SQL and idempotency (loading the same data twice).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import responses

from fixtures import github_payloads as gp
from github_analytics import pipeline
from github_analytics.extract.client import GitHubClient
from github_analytics.load.quality import CHECKS, Check, QualityCheckError, run_quality_checks
from github_analytics.utils.config import Settings

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("TEST_POSTGRES_DSN")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="TEST_POSTGRES_DSN not set"),
]
API = "https://api.github.test"


@pytest.fixture
def database() -> Iterator[str]:
    name = f"gh_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    try:
        yield psycopg.conninfo.make_conninfo(DSN, dbname=name)
    finally:
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def settings(database: str, tmp_path: Path) -> Settings:
    return Settings.load(
        github_token=None,
        repositories=[gp.REPO],
        database_url=database,
        data_dir=tmp_path / "data",
        reports_dir=tmp_path / "reports",
        github_api_url=API,
        initial_lookback_days=3650,
    )


def mock_github(
    pulls: list[dict[str, Any]],
    reviews: dict[int, list[dict[str, Any]]],
    issues: list[dict[str, Any]],
    commits: list[dict[str, Any]],
) -> None:
    responses.get(f"{API}/repos/{gp.REPO}", json=gp.repository())
    responses.get(f"{API}/repos/{gp.REPO}/commits", json=commits)
    responses.get(f"{API}/repos/{gp.REPO}/pulls", json=pulls)
    responses.get(f"{API}/repos/{gp.REPO}/issues", json=issues)
    for number, body in reviews.items():
        responses.get(f"{API}/repos/{gp.REPO}/pulls/{number}/reviews", json=body)


PULLS = [
    gp.pull_request(2, 202, "2024-06-03T09:00:00Z", "2024-06-05T09:00:00Z", gp.BOB),
    gp.pull_request(
        1,
        201,
        "2024-06-01T09:00:00Z",
        "2024-06-02T21:00:00Z",
        gp.ALICE,
        merged="2024-06-02T21:00:00Z",
    ),
]
REVIEWS = {
    1: [
        gp.review(9001, gp.BOB, "CHANGES_REQUESTED", "2024-06-01T15:00:00Z"),
        gp.review(9002, gp.BOB, "APPROVED", "2024-06-02T20:00:00Z"),
    ],
    2: [],
}
ISSUES = [
    gp.issue(
        10,
        810,
        "2024-06-01T00:00:00Z",
        "2024-06-04T00:00:00Z",
        gp.ALICE,
        closed="2024-06-04T00:00:00Z",
    ),
    gp.issue(11, 811, "2024-06-02T00:00:00Z", "2024-06-02T00:00:00Z", gp.DEPENDABOT),
]
COMMITS = [
    gp.commit(1, "2024-06-02T20:30:00Z", gp.ALICE),
    gp.commit(2, "2024-06-03T08:00:00Z", None, git_name="Carol", git_email="carol@example.com"),
    gp.commit(3, "2024-06-10T08:00:00Z", gp.BOB, parents=2),
]


def fetch(dsn: str, sql: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql).fetchall()


def run(settings: Settings, run_id: str) -> dict[str, Any]:
    client = GitHubClient(token=None, base_url=API, sleep=lambda _s: None)
    pipeline.extract(settings, run_id, client=client)
    pipeline.validate(settings, run_id)
    pipeline.transform(settings, run_id)
    loaded = pipeline.load(settings, run_id)
    pipeline.quality(settings, run_id)
    pipeline.report(settings, run_id)
    return loaded


@responses.activate
def test_full_pipeline_builds_star_schema(settings: Settings, database: str) -> None:
    mock_github(PULLS, REVIEWS, ISSUES, COMMITS)
    loaded = run(settings, "run_1")

    assert loaded["staged_rows"]["pull_requests"] == 2
    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_commit") == [(3,)]
    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_review") == [(2,)]
    assert fetch(
        database,
        "SELECT pr_number, is_merged, cycle_time_hours "
        "FROM dw.fact_pull_request ORDER BY pr_number",
    ) == [(1, True, 36), (2, False, None)]
    assert fetch(
        database, "SELECT time_to_close_hours FROM dw.fact_issue WHERE issue_number = 10"
    ) == [(72,)]
    # Contributors: alice, bob, dependabot + one unlinked git identity
    assert fetch(
        database, "SELECT user_type, COUNT(*) FROM dw.dim_contributor GROUP BY 1 ORDER BY 1"
    ) == [("Bot", 1), ("Unlinked", 1), ("User", 2)]
    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_commit WHERE is_merge_commit") == [(1,)]
    # Date dimension wired correctly (2024-06-03 is a Monday)
    assert fetch(
        database,
        "SELECT d.week_start_date::TEXT FROM dw.fact_pull_request p "
        "JOIN dw.dim_date d ON d.date_key = p.created_date_key "
        "WHERE p.pr_number = 2",
    ) == [("2024-06-03",)]

    marks = dict(fetch(database, "SELECT entity, watermark_at FROM etl.extraction_watermarks"))
    assert marks["pull_requests"] == datetime(2024, 6, 5, 9, tzinfo=UTC)
    assert marks["commits"] == datetime(2024, 6, 10, 8, tzinfo=UTC)
    assert fetch(database, "SELECT status FROM etl.pipeline_runs") == [("succeeded",)]

    report = (settings.reports_dir / "latest" / "report.md").read_text()
    assert "Pr cycle time by repository" in report
    assert (settings.reports_dir / "run_1" / "weekly_commit_volume.csv").exists()


@responses.activate
def test_reloading_is_idempotent_and_updates_change(settings: Settings, database: str) -> None:
    mock_github(PULLS, REVIEWS, ISSUES, COMMITS)
    run(settings, "run_1")
    run(settings, "run_1_retry")  # identical data again (e.g. overlap window)

    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_pull_request") == [(2,)]
    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_commit") == [(3,)]
    assert fetch(database, "SELECT COUNT(*) FROM dw.dim_contributor") == [(4,)]

    responses.reset()
    merged_later = gp.pull_request(
        2,
        202,
        "2024-06-03T09:00:00Z",
        "2024-06-06T09:00:00Z",
        gp.BOB,
        merged="2024-06-06T09:00:00Z",
    )
    mock_github([merged_later], {2: []}, [], [])
    run(settings, "run_2")
    assert fetch(
        database, "SELECT is_merged, state FROM dw.fact_pull_request WHERE pr_number = 2"
    ) == [(True, "closed")]
    assert fetch(database, "SELECT COUNT(*) FROM dw.fact_pull_request") == [(2,)]


@responses.activate
def test_all_analytics_queries_return_rows(settings: Settings, database: str) -> None:
    mock_github(PULLS, REVIEWS, ISSUES, COMMITS)
    run(settings, "run_1")
    from github_analytics.analytics.report import run_queries

    results = run_queries(database)
    assert len(results) == 10
    empty = [name for name, (_, rows) in results.items() if not rows]
    assert empty == []
    cols, rows = results["review_activity"]
    row = dict(zip(cols, rows[0], strict=True))
    assert row["prs_reviewed"] == 1
    assert float(row["median_hours_to_first_review"]) == 6.0


def test_quality_gate_raises_on_error_check(database: str, settings: Settings) -> None:
    from github_analytics.load.postgres import PostgresLoader

    PostgresLoader(database).init_schema()
    failing = (*CHECKS, Check("always_fails", "error", "SELECT 1", "test"))
    with pytest.raises(QualityCheckError, match="always_fails"):
        run_quality_checks(database, failing)
