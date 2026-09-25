"""Post-load data quality checks on the star schema.

Each check is a SQL query returning the number of violating rows. ``error``
checks fail the pipeline (the Airflow task goes red and downstream reporting
is skipped); ``warn`` checks are logged and recorded in etl.pipeline_runs.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

logger = logging.getLogger(__name__)


class QualityCheckError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # "error" | "warn"
    sql: str
    description: str


CHECKS: tuple[Check, ...] = (
    Check(
        "staged_prs_not_loaded",
        "error",
        "SELECT COUNT(*) FROM stg.pull_requests s LEFT JOIN dw.fact_pull_request f "
        "ON f.pull_request_key = s.github_pr_id WHERE f.pull_request_key IS NULL",
        "every staged pull request reached the fact table",
    ),
    Check(
        "staged_commits_not_loaded",
        "error",
        "SELECT COUNT(*) FROM stg.commits s JOIN dw.dim_repository r "
        "ON r.full_name = s.repository_full_name LEFT JOIN dw.fact_commit f "
        "ON f.repository_key = r.repository_key AND f.commit_sha = s.commit_sha "
        "WHERE f.commit_sha IS NULL",
        "every staged commit reached the fact table",
    ),
    Check(
        "staged_issues_not_loaded",
        "error",
        "SELECT COUNT(*) FROM stg.issues s LEFT JOIN dw.fact_issue f "
        "ON f.issue_key = s.github_issue_id WHERE f.issue_key IS NULL",
        "every staged issue reached the fact table",
    ),
    Check(
        "pr_merged_before_created",
        "error",
        "SELECT COUNT(*) FROM dw.fact_pull_request WHERE merged_at < created_at",
        "merge time is never before creation time",
    ),
    Check(
        "pr_merged_but_open",
        "error",
        "SELECT COUNT(*) FROM dw.fact_pull_request WHERE is_merged AND state = 'open'",
        "a merged pull request is closed",
    ),
    Check(
        "issue_closed_before_created",
        "error",
        "SELECT COUNT(*) FROM dw.fact_issue WHERE closed_at < created_at",
        "close time is never before creation time",
    ),
    Check(
        "closed_issue_without_close_time",
        "warn",
        "SELECT COUNT(*) FROM dw.fact_issue WHERE state = 'closed' AND closed_at IS NULL",
        "closed issues carry a close time",
    ),
    Check(
        "reviews_without_pull_request",
        "warn",
        "SELECT COUNT(*) FROM stg.reviews s JOIN dw.dim_repository r "
        "ON r.full_name = s.repository_full_name LEFT JOIN dw.fact_pull_request p "
        "ON p.repository_key = r.repository_key AND p.pr_number = s.pr_number "
        "WHERE p.pull_request_key IS NULL",
        "staged reviews belong to a known pull request",
    ),
    Check(
        "commits_without_author",
        "warn",
        "SELECT COUNT(*) FROM dw.fact_commit WHERE author_key IS NULL",
        "commits can be attributed to a contributor",
    ),
    Check(
        "future_dated_activity",
        "warn",
        "SELECT (SELECT COUNT(*) FROM dw.fact_commit WHERE committed_at > now() + "
        "INTERVAL '1 day') + (SELECT COUNT(*) FROM dw.fact_pull_request WHERE created_at > "
        "now() + INTERVAL '1 day')",
        "no activity dated in the future (clock skew / bad data)",
    ),
)


def run_quality_checks(dsn: str, checks: tuple[Check, ...] = CHECKS) -> list[dict[str, Any]]:
    results = []
    with psycopg.connect(dsn) as conn:
        for check in checks:
            row = conn.execute(check.sql).fetchone()
            violations = int(row[0]) if row else 0
            passed = violations == 0
            results.append({**asdict(check), "violations": violations, "passed": passed})
            level = (
                logging.INFO
                if passed
                else (logging.ERROR if check.severity == "error" else logging.WARNING)
            )
            logger.log(
                level,
                "quality check",
                extra={"check": check.name, "violations": violations, "severity": check.severity},
            )
    failed = [r["name"] for r in results if not r["passed"] and r["severity"] == "error"]
    if failed:
        raise QualityCheckError(f"quality checks failed: {', '.join(failed)}")
    return results
