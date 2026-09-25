"""Run the named analytics queries and publish CSV + a Markdown summary."""

from __future__ import annotations

import csv
import logging
import shutil
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from github_analytics.utils.config import PROJECT_ROOT
from github_analytics.utils.sql import load_named_queries

logger = logging.getLogger(__name__)

ANALYTICS_SQL = PROJECT_ROOT / "sql" / "analytics.sql"


def markdown_table(columns: Sequence[str], rows: Sequence[Sequence[Any]], limit: int = 15) -> str:
    def fmt(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M")
        return str(v).replace("|", "\\|")

    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in rows[:limit]]
    if len(rows) > limit:
        lines.append(f"\n_{len(rows) - limit} more rows in the CSV._")
    return "\n".join(lines)


def run_queries(dsn: str) -> dict[str, tuple[list[str], list[tuple[Any, ...]]]]:
    results = {}
    with psycopg.connect(dsn) as conn:
        for name, sql in load_named_queries(ANALYTICS_SQL).items():
            cur = conn.execute(sql)
            columns = [d.name for d in cur.description or []]
            results[name] = (columns, cur.fetchall())
    return results


def write_report(dsn: str, reports_dir: Path, run_id: str) -> Path:
    results = run_queries(dsn)
    out = reports_dir / run_id
    out.mkdir(parents=True, exist_ok=True)
    sections = [
        f"# Engineering activity report\n\nRun `{run_id}` generated "
        f"{datetime.now(UTC):%Y-%m-%d %H:%M} UTC.\n"
    ]
    for name, (columns, rows) in results.items():
        with (out / f"{name}.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            writer.writerows(rows)
        sections.append(
            f"## {name.replace('_', ' ').capitalize()}\n\n{markdown_table(columns, rows)}\n"
        )
    report = out / "report.md"
    report.write_text("\n".join(sections), encoding="utf-8")
    latest = reports_dir / "latest"
    shutil.rmtree(latest, ignore_errors=True)
    shutil.copytree(out, latest)
    logger.info("report written", extra={"path": str(report), "queries": len(results)})
    return report
