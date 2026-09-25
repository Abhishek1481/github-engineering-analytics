"""PostgreSQL loading: COPY into staging, SQL upserts into the star schema.

A load is one transaction: truncate staging -> COPY -> dimensions.sql ->
facts.sql -> advance watermarks. Either everything becomes visible or
nothing does, and because every statement is an upsert the same run can be
loaded twice without duplicating facts (idempotent).
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import psycopg

from github_analytics.transform.transformers import COLUMNS
from github_analytics.utils.config import PROJECT_ROOT
from github_analytics.utils.sql import split_statements

logger = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"
STAGING_TABLES = tuple(COLUMNS)


def _clean(value: Any) -> Any:
    """Convert pandas/numpy scalars to plain Python values for COPY."""
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.ndarray):
        return [_clean(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    return value


class PostgresLoader:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection[Any]]:
        with psycopg.connect(self.dsn) as conn:
            yield conn

    def run_sql_file(self, conn: psycopg.Connection[Any], name: str) -> None:
        for statement in split_statements((SQL_DIR / name).read_text(encoding="utf-8")):
            conn.execute(statement)

    def init_schema(self) -> None:
        with self.connect() as conn:
            self.run_sql_file(conn, "schema.sql")
            self.run_sql_file(conn, "dimensions.sql")  # seeds dim_date on first run
        logger.info("schema initialised")

    # ----------------------------------------------------------- watermarks
    def read_watermarks(self) -> dict[tuple[str, str], datetime]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT repository_full_name, entity, watermark_at FROM etl.extraction_watermarks"
            ).fetchall()
        return {(r[0], r[1]): r[2] for r in rows}

    @staticmethod
    def _update_watermarks(conn: psycopg.Connection[Any], manifest: dict[str, Any]) -> int:
        updated = 0
        for entity, by_repo in manifest["entities"].items():
            for repo, result in by_repo.items():
                if not result.get("new_watermark"):
                    continue
                conn.execute(
                    "INSERT INTO etl.extraction_watermarks "
                    "(repository_full_name, entity, watermark_at, updated_by_run) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (repository_full_name, entity) DO UPDATE SET "
                    "watermark_at = GREATEST(etl.extraction_watermarks.watermark_at, "
                    "EXCLUDED.watermark_at), updated_by_run = EXCLUDED.updated_by_run, "
                    "updated_at = now()",
                    (repo, entity, result["new_watermark"], manifest["run_id"]),
                )
                updated += 1
        return updated

    # ------------------------------------------------------------------ load
    @staticmethod
    def _copy(conn: psycopg.Connection[Any], table: str, df: pd.DataFrame) -> int:
        columns = COLUMNS[table]
        sql = f"COPY stg.{table} ({', '.join(columns)}) FROM STDIN"
        with conn.cursor() as cur, cur.copy(sql) as copy:
            for row in df[columns].itertuples(index=False, name=None):
                copy.write_row([_clean(v) for v in row])
        return len(df)

    def load(self, tables: dict[str, pd.DataFrame], manifest: dict[str, Any]) -> dict[str, Any]:
        with self.connect() as conn, conn.transaction():
            conn.execute("TRUNCATE " + ", ".join(f"stg.{t}" for t in STAGING_TABLES))
            staged = {t: self._copy(conn, t, df) for t, df in tables.items()}
            self.run_sql_file(conn, "dimensions.sql")
            self.run_sql_file(conn, "facts.sql")
            watermarks = self._update_watermarks(conn, manifest)
        logger.info(
            "load committed",
            extra={"run_id": manifest["run_id"], **staged, "watermarks_updated": watermarks},
        )
        return {"staged_rows": staged, "watermarks_updated": watermarks}

    # -------------------------------------------------------------- run log
    def record_run(self, run_id: str, status: str, **fields: Any) -> None:
        """Upsert the etl.pipeline_runs row (lineage + observability)."""
        json_fields = {
            k: json.dumps(v, default=str)
            for k, v in fields.items()
            if k in {"extracted", "loaded", "quality_checks"}
        }
        repositories = fields.get("repositories")
        error = fields.get("error_message")
        finished = status in {"succeeded", "failed"}
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO etl.pipeline_runs (run_id, status, repositories, extracted, loaded, "
                "quality_checks, error_message, finished_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, CASE WHEN %s THEN now() END) "
                "ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, "
                "repositories = COALESCE(EXCLUDED.repositories, etl.pipeline_runs.repositories), "
                "extracted = COALESCE(EXCLUDED.extracted, etl.pipeline_runs.extracted), "
                "loaded = COALESCE(EXCLUDED.loaded, etl.pipeline_runs.loaded), "
                "quality_checks = COALESCE(EXCLUDED.quality_checks, "
                "etl.pipeline_runs.quality_checks), "
                "error_message = EXCLUDED.error_message, finished_at = EXCLUDED.finished_at",
                (
                    run_id,
                    status,
                    repositories,
                    json_fields.get("extracted"),
                    json_fields.get("loaded"),
                    json_fields.get("quality_checks"),
                    error,
                    finished,
                ),
            )
