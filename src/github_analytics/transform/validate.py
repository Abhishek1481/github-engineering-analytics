"""Structural validation of raw API records before transformation.

Invalid records are written to ``quarantine/<entity>.jsonl`` with the reason,
excluded from loading, and counted. If the invalid share of any entity
exceeds ``max_invalid_ratio`` the step fails: that usually means the API
contract changed and silently loading partial data would be worse.
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from github_analytics.extract.raw_store import RawRecord, RawStore

logger = logging.getLogger(__name__)

SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class RawValidationError(RuntimeError):
    pass


def _get(obj: Any, path: str) -> Any:
    for part in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_str(v: Any) -> bool:
    return isinstance(v, str) and v != ""


def _is_ts(v: Any) -> bool:
    if not isinstance(v, str):
        return False
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _optional(check: Callable[[Any], bool]) -> Callable[[Any], bool]:
    return lambda v: v is None or check(v)


def _one_of(*allowed: str) -> Callable[[Any], bool]:
    return lambda v: v in allowed


Rule = tuple[str, Callable[[Any], bool], str]

RULES: dict[str, list[Rule]] = {
    "repositories": [
        ("id", _is_int, "integer"),
        ("full_name", _is_str, "non-empty string"),
        ("owner.login", _is_str, "non-empty string"),
        ("created_at", _is_ts, "ISO timestamp"),
    ],
    "commits": [
        ("sha", lambda v: isinstance(v, str) and bool(SHA_RE.match(v)), "40-char hex SHA"),
        ("commit.author.date", _is_ts, "ISO timestamp"),
        ("commit.committer.date", _is_ts, "ISO timestamp"),
        ("parents", lambda v: isinstance(v, list), "list"),
    ],
    "pull_requests": [
        ("id", _is_int, "integer"),
        ("number", _is_int, "integer"),
        ("state", _one_of("open", "closed"), "open|closed"),
        ("created_at", _is_ts, "ISO timestamp"),
        ("updated_at", _is_ts, "ISO timestamp"),
        ("merged_at", _optional(_is_ts), "ISO timestamp or null"),
        ("closed_at", _optional(_is_ts), "ISO timestamp or null"),
    ],
    "reviews": [
        ("id", _is_int, "integer"),
        ("state", _is_str, "non-empty string"),
        ("submitted_at", _optional(_is_ts), "ISO timestamp or null"),
    ],
    "issues": [
        ("id", _is_int, "integer"),
        ("number", _is_int, "integer"),
        ("state", _one_of("open", "closed"), "open|closed"),
        ("created_at", _is_ts, "ISO timestamp"),
        ("updated_at", _is_ts, "ISO timestamp"),
        ("closed_at", _optional(_is_ts), "ISO timestamp or null"),
    ],
}


def validate_record(record: RawRecord) -> list[str]:
    """Return a list of human-readable problems (empty = valid)."""
    problems = [
        f"{path}: expected {expected}, got {_get(record.data, path)!r}"
        for path, check, expected in RULES[record.entity]
        if not check(_get(record.data, path))
    ]
    if record.entity == "reviews" and not _is_int(record.context.get("pull_number")):
        problems.append("context.pull_number missing")
    return problems


def is_valid(record: RawRecord) -> bool:
    return not validate_record(record)


@dataclass
class EntityReport:
    checked: int = 0
    invalid: int = 0
    reasons: Counter[str] = field(default_factory=Counter)

    @property
    def invalid_ratio(self) -> float:
        return self.invalid / self.checked if self.checked else 0.0


def validate_run(store: RawStore, max_invalid_ratio: float) -> dict[str, dict[str, Any]]:
    reports: dict[str, EntityReport] = {}
    quarantine_dir = store.root / "quarantine"
    for entity in RULES:
        report = reports.setdefault(entity, EntityReport())
        quarantined: list[str] = []
        for record in store.read(entity):
            report.checked += 1
            problems = validate_record(record)
            if problems:
                report.invalid += 1
                report.reasons.update(p.split(":")[0] for p in problems)
                quarantined.append(
                    json.dumps(
                        {"repository": record.repository, "problems": problems, "data": record.data}
                    )
                )
        if quarantined:
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            (quarantine_dir / f"{entity}.jsonl").write_text(
                "\n".join(quarantined) + "\n", encoding="utf-8"
            )
        logger.info(
            "raw records validated",
            extra={"entity": entity, "checked": report.checked, "invalid": report.invalid},
        )

    summary: dict[str, dict[str, Any]] = {
        e: {
            "checked": r.checked,
            "invalid": r.invalid,
            "invalid_ratio": round(r.invalid_ratio, 4),
            "reasons": dict(r.reasons),
        }
        for e, r in reports.items()
    }
    breaches = {e: s for e, s in summary.items() if s["invalid_ratio"] > max_invalid_ratio}
    if breaches:
        raise RawValidationError(
            f"invalid record ratio above {max_invalid_ratio:.1%}: {json.dumps(breaches)}"
        )
    return summary
