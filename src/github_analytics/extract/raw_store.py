"""Raw landing zone: unmodified API payloads as JSONL, one directory per run.

    <data_dir>/raw/<run_id>/manifest.json
    <data_dir>/raw/<run_id>/<entity>/<owner>__<repo>.jsonl

Each line wraps the untouched API object with lineage metadata::

    {"repository": "owner/repo", "extracted_at": "...", "context": {...}, "data": {...}}

Keeping raw payloads makes transformations replayable without re-calling
the API (ELT-style), and gives an audit trail of what the source returned.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MANIFEST = "manifest.json"


def repo_slug(full_name: str) -> str:
    return full_name.replace("/", "__")


@dataclass(frozen=True)
class RawRecord:
    repository: str
    entity: str
    data: dict[str, Any]
    context: dict[str, Any]
    extracted_at: str


class RawStore:
    def __init__(self, data_dir: Path, run_id: str) -> None:
        self.run_id = run_id
        self.root = Path(data_dir) / "raw" / run_id

    def reset(self) -> None:
        """Remove this run's files so a retried extract starts clean (idempotent)."""
        shutil.rmtree(self.root, ignore_errors=True)

    def write(
        self,
        entity: str,
        repository: str,
        records: Iterable[dict[str, Any]],
        context: dict[str, Any] | None = None,
    ) -> tuple[Path, int]:
        path = self.root / entity / f"{repo_slug(repository)}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(UTC).isoformat()
        count = 0
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            for record in records:
                line = {
                    "repository": repository,
                    "extracted_at": now,
                    "context": context or {},
                    "data": record,
                }
                fh.write(json.dumps(line, separators=(",", ":")) + "\n")
                count += 1
        return path, count

    def read(self, entity: str) -> Iterator[RawRecord]:
        """Stream records for an entity across all repositories (memory-flat)."""
        folder = self.root / entity
        if not folder.exists():
            return
        for path in sorted(folder.glob("*.jsonl")):
            with path.open(encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    yield RawRecord(
                        obj["repository"],
                        entity,
                        obj["data"],
                        obj.get("context", {}),
                        obj["extracted_at"],
                    )

    def write_manifest(self, manifest: dict[str, Any]) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / MANIFEST
        path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        return path

    def read_manifest(self) -> dict[str, Any]:
        data: dict[str, Any] = json.loads((self.root / MANIFEST).read_text(encoding="utf-8"))
        return data
