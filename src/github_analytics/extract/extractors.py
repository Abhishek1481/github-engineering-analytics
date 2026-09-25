"""Incremental extraction of repositories, commits, pull requests, reviews and issues.

Watermarks are stored per (repository, entity) in ``etl.extraction_watermarks``
and only advanced by the *load* step after data is safely in PostgreSQL, so a
failed run is simply re-extracted next time.

Incremental strategy per entity:
  * commits        GET /repos/{r}/commits?since=           server-side ``since`` (commit date)
  * issues         GET /repos/{r}/issues?since=&state=all  server-side ``since`` (updated_at)
  * pull_requests  GET /repos/{r}/pulls?sort=updated        stop paging once updated_at < since
  * reviews        GET /repos/{r}/pulls/{n}/reviews         only for PRs updated in the window
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from github_analytics.extract.client import GitHubAPIError, GitHubClient, NotFoundError
from github_analytics.extract.raw_store import RawStore

logger = logging.getLogger(__name__)

ENTITIES = ("repositories", "commits", "pull_requests", "reviews", "issues")
WATERMARKED_ENTITIES = ("commits", "pull_requests", "issues")

Watermarks = Mapping[tuple[str, str], datetime]


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class EntityResult:
    records: int = 0
    since: str | None = None
    new_watermark: str | None = None
    truncated: bool = False
    file: str | None = None


@dataclass
class ExtractionConfig:
    initial_lookback_days: int = 30
    watermark_overlap_minutes: int = 60
    include_forks: bool = False


@dataclass
class Extractor:
    client: GitHubClient
    store: RawStore
    watermarks: Watermarks = field(default_factory=dict)
    config: ExtractionConfig = field(default_factory=ExtractionConfig)
    now: datetime = field(default_factory=lambda: datetime.now(UTC))

    # ------------------------------------------------------------ discovery
    def discover_repositories(self, owner: str, owner_type: str) -> list[str]:
        path = f"/orgs/{owner}/repos" if owner_type == "org" else f"/users/{owner}/repos"
        params = {"type": "all" if owner_type == "org" else "owner", "sort": "full_name"}
        repos = [
            r["full_name"]
            for r in self.client.paginate(path, params)
            if self.config.include_forks or not r.get("fork")
        ]
        logger.info("repositories discovered", extra={"owner": owner, "count": len(repos)})
        return repos

    # ------------------------------------------------------------- windows
    def since_for(self, repo: str, entity: str) -> datetime:
        watermark = self.watermarks.get((repo, entity))
        if watermark is None:
            return self.now - timedelta(days=self.config.initial_lookback_days)
        return watermark - timedelta(minutes=self.config.watermark_overlap_minutes)

    def _new_watermark(
        self, repo: str, entity: str, max_seen: datetime | None, truncated: bool
    ) -> str | None:
        previous = self.watermarks.get((repo, entity))
        if truncated:
            # Listing was cut short by max_pages: never advance past unread data.
            logger.warning(
                "extraction truncated; watermark not advanced",
                extra={"repository": repo, "entity": entity},
            )
            return iso(previous) if previous else None
        candidates = [t for t in (previous, max_seen) if t is not None]
        return iso(max(candidates)) if candidates else None

    # ------------------------------------------------------------ entities
    def extract_repository(self, repo: str) -> EntityResult:
        data = self.client.get_json(f"/repos/{repo}")
        path, count = self.store.write("repositories", repo, [data])
        return EntityResult(records=count, file=str(path))

    def extract_commits(self, repo: str) -> EntityResult:
        since = self.since_for(repo, "commits")
        pager = self.client.paginate(f"/repos/{repo}/commits", {"since": iso(since)})
        try:
            commits = list(pager)
        except GitHubAPIError as exc:
            if exc.status == 409:  # "Git Repository is empty"
                logger.info("empty repository", extra={"repository": repo})
                commits = []
            else:
                raise
        max_seen = max(
            (
                ts
                for c in commits
                if (ts := parse_ts(c.get("commit", {}).get("committer", {}).get("date")))
            ),
            default=None,
        )
        path, count = self.store.write("commits", repo, commits)
        return EntityResult(
            count,
            iso(since),
            self._new_watermark(repo, "commits", max_seen, pager.truncated),
            pager.truncated,
            str(path),
        )

    def extract_pull_requests(self, repo: str) -> tuple[EntityResult, list[int]]:
        since = self.since_for(repo, "pull_requests")
        pager = self.client.paginate(
            f"/repos/{repo}/pulls", {"state": "all", "sort": "updated", "direction": "desc"}
        )
        pulls: list[dict[str, Any]] = []
        for pr in pager:
            updated = parse_ts(pr.get("updated_at"))
            if updated is not None and updated < since:
                break  # sorted by updated desc: everything after this is older
            pulls.append(pr)
        max_seen = max((t for p in pulls if (t := parse_ts(p.get("updated_at")))), default=None)
        path, count = self.store.write("pull_requests", repo, pulls)
        result = EntityResult(
            count,
            iso(since),
            self._new_watermark(repo, "pull_requests", max_seen, pager.truncated),
            pager.truncated,
            str(path),
        )
        return result, [int(p["number"]) for p in pulls if "number" in p]

    def extract_reviews(self, repo: str, pr_numbers: list[int]) -> EntityResult:
        total = 0
        path = None
        for number in pr_numbers:
            try:
                reviews = list(self.client.paginate(f"/repos/{repo}/pulls/{number}/reviews"))
            except NotFoundError:
                logger.warning(
                    "pull request vanished", extra={"repository": repo, "number": number}
                )
                continue
            path, count = self.store.write(
                "reviews", repo, reviews, context={"pull_number": number}
            )
            total += count
        return EntityResult(records=total, file=str(path) if path else None)

    def extract_issues(self, repo: str) -> EntityResult:
        since = self.since_for(repo, "issues")
        pager = self.client.paginate(
            f"/repos/{repo}/issues",
            {"state": "all", "since": iso(since), "sort": "updated", "direction": "asc"},
        )
        # The issues endpoint also returns pull requests; those are handled above.
        issues = [i for i in pager if "pull_request" not in i]
        max_seen = max((t for i in issues if (t := parse_ts(i.get("updated_at")))), default=None)
        path, count = self.store.write("issues", repo, issues)
        return EntityResult(
            count,
            iso(since),
            self._new_watermark(repo, "issues", max_seen, pager.truncated),
            pager.truncated,
            str(path),
        )

    # ----------------------------------------------------------------- run
    def run(self, repositories: list[str]) -> dict[str, Any]:
        started = datetime.now(UTC)
        self.store.reset()
        entities: dict[str, dict[str, Any]] = {e: {} for e in ENTITIES}
        for repo in repositories:
            logger.info("extracting repository", extra={"repository": repo})
            entities["repositories"][repo] = vars(self.extract_repository(repo))
            entities["commits"][repo] = vars(self.extract_commits(repo))
            pr_result, numbers = self.extract_pull_requests(repo)
            entities["pull_requests"][repo] = vars(pr_result)
            entities["reviews"][repo] = vars(self.extract_reviews(repo, numbers))
            entities["issues"][repo] = vars(self.extract_issues(repo))
        counts = {e: sum(r["records"] for r in by_repo.values()) for e, by_repo in entities.items()}
        manifest = {
            "run_id": self.store.run_id,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "repositories": repositories,
            "record_counts": counts,
            "entities": entities,
            "request_stats": vars(self.client.stats),
        }
        self.store.write_manifest(manifest)
        logger.info(
            "extraction complete",
            extra={
                "run_id": self.store.run_id,
                **counts,
                "api_requests": self.client.stats.requests,
            },
        )
        return manifest
