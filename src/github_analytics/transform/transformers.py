"""Raw GitHub payloads -> normalised tables for the star schema.

Every function takes an iterable of validated ``RawRecord`` and returns a
pandas DataFrame whose columns match the staging table of the same name
(``sql/schema.sql``). Timestamps are timezone-aware UTC.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

import pandas as pd

from github_analytics.extract.raw_store import RawRecord

COLUMNS: dict[str, list[str]] = {
    "repositories": [
        "github_repo_id",
        "full_name",
        "owner_login",
        "name",
        "description",
        "language",
        "is_private",
        "is_fork",
        "is_archived",
        "default_branch",
        "created_at",
        "pushed_at",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "html_url",
    ],
    "contributors": [
        "contributor_nk",
        "github_user_id",
        "login",
        "display_name",
        "user_type",
        "is_bot",
    ],
    "commits": [
        "repository_full_name",
        "commit_sha",
        "author_nk",
        "committer_nk",
        "authored_at",
        "committed_at",
        "message_headline",
        "parent_count",
        "html_url",
    ],
    "pull_requests": [
        "github_pr_id",
        "repository_full_name",
        "pr_number",
        "author_nk",
        "title",
        "state",
        "is_draft",
        "created_at",
        "updated_at",
        "closed_at",
        "merged_at",
        "base_ref",
        "html_url",
    ],
    "reviews": [
        "github_review_id",
        "repository_full_name",
        "pr_number",
        "reviewer_nk",
        "review_state",
        "submitted_at",
    ],
    "issues": [
        "github_issue_id",
        "repository_full_name",
        "issue_number",
        "author_nk",
        "title",
        "state",
        "state_reason",
        "created_at",
        "updated_at",
        "closed_at",
        "comments_count",
        "labels",
    ],
}

TIMESTAMP_COLUMNS = frozenset(
    {
        "created_at",
        "pushed_at",
        "authored_at",
        "committed_at",
        "updated_at",
        "closed_at",
        "merged_at",
        "submitted_at",
    }
)


INTEGER_COLUMNS = frozenset(
    {
        "github_repo_id",
        "github_user_id",
        "github_pr_id",
        "github_review_id",
        "github_issue_id",
        "pr_number",
        "issue_number",
        "parent_count",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "comments_count",
    }
)


def _frame(table: str, rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=COLUMNS[table])
    for col in TIMESTAMP_COLUMNS.intersection(df.columns):
        df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
    # Nullable Int64 keeps ids integral when some values are missing
    # (plain int64 would silently become float64: 12 -> 12.0).
    for col in INTEGER_COLUMNS.intersection(df.columns):
        df[col] = df[col].astype("Int64")
    return df


# ----------------------------------------------------------------- people
def user_contributor(user: dict[str, Any] | None) -> dict[str, Any] | None:
    """Contributor row for a GitHub user object (None for deleted/missing users)."""
    if not user or not user.get("login"):
        return None
    login = str(user["login"])
    user_type = str(user.get("type") or "User")
    return {
        "contributor_nk": f"gh:{login.lower()}",
        "github_user_id": user.get("id"),
        "login": login,
        "display_name": login,
        "user_type": user_type,
        "is_bot": user_type == "Bot" or login.endswith("[bot]"),
    }


def git_identity_contributor(identity: dict[str, Any] | None) -> dict[str, Any] | None:
    """Contributor for a commit author not linked to a GitHub account.

    The email is hashed: we need a stable key, not the address itself.
    """
    if not identity or not (identity.get("email") or identity.get("name")):
        return None
    email = str(identity.get("email") or "").strip().lower()
    basis = email or f"name:{identity.get('name')}"
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]
    name = str(identity.get("name") or "unknown")
    return {
        "contributor_nk": f"git:{digest}",
        "github_user_id": None,
        "login": None,
        "display_name": name,
        "user_type": "Unlinked",
        "is_bot": name.endswith("[bot]"),
    }


def commit_person(commit: dict[str, Any], role: str) -> dict[str, Any] | None:
    """Prefer the linked GitHub account; fall back to the raw git identity."""
    return user_contributor(commit.get(role)) or git_identity_contributor(
        commit.get("commit", {}).get(role)
    )


def _nk(person: dict[str, Any] | None) -> str | None:
    return person["contributor_nk"] if person else None


# --------------------------------------------------------------- entities
def transform_repositories(records: Iterable[RawRecord]) -> pd.DataFrame:
    rows = []
    for r in records:
        d = r.data
        rows.append(
            {
                "github_repo_id": d["id"],
                "full_name": d["full_name"],
                "owner_login": d["owner"]["login"],
                "name": d.get("name"),
                "description": d.get("description"),
                "language": d.get("language"),
                "is_private": bool(d.get("private")),
                "is_fork": bool(d.get("fork")),
                "is_archived": bool(d.get("archived")),
                "default_branch": d.get("default_branch"),
                "created_at": d.get("created_at"),
                "pushed_at": d.get("pushed_at"),
                "stargazers_count": d.get("stargazers_count"),
                "forks_count": d.get("forks_count"),
                "open_issues_count": d.get("open_issues_count"),
                "html_url": d.get("html_url"),
            }
        )
    return _frame("repositories", rows).drop_duplicates("github_repo_id", keep="last")


def transform_commits(records: Iterable[RawRecord]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows, people = [], []
    for r in records:
        d = r.data
        author, committer = commit_person(d, "author"), commit_person(d, "committer")
        people += [p for p in (author, committer) if p]
        message = (d.get("commit", {}).get("message") or "").splitlines()
        rows.append(
            {
                "repository_full_name": r.repository,
                "commit_sha": d["sha"],
                "author_nk": _nk(author),
                "committer_nk": _nk(committer),
                "authored_at": d["commit"]["author"]["date"],
                "committed_at": d["commit"]["committer"]["date"],
                "message_headline": message[0][:250] if message else None,
                "parent_count": len(d.get("parents") or []),
                "html_url": d.get("html_url"),
            }
        )
    df = _frame("commits", rows).drop_duplicates(["repository_full_name", "commit_sha"])
    return df, people


def transform_pull_requests(
    records: Iterable[RawRecord],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows, people = [], []
    for r in records:
        d = r.data
        author = user_contributor(d.get("user"))
        if author:
            people.append(author)
        rows.append(
            {
                "github_pr_id": d["id"],
                "repository_full_name": r.repository,
                "pr_number": d["number"],
                "author_nk": _nk(author),
                "title": d.get("title"),
                "state": d["state"],
                "is_draft": bool(d.get("draft")),
                "created_at": d["created_at"],
                "updated_at": d["updated_at"],
                "closed_at": d.get("closed_at"),
                "merged_at": d.get("merged_at"),
                "base_ref": (d.get("base") or {}).get("ref"),
                "html_url": d.get("html_url"),
            }
        )
    return _frame("pull_requests", rows).drop_duplicates("github_pr_id", keep="last"), people


def transform_reviews(records: Iterable[RawRecord]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows, people = [], []
    for r in records:
        d = r.data
        if not d.get("submitted_at"):
            continue  # PENDING reviews are drafts visible only to their author
        reviewer = user_contributor(d.get("user"))
        if reviewer:
            people.append(reviewer)
        rows.append(
            {
                "github_review_id": d["id"],
                "repository_full_name": r.repository,
                "pr_number": r.context["pull_number"],
                "reviewer_nk": _nk(reviewer),
                "review_state": d["state"],
                "submitted_at": d["submitted_at"],
            }
        )
    return _frame("reviews", rows).drop_duplicates("github_review_id", keep="last"), people


def transform_issues(records: Iterable[RawRecord]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows, people = [], []
    for r in records:
        d = r.data
        author = user_contributor(d.get("user"))
        if author:
            people.append(author)
        rows.append(
            {
                "github_issue_id": d["id"],
                "repository_full_name": r.repository,
                "issue_number": d["number"],
                "author_nk": _nk(author),
                "title": d.get("title"),
                "state": d["state"],
                "state_reason": d.get("state_reason"),
                "created_at": d["created_at"],
                "updated_at": d["updated_at"],
                "closed_at": d.get("closed_at"),
                "comments_count": d.get("comments", 0),
                "labels": [lbl["name"] for lbl in d.get("labels", []) if isinstance(lbl, dict)],
            }
        )
    return _frame("issues", rows).drop_duplicates("github_issue_id", keep="last"), people


def build_contributors(people: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """One row per contributor_nk, preferring rows that carry a GitHub user id."""
    df = _frame("contributors", list(people))
    if df.empty:
        return df
    df["_has_id"] = df["github_user_id"].notna()
    df = df.sort_values("_has_id").drop_duplicates("contributor_nk", keep="last")
    return df.drop(columns="_has_id").reset_index(drop=True)
