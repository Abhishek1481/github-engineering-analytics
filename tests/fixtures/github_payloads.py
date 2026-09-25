"""Synthetic GitHub REST API payloads (trimmed to the fields the pipeline uses,
with the same shapes as the real API). Not copied from any real repository."""

from __future__ import annotations

from typing import Any

REPO = "acme/widgets"


def user(login: str, uid: int, kind: str = "User") -> dict[str, Any]:
    return {"login": login, "id": uid, "type": kind}


def repository(full_name: str = REPO, repo_id: int = 1001) -> dict[str, Any]:
    owner, name = full_name.split("/")
    return {
        "id": repo_id,
        "name": name,
        "full_name": full_name,
        "private": False,
        "owner": {"login": owner, "id": 1},
        "description": "Widget factory",
        "fork": False,
        "archived": False,
        "language": "Python",
        "default_branch": "main",
        "created_at": "2020-01-01T00:00:00Z",
        "pushed_at": "2024-06-10T12:00:00Z",
        "stargazers_count": 42,
        "forks_count": 7,
        "open_issues_count": 3,
        "html_url": f"https://github.com/{full_name}",
    }


def commit(
    sha_seed: int,
    date: str,
    author: dict[str, Any] | None,
    git_name: str = "Dev",
    git_email: str = "dev@example.com",
    parents: int = 1,
    message: str = "Fix widget alignment\n\nDetails",
) -> dict[str, Any]:
    return {
        "sha": f"{sha_seed:040x}",
        "commit": {
            "author": {"name": git_name, "email": git_email, "date": date},
            "committer": {"name": git_name, "email": git_email, "date": date},
            "message": message,
        },
        "author": author,
        "committer": author,
        "parents": [{"sha": f"{i:040x}"} for i in range(parents)],
        "html_url": f"https://github.com/{REPO}/commit/{sha_seed:040x}",
    }


def pull_request(
    number: int,
    pr_id: int,
    created: str,
    updated: str,
    author: dict[str, Any] | None,
    merged: str | None = None,
    closed: str | None = None,
    draft: bool = False,
) -> dict[str, Any]:
    return {
        "id": pr_id,
        "number": number,
        "state": "closed" if closed or merged else "open",
        "title": f"PR {number}",
        "user": author,
        "draft": draft,
        "created_at": created,
        "updated_at": updated,
        "closed_at": closed or merged,
        "merged_at": merged,
        "base": {"ref": "main"},
        "html_url": f"https://github.com/{REPO}/pull/{number}",
    }


def review(
    review_id: int, reviewer: dict[str, Any], state: str, submitted: str | None
) -> dict[str, Any]:
    return {"id": review_id, "user": reviewer, "state": state, "submitted_at": submitted}


def issue(
    number: int,
    issue_id: int,
    created: str,
    updated: str,
    author: dict[str, Any] | None,
    closed: str | None = None,
    labels: tuple[str, ...] = ("bug",),
    is_pr: bool = False,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": issue_id,
        "number": number,
        "title": f"Issue {number}",
        "user": author,
        "state": "closed" if closed else "open",
        "state_reason": "completed" if closed else None,
        "created_at": created,
        "updated_at": updated,
        "closed_at": closed,
        "comments": 2,
        "labels": [{"name": n} for n in labels],
    }
    if is_pr:
        body["pull_request"] = {"url": "..."}
    return body


ALICE = user("alice", 11)
BOB = user("bob", 12)
DEPENDABOT = user("dependabot[bot]", 49699333, "Bot")
