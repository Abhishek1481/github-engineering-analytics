from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import responses

from fixtures import github_payloads as gp
from github_analytics.extract.client import GitHubClient
from github_analytics.extract.extractors import ExtractionConfig, Extractor
from github_analytics.extract.raw_store import RawStore

API = "https://api.github.test"
NOW = datetime(2024, 6, 15, tzinfo=UTC)


def make_extractor(client: GitHubClient, tmp_path: Path, watermarks=None) -> Extractor:  # type: ignore[no-untyped-def]
    return Extractor(
        client,
        RawStore(tmp_path, "run1"),
        watermarks or {},
        ExtractionConfig(initial_lookback_days=30, watermark_overlap_minutes=60),
        now=NOW,
    )


def mock_repo_endpoints(pulls, issues, commits, reviews=None) -> None:  # type: ignore[no-untyped-def]
    responses.get(f"{API}/repos/{gp.REPO}", json=gp.repository())
    responses.get(f"{API}/repos/{gp.REPO}/commits", json=commits)
    responses.get(f"{API}/repos/{gp.REPO}/pulls", json=pulls)
    responses.get(f"{API}/repos/{gp.REPO}/issues", json=issues)
    for number, body in (reviews or {}).items():
        responses.get(f"{API}/repos/{gp.REPO}/pulls/{number}/reviews", json=body)


@responses.activate
def test_first_run_uses_lookback_window(client: GitHubClient, tmp_path: Path) -> None:
    mock_repo_endpoints([], [], [gp.commit(1, "2024-06-10T10:00:00Z", gp.ALICE)])
    manifest = make_extractor(client, tmp_path).run([gp.REPO])

    commits_call = next(c for c in responses.calls if "/commits" in c.request.url)
    assert "since=2024-05-16T00%3A00%3A00Z" in commits_call.request.url
    result = manifest["entities"]["commits"][gp.REPO]
    assert result["records"] == 1
    assert result["new_watermark"] == "2024-06-10T10:00:00Z"


@responses.activate
def test_incremental_run_starts_from_watermark_minus_overlap(
    client: GitHubClient, tmp_path: Path
) -> None:
    mock_repo_endpoints([], [], [])
    wm = {
        (gp.REPO, "commits"): datetime(2024, 6, 12, 8, 0, tzinfo=UTC),
        (gp.REPO, "issues"): datetime(2024, 6, 12, 8, 0, tzinfo=UTC),
    }
    manifest = make_extractor(client, tmp_path, wm).run([gp.REPO])

    urls = [c.request.url for c in responses.calls]
    assert any("/commits" in u and "since=2024-06-12T07%3A00%3A00Z" in u for u in urls)
    assert any("/issues" in u and "since=2024-06-12T07%3A00%3A00Z" in u for u in urls)
    # nothing new -> watermark stays where it was
    assert manifest["entities"]["commits"][gp.REPO]["new_watermark"] == "2024-06-12T08:00:00Z"


@responses.activate
def test_pull_requests_stop_paging_at_watermark(client: GitHubClient, tmp_path: Path) -> None:
    pulls = [
        gp.pull_request(3, 303, "2024-06-01T00:00:00Z", "2024-06-14T00:00:00Z", gp.ALICE),
        gp.pull_request(
            2,
            302,
            "2024-05-01T00:00:00Z",
            "2024-06-13T00:00:00Z",
            gp.BOB,
            merged="2024-06-13T00:00:00Z",
        ),
        gp.pull_request(1, 301, "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", gp.BOB),
    ]
    reviews = {3: [gp.review(1, gp.BOB, "APPROVED", "2024-06-14T00:00:00Z")], 2: []}
    mock_repo_endpoints(pulls, [], [], reviews)
    wm = {(gp.REPO, "pull_requests"): datetime(2024, 6, 10, tzinfo=UTC)}
    manifest = make_extractor(client, tmp_path, wm).run([gp.REPO])

    pr = manifest["entities"]["pull_requests"][gp.REPO]
    assert pr["records"] == 2  # PR 1 is older than the window
    assert pr["new_watermark"] == "2024-06-14T00:00:00Z"
    assert manifest["entities"]["reviews"][gp.REPO]["records"] == 1
    review_urls = [c.request.url for c in responses.calls if "/reviews" in c.request.url]
    assert len(review_urls) == 2  # no review call for the out-of-window PR


@responses.activate
def test_issues_endpoint_pull_requests_are_filtered(client: GitHubClient, tmp_path: Path) -> None:
    issues = [
        gp.issue(10, 910, "2024-06-01T00:00:00Z", "2024-06-02T00:00:00Z", gp.ALICE),
        gp.issue(11, 911, "2024-06-01T00:00:00Z", "2024-06-03T00:00:00Z", gp.ALICE, is_pr=True),
    ]
    mock_repo_endpoints([], issues, [])
    manifest = make_extractor(client, tmp_path).run([gp.REPO])
    assert manifest["entities"]["issues"][gp.REPO]["records"] == 1
    assert manifest["entities"]["issues"][gp.REPO]["new_watermark"] == "2024-06-02T00:00:00Z"


@responses.activate
def test_empty_repository_409_is_tolerated(client: GitHubClient, tmp_path: Path) -> None:
    responses.get(f"{API}/repos/{gp.REPO}", json=gp.repository())
    responses.get(
        f"{API}/repos/{gp.REPO}/commits", status=409, json={"message": "Git Repository is empty."}
    )
    responses.get(f"{API}/repos/{gp.REPO}/pulls", json=[])
    responses.get(f"{API}/repos/{gp.REPO}/issues", json=[])
    manifest = make_extractor(client, tmp_path).run([gp.REPO])
    assert manifest["record_counts"]["commits"] == 0


@responses.activate
def test_truncated_listing_does_not_advance_watermark(tmp_path: Path) -> None:
    client = GitHubClient(token="t", base_url=API, max_pages=1, sleep=lambda _s: None)
    responses.get(f"{API}/repos/{gp.REPO}", json=gp.repository())
    responses.get(
        f"{API}/repos/{gp.REPO}/commits",
        json=[gp.commit(1, "2024-06-14T00:00:00Z", gp.ALICE)],
        headers={"Link": f'<{API}/repos/{gp.REPO}/commits?page=2>; rel="next"'},
    )
    responses.get(f"{API}/repos/{gp.REPO}/pulls", json=[])
    responses.get(f"{API}/repos/{gp.REPO}/issues", json=[])
    previous = datetime(2024, 6, 1, tzinfo=UTC)
    manifest = make_extractor(client, tmp_path, {(gp.REPO, "commits"): previous}).run([gp.REPO])
    result = manifest["entities"]["commits"][gp.REPO]
    assert result["truncated"] is True
    assert result["new_watermark"] == "2024-06-01T00:00:00Z"


@responses.activate
def test_rerun_of_same_run_id_does_not_duplicate_raw_lines(
    client: GitHubClient, tmp_path: Path
) -> None:
    mock_repo_endpoints([], [], [gp.commit(1, "2024-06-10T10:00:00Z", gp.ALICE)])
    make_extractor(client, tmp_path).run([gp.REPO])
    make_extractor(client, tmp_path).run([gp.REPO])  # e.g. Airflow task retry
    assert len(list(RawStore(tmp_path, "run1").read("commits"))) == 1


@responses.activate
def test_discovery_excludes_forks(client: GitHubClient, tmp_path: Path) -> None:
    responses.get(
        f"{API}/orgs/acme/repos",
        json=[{"full_name": "acme/a", "fork": False}, {"full_name": "acme/b", "fork": True}],
    )
    assert make_extractor(client, tmp_path).discover_repositories("acme", "org") == ["acme/a"]
