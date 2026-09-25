from __future__ import annotations

import pytest
import requests
import responses

from conftest import FakeClock
from github_analytics.extract.client import (
    GitHubAPIError,
    GitHubClient,
    NotFoundError,
    RateLimitError,
)

API = "https://api.github.test"


@responses.activate
def test_sends_auth_and_version_headers(client: GitHubClient) -> None:
    responses.get(f"{API}/repos/a/b", json={"id": 1})
    assert client.get_json("/repos/a/b") == {"id": 1}
    sent = responses.calls[0].request.headers
    assert sent["Authorization"] == "Bearer test-token"
    assert sent["X-GitHub-Api-Version"] == "2022-11-28"


def test_token_hidden_from_repr(client: GitHubClient) -> None:
    assert "test-token" not in repr(client)


@responses.activate
def test_paginates_by_link_header(client: GitHubClient) -> None:
    responses.get(
        f"{API}/items",
        json=[{"n": 1}, {"n": 2}],
        headers={"Link": f'<{API}/items?page=2>; rel="next"'},
    )
    responses.get(f"{API}/items?page=2", json=[{"n": 3}])
    pager = client.paginate("/items", {"state": "all"})
    assert [i["n"] for i in pager] == [1, 2, 3]
    assert not pager.truncated
    assert "per_page=100" in responses.calls[0].request.url
    assert "state=all" in responses.calls[0].request.url


@responses.activate
def test_pagination_stops_at_max_pages_and_flags_truncation(client: GitHubClient) -> None:
    for page in range(1, 8):
        responses.get(
            f"{API}/items?page={page}" if page > 1 else f"{API}/items",
            json=[{"n": page}],
            headers={"Link": f'<{API}/items?page={page + 1}>; rel="next"'},
        )
    pager = client.paginate("/items")
    assert len(list(pager)) == 5
    assert pager.truncated


@responses.activate
def test_retries_server_errors_with_backoff(client: GitHubClient, clock: FakeClock) -> None:
    responses.get(f"{API}/x", status=502)
    responses.get(f"{API}/x", status=503)
    responses.get(f"{API}/x", json={"ok": True})
    assert client.get_json("/x") == {"ok": True}
    assert len(clock.sleeps) == 2
    assert clock.sleeps[1] > clock.sleeps[0] * 0.9  # exponential (with jitter)
    assert client.stats.retries == 2


@responses.activate
def test_retries_network_errors_then_gives_up(client: GitHubClient) -> None:
    responses.get(f"{API}/x", body=requests.ConnectionError("reset"))
    with pytest.raises(GitHubAPIError, match="network error after 4 attempts"):
        client.get_json("/x")


@responses.activate
def test_timeout_is_retried(client: GitHubClient) -> None:
    responses.get(f"{API}/x", body=requests.Timeout("slow"))
    responses.get(f"{API}/x", json=[])
    assert client.get_json("/x") == []


@responses.activate
def test_waits_for_primary_rate_limit_reset(client: GitHubClient, clock: FakeClock) -> None:
    reset = int(clock.now) + 120
    responses.get(
        f"{API}/x",
        status=403,
        json={"message": "API rate limit exceeded"},
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset)},
    )
    responses.get(f"{API}/x", json={"ok": 1}, headers={"X-RateLimit-Remaining": "4999"})
    assert client.get_json("/x") == {"ok": 1}
    assert clock.sleeps == [121.0]
    assert client.stats.rate_limit_waits == 1
    assert client.stats.rate_limit_remaining == 4999


@responses.activate
def test_honours_retry_after_for_secondary_limit(client: GitHubClient, clock: FakeClock) -> None:
    responses.get(f"{API}/x", status=429, headers={"Retry-After": "30"})
    responses.get(f"{API}/x", json={})
    client.get_json("/x")
    assert clock.sleeps == [30.0]


@responses.activate
def test_refuses_to_wait_longer_than_allowed(clock: FakeClock) -> None:
    client = GitHubClient(
        token=None,
        base_url=API,
        sleep=clock.sleep,
        clock=clock.time,
        max_rate_limit_wait_seconds=60,
    )
    responses.get(
        f"{API}/x",
        status=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(clock.now) + 3600)},
    )
    with pytest.raises(RateLimitError):
        client.get_json("/x")
    assert clock.sleeps == []


@responses.activate
def test_permission_error_is_not_retried(client: GitHubClient) -> None:
    responses.get(
        f"{API}/x",
        status=403,
        json={"message": "Resource not accessible"},
        headers={"X-RateLimit-Remaining": "4000"},
    )
    with pytest.raises(GitHubAPIError) as err:
        client.get_json("/x")
    assert err.value.status == 403
    assert len(responses.calls) == 1


@responses.activate
def test_not_found(client: GitHubClient) -> None:
    responses.get(f"{API}/x", status=404)
    with pytest.raises(NotFoundError):
        client.get_json("/x")
