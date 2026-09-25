"""GitHub REST API client: authentication, pagination, rate limits and retries.

Retry policy
  * 5xx, 408, connection errors, timeouts   exponential backoff with jitter
  * 403/429 with X-RateLimit-Remaining: 0     sleep until X-RateLimit-Reset
  * 403/429 with Retry-After                 sleep Retry-After (secondary limit)
  * other 4xx                                raise immediately (not retryable)
If the required wait exceeds ``max_rate_limit_wait_seconds`` the client raises
``RateLimitError`` rather than blocking an Airflow worker for an hour; the
task's own retry schedule takes over.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

API_VERSION = "2022-11-28"
RETRYABLE_STATUS = frozenset({408, 500, 502, 503, 504})


class GitHubAPIError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class NotFoundError(GitHubAPIError):
    pass


class RateLimitError(GitHubAPIError):
    pass


@dataclass
class RequestStats:
    requests: int = 0
    retries: int = 0
    rate_limit_waits: int = 0
    rate_limit_remaining: int | None = None


@dataclass
class GitHubClient:
    token: str | None = field(repr=False, default=None)
    base_url: str = "https://api.github.com"
    timeout: float = 30.0
    max_retries: int = 5
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    max_rate_limit_wait_seconds: float = 900.0
    max_pages: int = 50
    session: requests.Session = field(default_factory=requests.Session)
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.time
    stats: RequestStats = field(default_factory=RequestStats)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "github-engineering-analytics",
            }
        )
        if self.token:
            self.session.headers["Authorization"] = f"Bearer {self.token}"
        else:
            logger.warning("GITHUB_TOKEN not set: unauthenticated requests are limited to 60/hour")

    # ------------------------------------------------------------------ core
    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http"):
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    def _backoff(self, attempt: int) -> float:
        delay = min(self.backoff_max_seconds, self.backoff_base_seconds * 2 ** (attempt - 1))
        return float(delay * (0.5 + random.random() / 2))  # noqa: S311 - jitter, not crypto

    def _rate_limit_wait(self, response: requests.Response) -> float | None:
        """Seconds to wait if the response is a rate-limit rejection, else None."""
        if response.status_code not in (403, 429):
            return None
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            return max(float(retry_after), 1.0)
        if response.headers.get("X-RateLimit-Remaining") == "0":
            reset = float(response.headers.get("X-RateLimit-Reset", self.clock() + 60))
            return max(reset - self.clock(), 0.0) + 1.0
        return None

    def _wait_for_rate_limit(self, seconds: float, url: str) -> None:
        if seconds > self.max_rate_limit_wait_seconds:
            raise RateLimitError(
                f"rate limit resets in {seconds:.0f}s (> {self.max_rate_limit_wait_seconds:.0f}s"
                f" allowed) for {url}",
                status=403,
            )
        self.stats.rate_limit_waits += 1
        logger.warning("rate limited, waiting", extra={"wait_seconds": round(seconds, 1)})
        self.sleep(seconds)

    def request(self, path_or_url: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = self._url(path_or_url)
        attempt = 0
        while True:
            attempt += 1
            self.stats.requests += 1
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt > self.max_retries:
                    raise GitHubAPIError(f"network error after {attempt} attempts: {exc}") from exc
                self._retry(attempt, url, f"network error: {exc.__class__.__name__}")
                continue

            remaining = response.headers.get("X-RateLimit-Remaining")
            if remaining is not None and remaining.isdigit():
                self.stats.rate_limit_remaining = int(remaining)

            if response.ok:
                return response

            wait = self._rate_limit_wait(response)
            if wait is not None:
                if attempt > self.max_retries:
                    raise RateLimitError(
                        f"still rate limited after {attempt} attempts", status=response.status_code
                    )
                self._wait_for_rate_limit(wait, url)
                continue
            if response.status_code in RETRYABLE_STATUS and attempt <= self.max_retries:
                self._retry(attempt, url, f"HTTP {response.status_code}")
                continue
            if response.status_code == 404:
                raise NotFoundError(f"not found: {url}", status=404)
            raise GitHubAPIError(
                f"HTTP {response.status_code} for {url}: {response.text[:300]}",
                status=response.status_code,
            )

    def _retry(self, attempt: int, url: str, reason: str) -> None:
        delay = self._backoff(attempt)
        self.stats.retries += 1
        logger.warning(
            "retrying request",
            extra={
                "url": url,
                "attempt": attempt,
                "reason": reason,
                "wait_seconds": round(delay, 2),
            },
        )
        self.sleep(delay)

    # --------------------------------------------------------------- helpers
    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self.request(path, params).json()

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Pager:
        """Iterate items across pages by following the ``Link: rel="next"`` header."""
        return Pager(self, path, params)


class Pager:
    """Iterable over a paginated endpoint.

    After iteration, ``truncated`` tells whether ``max_pages`` cut the listing
    short. Extractors use it to avoid advancing a watermark past data that
    was never read.
    """

    def __init__(self, client: GitHubClient, path: str, params: dict[str, Any] | None) -> None:
        self.client = client
        self.path = path
        self.params = params
        self.truncated = False
        self.pages = 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        url: str | None = self.client._url(self.path)
        query: dict[str, Any] | None = {"per_page": 100, **(self.params or {})}
        while url:
            self.pages += 1
            response = self.client.request(url, query)
            items = response.json()
            if not isinstance(items, list):
                raise GitHubAPIError(f"expected a JSON list from {url}")
            yield from items
            url = response.links.get("next", {}).get("url")
            query = None  # the "next" URL already carries the query string
            if url and self.pages >= self.client.max_pages:
                self.truncated = True
                logger.warning(
                    "max_pages reached; remaining pages skipped",
                    extra={"path": self.path, "max_pages": self.client.max_pages},
                )
                return
