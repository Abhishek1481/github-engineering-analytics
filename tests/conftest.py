from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from github_analytics.extract.client import GitHubClient


class FakeClock:
    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(clock: FakeClock) -> GitHubClient:
    return GitHubClient(
        token="test-token",
        base_url="https://api.github.test",
        max_retries=3,
        sleep=clock.sleep,
        clock=clock.time,
        max_pages=5,
    )
