from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fixtures import github_payloads as gp
from github_analytics.extract.raw_store import RawRecord, RawStore
from github_analytics.transform import transformers as tf
from github_analytics.transform.validate import RawValidationError, validate_record, validate_run


def rec(entity: str, data: dict, **context: object) -> RawRecord:  # type: ignore[type-arg]
    return RawRecord(gp.REPO, entity, data, dict(context), "2024-06-15T00:00:00+00:00")


def test_commit_prefers_linked_github_user() -> None:
    df, people = tf.transform_commits(
        [rec("commits", gp.commit(1, "2024-06-10T10:00:00Z", gp.ALICE, parents=2))]
    )
    row = df.iloc[0]
    assert row["author_nk"] == "gh:alice"
    assert row["parent_count"] == 2
    assert row["message_headline"] == "Fix widget alignment"
    assert row["committed_at"] == pd.Timestamp("2024-06-10T10:00:00Z")
    assert people[0]["github_user_id"] == 11


def test_unlinked_commit_author_gets_hashed_email_key() -> None:
    df, people = tf.transform_commits(
        [
            rec(
                "commits",
                gp.commit(
                    2,
                    "2024-06-10T10:00:00Z",
                    None,
                    git_name="Old Laptop",
                    git_email="Me@Example.com",
                ),
            )
        ]
    )
    nk = df.iloc[0]["author_nk"]
    assert nk.startswith("git:")
    assert "example" not in nk.lower()  # the email itself is not stored
    assert people[0]["user_type"] == "Unlinked"
    # stable across case differences
    _, again = tf.transform_commits(
        [rec("commits", gp.commit(3, "2024-06-10T10:00:00Z", None, git_email="me@example.com"))]
    )
    assert again[0]["contributor_nk"] == nk


def test_bots_are_flagged() -> None:
    assert tf.user_contributor(gp.DEPENDABOT)["is_bot"] is True  # type: ignore[index]
    assert tf.user_contributor(gp.ALICE)["is_bot"] is False  # type: ignore[index]
    assert tf.user_contributor(None) is None


def test_pull_request_fields() -> None:
    pr = gp.pull_request(
        5,
        505,
        "2024-06-01T00:00:00Z",
        "2024-06-03T00:00:00Z",
        gp.BOB,
        merged="2024-06-02T12:00:00Z",
        draft=True,
    )
    df, _ = tf.transform_pull_requests([rec("pull_requests", pr)])
    row = df.iloc[0]
    assert (row["state"], row["is_draft"], row["base_ref"]) == ("closed", True, "main")
    assert row["merged_at"] == pd.Timestamp("2024-06-02T12:00:00Z")
    assert row["closed_at"] == row["merged_at"]


def test_pending_reviews_are_dropped() -> None:
    df, _ = tf.transform_reviews(
        [
            rec(
                "reviews", gp.review(1, gp.ALICE, "APPROVED", "2024-06-02T00:00:00Z"), pull_number=5
            ),
            rec("reviews", gp.review(2, gp.ALICE, "PENDING", None), pull_number=5),
        ]
    )
    assert list(df["github_review_id"]) == [1]
    assert df.iloc[0]["pr_number"] == 5


def test_issue_labels_and_duplicates() -> None:
    older = gp.issue(7, 707, "2024-06-01T00:00:00Z", "2024-06-02T00:00:00Z", gp.ALICE)
    newer = gp.issue(
        7,
        707,
        "2024-06-01T00:00:00Z",
        "2024-06-05T00:00:00Z",
        gp.ALICE,
        closed="2024-06-05T00:00:00Z",
        labels=("bug", "p1"),
    )
    df, _ = tf.transform_issues([rec("issues", older), rec("issues", newer)])
    assert len(df) == 1
    assert df.iloc[0]["state"] == "closed"
    assert df.iloc[0]["labels"] == ["bug", "p1"]


def test_build_contributors_dedupes_preferring_github_id() -> None:
    people = [
        {
            "contributor_nk": "gh:alice",
            "github_user_id": None,
            "login": "alice",
            "display_name": "alice",
            "user_type": "User",
            "is_bot": False,
        },
        tf.user_contributor(gp.ALICE),
    ]
    df = tf.build_contributors(people)  # type: ignore[arg-type]
    assert len(df) == 1
    assert df.iloc[0]["github_user_id"] == 11


def test_empty_inputs_produce_typed_empty_frames() -> None:
    df, people = tf.transform_commits([])
    assert list(df.columns) == tf.COLUMNS["commits"]
    assert people == []
    assert tf.build_contributors([]).empty


@pytest.mark.parametrize(
    ("entity", "data", "problem"),
    [
        ("commits", {**gp.commit(1, "2024-06-10T10:00:00Z", None), "sha": "xyz"}, "sha"),
        (
            "pull_requests",
            {
                **gp.pull_request(1, 1, "2024-06-01T00:00:00Z", "2024-06-01T00:00:00Z", None),
                "state": "merged",
            },
            "state",
        ),
        ("issues", {**gp.issue(1, 1, "not-a-date", "2024-06-01T00:00:00Z", None)}, "created_at"),
        ("repositories", {**gp.repository(), "id": "1001"}, "id"),
    ],
)
def test_validation_detects_contract_violations(entity: str, data: dict, problem: str) -> None:  # type: ignore[type-arg]
    problems = validate_record(rec(entity, data))
    assert problems and problems[0].startswith(problem)


def test_review_needs_pull_number_context() -> None:
    assert "context.pull_number missing" in validate_record(
        rec("reviews", gp.review(1, gp.ALICE, "APPROVED", "2024-06-01T00:00:00Z"))
    )


def test_validate_run_quarantines_and_enforces_ratio(tmp_path: Path) -> None:
    store = RawStore(tmp_path, "r")
    good = [
        gp.issue(i, i, "2024-06-01T00:00:00Z", "2024-06-01T00:00:00Z", gp.ALICE)
        for i in range(1, 20)
    ]
    bad = {**good[0], "id": None}
    store.write("issues", gp.REPO, [*good, bad])

    summary = validate_run(store, max_invalid_ratio=0.10)
    assert summary["issues"] == {
        "checked": 20,
        "invalid": 1,
        "invalid_ratio": 0.05,
        "reasons": {"id": 1},
    }
    assert (store.root / "quarantine" / "issues.jsonl").exists()

    with pytest.raises(RawValidationError):
        validate_run(store, max_invalid_ratio=0.01)
