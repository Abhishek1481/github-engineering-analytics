"""DAG structure tests. Require Apache Airflow (installed in the CI 'airflow' job)."""

from __future__ import annotations

import importlib.util
import itertools
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow.sdk")
pytestmark = pytest.mark.integration

DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "github_analytics_dag.py"


@pytest.fixture(scope="module")
def dag() -> Any:
    spec = importlib.util.spec_from_file_location("github_analytics_dag", DAG_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag_object


def test_dag_loads_with_expected_tasks(dag: Any) -> None:
    assert dag.dag_id == "github_engineering_analytics"
    assert set(dag.task_ids) == {
        "extract",
        "validate",
        "transform",
        "load",
        "quality_check",
        "analytics_report",
    }


def test_linear_dependencies(dag: Any) -> None:
    order = ["extract", "validate", "transform", "load", "quality_check", "analytics_report"]
    for upstream, downstream in itertools.pairwise(order):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_operational_defaults(dag: Any) -> None:
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.get_task("extract").retries == 3
    assert "repositories" in dag.params


def test_dag_file_contains_no_credentials() -> None:
    text = DAG_FILE.read_text(encoding="utf-8").lower()
    assert "ghp_" not in text and "github_pat_" not in text
    assert "password" not in text
