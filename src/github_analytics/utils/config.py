"""Settings from environment variables plus an optional YAML file (config/pipeline.yml).

Precedence: environment variable > YAML value > default. Secrets (the GitHub
token, database password) are only read from the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class ConfigError(ValueError):
    pass


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a mapping")
    return data


def _resolve(path: Any) -> Path:
    p = Path(path)
    return p if p.is_absolute() else PROJECT_ROOT / p


def _split_csv(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else value.split(",")
    return [i.strip() for i in items if i and i.strip()]


@dataclass(frozen=True)
class Settings:
    github_token: str | None = field(repr=False)
    github_api_url: str
    repositories: list[str]
    discover_owner: str | None
    discover_owner_type: str
    include_forks: bool
    initial_lookback_days: int
    watermark_overlap_minutes: int
    max_pages: int
    request_timeout_seconds: float
    max_retries: int
    max_rate_limit_wait_seconds: float
    database_url: str | None = field(repr=False)
    data_dir: Path
    reports_dir: Path
    max_invalid_ratio: float

    @classmethod
    def load(cls, config_path: Path | None = None, **overrides: Any) -> Settings:
        path = config_path or Path(
            os.environ.get("PIPELINE_CONFIG", PROJECT_ROOT / "config" / "pipeline.yml")
        )
        cfg = _load_yaml(path)
        gh = cfg.get("github", {})
        extraction = cfg.get("extraction", {})
        quality = cfg.get("quality", {})

        def pick(env: str, section: dict[str, Any], key: str, default: Any) -> Any:
            if env in os.environ and os.environ[env] != "":
                return os.environ[env]
            return section.get(key, default)

        owner_type = str(pick("GITHUB_OWNER_TYPE", gh, "owner_type", "org")).lower()
        if owner_type not in {"org", "user"}:
            raise ConfigError("GITHUB_OWNER_TYPE must be 'org' or 'user'")

        settings = cls(
            github_token=os.environ.get("GITHUB_TOKEN") or None,
            github_api_url=str(pick("GITHUB_API_URL", gh, "api_url", "https://api.github.com")),
            repositories=_split_csv(pick("GITHUB_REPOSITORIES", gh, "repositories", None)),
            discover_owner=pick("GITHUB_OWNER", gh, "owner", None) or None,
            discover_owner_type=owner_type,
            include_forks=str(pick("GITHUB_INCLUDE_FORKS", gh, "include_forks", False)).lower()
            in {"1", "true", "yes"},
            initial_lookback_days=int(
                pick("GITHUB_INITIAL_LOOKBACK_DAYS", extraction, "initial_lookback_days", 30)
            ),
            watermark_overlap_minutes=int(
                pick("WATERMARK_OVERLAP_MINUTES", extraction, "watermark_overlap_minutes", 60)
            ),
            max_pages=int(pick("GITHUB_MAX_PAGES", extraction, "max_pages", 50)),
            request_timeout_seconds=float(
                pick("GITHUB_TIMEOUT_SECONDS", extraction, "request_timeout_seconds", 30)
            ),
            max_retries=int(pick("GITHUB_MAX_RETRIES", extraction, "max_retries", 5)),
            max_rate_limit_wait_seconds=float(
                pick(
                    "GITHUB_MAX_RATE_LIMIT_WAIT_SECONDS",
                    extraction,
                    "max_rate_limit_wait_seconds",
                    900,
                )
            ),
            database_url=os.environ.get("ANALYTICS_DATABASE_URL") or None,
            data_dir=_resolve(pick("DATA_DIR", cfg, "data_dir", "data")),
            reports_dir=_resolve(pick("REPORTS_DIR", cfg, "reports_dir", "reports")),
            max_invalid_ratio=float(pick("MAX_INVALID_RATIO", quality, "max_invalid_ratio", 0.05)),
        )
        if overrides:
            settings = cls(**{**settings.__dict__, **overrides})
        return settings

    def require_database(self) -> str:
        if not self.database_url:
            raise ConfigError("ANALYTICS_DATABASE_URL is not set")
        return self.database_url
