"""``gh-analytics``: run the pipeline (or single steps) without Airflow.

gh-analytics init-db
gh-analytics run [--run-id ID]
gh-analytics extract|validate|transform|load|quality|report --run-id ID
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from github_analytics import pipeline
from github_analytics.load.postgres import PostgresLoader
from github_analytics.utils.config import Settings
from github_analytics.utils.logging_setup import configure_logging

STEPS = {
    "extract": pipeline.extract,
    "validate": pipeline.validate,
    "transform": pipeline.transform,
    "load": pipeline.load,
    "quality": pipeline.quality,
    "report": pipeline.report,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=["init-db", "run", *STEPS])
    parser.add_argument("--run-id", help="run identifier (required for single steps)")
    args = parser.parse_args(argv)

    configure_logging()
    settings = Settings.load()
    if args.command == "init-db":
        PostgresLoader(settings.require_database()).init_schema()
        return 0
    result: Any
    if args.command == "run":
        result = pipeline.run_all(settings, args.run_id)
    else:
        if not args.run_id:
            parser.error(f"{args.command} requires --run-id")
        result = STEPS[args.command](settings, pipeline.safe_run_id(args.run_id))
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
