# Resume bullets: GitHub Engineering Activity Analytics

Every bullet describes something implemented and tested in this repository. There are **no invented performance numbers**: no benchmarks were run. The only figures come from runs recorded in this project's `docs/example-output.md`, and they describe small demo runs, not production workloads. Label these as personal/portfolio projects, and adjust the tense and wording to your own resume style.

## GitHub Engineering Activity Analytics (Python, GitHub REST API, Apache Airflow, PostgreSQL, SQL)

- Developed an Airflow 3 pipeline (extract → validate → transform → load → quality check → analytics) that ingests GitHub commits, pull requests, reviews and issues into a PostgreSQL star schema with conformed date, repository and contributor dimensions.
- Implemented incremental extraction with per-repository watermarks, committed in the same transaction as the data load, and an overlap window. A second run on a live repository needed 5 API requests instead of 14 by fetching only changed records.
- Built a resilient REST client handling Link-header pagination, primary and secondary GitHub rate limits, jittered exponential retries, and protection against advancing watermarks past truncated result sets.
- Wrote analytical SQL with CTEs, window functions and percentile aggregates for PR cycle time (median/p90), time to first review, weekly throughput with moving averages, and issue backlog trends.
- Added contract validation with quarantine and post-load SQL quality checks. An integration test showed that re-running a load is idempotent, and it also caught and fixed a pandas nullable-integer bug.
