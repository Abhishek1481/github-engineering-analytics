# GitHub Engineering Activity Analytics

An **incremental ETL/ELT pipeline**. It pulls commits, pull requests, reviews and issues from the **GitHub REST API**, lands the raw JSON, validates and transforms it with **Python/pandas**, and loads a **PostgreSQL star schema**. SQL on top of that schema answers engineering-metrics questions such as PR cycle time, review turnaround, weekly throughput and issue backlog. **Apache Airflow** orchestrates the whole run.

> **Status: local development / demonstration environment.**
>
> - The pipeline was run end to end on the build machine against the **live GitHub API** and a local PostgreSQL 16. The output is in [docs/example-output.md](docs/example-output.md).
> - The Airflow DAG and Docker Compose stack are written but were **not executed locally**: Airflow does not run on native Windows, and Docker wasn't installed.
> - The DAG structure tests run in CI on Linux.

---

## 1. Project overview

Engineering leaders ask:

- How long do PRs take to merge?
- Are reviews a bottleneck?
- Is the issue backlog growing?
- Who is carrying the review load?

GitHub's UI answers these per repository, one screen at a time. This project extracts the activity on a schedule, models it dimensionally, and makes those questions a SQL query or a CSV away.

## 2. Business value

- **Delivery metrics** (cycle time, time to first review, throughput) for retrospectives and planning. These are DORA-adjacent signals.
- **Bottleneck detection:** the share of PRs reviewed, and review turnaround per repository.
- **Backlog health:** issues opened vs closed each week, a running open count, and time to close.
- **A single model across many repositories**, instead of per-repo dashboards.
- **Low API cost:** incremental extraction only asks GitHub for what changed since the last successful load.

## 3. Architecture

```mermaid
flowchart LR
    GH[(GitHub REST API)] -->|"paginated, rate-limit aware,<br/>incremental (since / updated_at)"| EX[Python extractor]
    WM[("etl.extraction_watermarks")] -.->|since = watermark - overlap| EX
    EX --> RAW[/"Raw zone<br/>JSONL + manifest"/]
    RAW --> VAL{Validate<br/>contract checks}
    VAL -->|invalid| Q[/quarantine/]
    VAL --> TR["Transform<br/>pandas -> Parquet"]
    TR -->|COPY| STG[("stg.* (unlogged)")]
    STG -->|dimensions.sql / facts.sql upserts| DW[("dw star schema")]
    DW --> QC{Quality checks}
    QC --> AN["analytics.sql<br/>CSV + Markdown report"]
    DW -.->|advance watermarks<br/>same transaction| WM
    subgraph Airflow DAG
      EX
      VAL
      TR
      STG
      QC
      AN
    end
```

### Star schema

```mermaid
erDiagram
    dim_repository ||--o{ fact_commit : ""
    dim_repository ||--o{ fact_pull_request : ""
    dim_repository ||--o{ fact_review : ""
    dim_repository ||--o{ fact_issue : ""
    dim_contributor ||--o{ fact_commit : "author / committer"
    dim_contributor ||--o{ fact_pull_request : author
    dim_contributor ||--o{ fact_review : reviewer
    dim_contributor ||--o{ fact_issue : author
    dim_date ||--o{ fact_commit : "authored / committed"
    dim_date ||--o{ fact_pull_request : "created / closed / merged"
    dim_date ||--o{ fact_review : submitted
    dim_date ||--o{ fact_issue : "created / closed"
    fact_pull_request ||--o{ fact_review : ""
```

| Table | Grain | Key |
|---|---|---|
| `dim_date` | one calendar day, 2008 to 2035 | `date_key` (YYYYMMDD) |
| `dim_repository` | one repository (SCD Type 1) | identity surrogate; natural key `github_repo_id` |
| `dim_contributor` | one person or bot | identity surrogate; natural key `gh:<login>` or `git:<sha256(email)[:16]>` for commit authors without a GitHub account |
| `fact_commit` | one commit in one repository | (`repository_key`, `commit_sha`) |
| `fact_pull_request` | one PR, updated in place as it progresses (accumulating snapshot) | GitHub PR id |
| `fact_review` | one submitted review | GitHub review id |
| `fact_issue` | one issue, updated in place | GitHub issue id |

`cycle_time_hours`, `time_to_close_hours`, `is_merged` and `is_merge_commit` are **generated columns**, so every consumer gets the same definition. Indexes follow the two dominant access paths: *repository over time* (`repository_key, *_date_key`) and *per person* (`author_key`, `reviewer_key`). Partial indexes cover merged PRs and open issues.

## 4. Technology stack

| Technology | Purpose |
|---|---|
| Python 3.11, `requests` | GitHub client: auth, pagination via `Link` headers, retries, rate limits |
| pandas + PyArrow | Typed transformation. Parquet hand-off between the transform and load tasks. |
| PostgreSQL 16 | Staging, star schema, ETL metadata (watermarks, run log) |
| psycopg 3 | `COPY` bulk loading and transactional upserts |
| Apache Airflow 3.3 | Scheduling, retries with exponential backoff, dependencies, run history |
| SQL | Dimensional upserts; analytics with CTEs, window functions, `PERCENTILE_CONT`, `FILTER`, `FULL OUTER JOIN` |
| Docker Compose | Local Airflow + PostgreSQL |
| pytest, `responses`, ruff, mypy (strict), uv | Tests with mocked HTTP, linting, type checking, locked dependencies |

## 5. Data flow

1. **Extract** (`extract/`)
   - Resolve repositories from `GITHUB_REPOSITORIES`, or discover every repository of `GITHUB_OWNER`.
   - For each repository, read its watermarks and request only newer data:
     - commits: `since=`
     - issues: `since=` and `state=all` (PRs returned by the issues endpoint are filtered out)
     - pull requests: sorted by `updated desc`, paging stops at the first PR older than the window
     - reviews: only for PRs in the window
   - The window starts at `watermark - WATERMARK_OVERLAP_MINUTES` (default 60) to absorb clock skew and late updates.
   - Raw payloads are written untouched to `data/raw/<run_id>/<entity>/<repo>.jsonl` with lineage fields (`repository`, `extracted_at`, `context`), plus a `manifest.json` with counts, windows, proposed new watermarks and API request stats.
2. **Validate** (`transform/validate.py`): contract checks per entity (types, required fields, enums, ISO timestamps, SHA format). Invalid records go to `quarantine/` with reasons. If an entity's invalid share exceeds `MAX_INVALID_RATIO` (5%), the step fails.
3. **Transform** (`transform/transformers.py`)
   - Map raw objects to the six staging tables.
   - Derive contributors from every actor. A commit author without a GitHub account is keyed by a **hash** of their email; the address itself isn't stored.
   - Flag bots, drop `PENDING` reviews and de-duplicate.
   - Write Parquet to `data/staged/<run_id>/`.
4. **Load** (`load/postgres.py`), **one transaction**:
   - `TRUNCATE stg.*`
   - `COPY` the Parquet data into staging
   - run `sql/dimensions.sql` then `sql/facts.sql` (upserts)
   - advance the watermarks from the manifest

   If any part fails, nothing is visible and the watermarks don't move.
5. **Quality check** (`load/quality.py`): ten SQL checks. Examples: every staged row reached its fact table; no PR merged before it was created; a merged PR is closed; reviews belong to known PRs. `error`-severity failures stop the DAG.
6. **Analytics** (`analytics/report.py`): runs the ten named queries in `sql/analytics.sql` and writes CSV files and `report.md` to `reports/<run_id>/` and `reports/latest/`. Each run's counts, checks and status are recorded in `etl.pipeline_runs`.

## 6. Installation

```bash
git clone <your-fork-url> data-engineering-portfolio
cd data-engineering-portfolio/project-2-github-engineering-analytics
uv sync
```

You need a PostgreSQL database for the pipeline. Either start the Compose stack (next section) or point `ANALYTICS_DATABASE_URL` at any PostgreSQL 13+.

## 7. Configuration

Non-secret defaults live in [`config/pipeline.yml`](config/pipeline.yml). Environment variables override them. Secrets are **only** read from the environment:

```bash
cp .env.example .env
```

| Variable | Meaning |
|---|---|
| `GITHUB_TOKEN` | Fine-grained, read-only token. Optional for public repos (60 requests/h without it, 5,000/h with it). |
| `GITHUB_REPOSITORIES` / `GITHUB_OWNER`, `GITHUB_OWNER_TYPE` | Explicit repository list, or discover all repositories of an org or user |
| `GITHUB_INITIAL_LOOKBACK_DAYS` | History to fetch the first time a repository or entity is seen |
| `WATERMARK_OVERLAP_MINUTES`, `GITHUB_MAX_PAGES`, `GITHUB_MAX_RETRIES`, `GITHUB_MAX_RATE_LIMIT_WAIT_SECONDS` | Incremental overlap and API safety limits |
| `ANALYTICS_DATABASE_URL` | PostgreSQL connection string |
| `DATA_DIR`, `REPORTS_DIR` | Raw/staged files and report output |
| `MAX_INVALID_RATIO` | Validation failure threshold |
| `GITHUB_ANALYTICS_SCHEDULE` | DAG schedule (default `@daily`) |

The token and database URL are excluded from `repr()`, and there is a test asserting that the DAG file contains no credentials.

## 8. Running the project

### A. Without Airflow (verified locally)

```bash
export ANALYTICS_DATABASE_URL=postgresql://user:pass@localhost:5432/github_analytics
export GITHUB_REPOSITORIES=psf/requests
uv run gh-analytics run                        # all six steps
uv run gh-analytics run                        # again: incremental, only new changes
cat reports/latest/report.md
```

Individual steps are also available, which is useful for debugging a failed DAG task:

```bash
uv run gh-analytics extract   --run-id my_run
uv run gh-analytics validate  --run-id my_run
uv run gh-analytics transform --run-id my_run
uv run gh-analytics load      --run-id my_run
uv run gh-analytics quality   --run-id my_run
uv run gh-analytics report    --run-id my_run
```

### B. With Airflow (Docker)

> Written but **not yet run** on the build machine (no Docker). Image tags were checked against Docker Hub.

```bash
cp .env.example .env                          # set POSTGRES_PASSWORD, ideally GITHUB_TOKEN
docker compose up -d --build                  # postgres + airflow standalone
docker compose exec airflow airflow dags unpause github_engineering_analytics
docker compose exec airflow airflow dags trigger github_engineering_analytics
# UI: http://localhost:8080  (local-only config: authentication disabled)
```

The DAG, `github_engineering_analytics`, runs:

```text
extract >> validate >> transform >> load >> quality_check >> analytics_report
```

- **Config:** `max_active_runs=1` (watermarks assume runs don't overlap), `catchup=False`.
- **Retries:** 3 per task with exponential backoff (`retry_exponential_backoff=2.0`, Airflow 3.3 semantics).
- **Non-retryable failures** raise `AirflowFailException` so they fail immediately instead of retrying: a missing repository, a data-contract break, or a failed quality gate.
- **Per-run override:** the `repositories` trigger parameter replaces the configured repository list for that run.
- **Failure logging:** an `on_failure_callback` marks the run `failed` in `etl.pipeline_runs`.

Only small summaries pass through XCom. Data moves through the shared `DATA_DIR` volume and PostgreSQL.

## 9. Testing

```bash
uv run pytest                                     # unit tests (integration tests auto-skip)
TEST_POSTGRES_DSN=postgresql://user:pass@localhost:5432/postgres uv run pytest   # + PostgreSQL
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

| Suite | Covers | Ran where |
|---|---|---|
| `unit/test_client.py` | auth headers, token not in repr, `Link` pagination, `max_pages` truncation flag, 5xx/timeout/network retries with backoff, primary rate-limit wait until reset, `Retry-After`, refusing overly long waits, non-retryable 403, 404 | Local ✅ |
| `unit/test_extractors.py` | first-run lookback, watermark minus overlap, PR early stop, issue/PR separation, empty repo (409), truncated listing doesn't advance the watermark, retry-safe raw writes, fork filtering | Local ✅ |
| `unit/test_transform.py` | linked vs unlinked authors (hashed email), bots, PR/review/issue mapping, dedupe, contract violations, quarantine and ratio gate | Local ✅ |
| `integration/test_pipeline_postgres.py` | full extract→report run into real PostgreSQL: star schema contents, exact cycle-time and review-turnaround values, watermarks, run log, **reloading the same data is idempotent**, later updates overwrite older snapshots, all 10 analytics queries return rows, quality gate raises | Local ✅ (PostgreSQL 16.2) |
| `integration/test_dag.py` | DAG imports; task set, linear dependencies, retries, params; no credentials in the DAG file | CI only (needs Airflow on Linux) |

Latest local run: **38 passed, 1 skipped** (the DAG test; Airflow can't be imported on Windows).

## 10. Example output

These come from a **real run** against the live GitHub API (`psf/requests`, 21-day window, unauthenticated). The full logs and generated report are in [docs/example-output.md](docs/example-output.md).

- Run 1 (initial): 14 API requests, loading 1 commit, 10 PRs, 19 reviews and 16 issues.
- Run 2 (incremental, a few minutes later): **5 API requests**. Only the 60-minute overlap was re-read, and fact counts were unchanged, which shows the upserts are idempotent.

```text
| repository   | merged_prs | avg_hours_to_merge | median_hours_to_merge | p90_hours_to_merge |
| psf/requests | 3          | 9.3                | 7.8                   | 14.9               |

| repository   | prs | prs_reviewed | pct_reviewed | total_reviews | approvals | change_requests | median_hours_to_first_review |
| psf/requests | 10  | 7            | 70.0         | 19            | 3         | 2               | 16.6                         |
```

These numbers describe a tiny window of one repository. They demonstrate the pipeline and say nothing meaningful about that project.

## 11. Design decisions

- **Why the REST API rather than GraphQL?** REST gives simple incremental filters (`since`, `sort=updated`), clear rate-limit headers, and `Link` pagination, which is easy to reason about and test. GraphQL would cut request counts for nested data such as reviews per PR, and is the natural next step at scale.
- **Why land raw JSON first?** It gives replayability (fix a transform and re-run without calling the API again), an audit trail, and decoupling. The API is the scarce resource; disk is cheap.
- **Why watermarks in the database, advanced in the load transaction?** If load fails, the watermark doesn't move, so the next run re-extracts the same window. Watermarks and data can never disagree.
- **Why an overlap window?** GitHub timestamps and "last updated" semantics aren't perfectly monotonic across endpoints. Re-reading 60 minutes costs little because the upserts are idempotent.
- **Why a star schema in PostgreSQL?**
  - Fact tables per business process (commit, PR, review, issue) plus conformed dimensions (repository, contributor, date) keep queries simple and fast.
  - PostgreSQL is enough at this scale, and features like `PERCENTILE_CONT`, `FILTER`, generated columns and partial indexes make it a solid analytical store.
- **Why accumulating-snapshot facts for PRs and issues?** A PR's lifecycle (opened, reviewed, merged) is naturally one row with milestone timestamps. Upserts guarded by `updated_at` never let an older snapshot overwrite a newer one.
- **Why SCD Type 1 for repository and contributor?** Analysts want current names, languages and star counts. History of those attributes isn't a requirement; Project 1 shows Type 2 where it is.
- **Why Airflow?** It gives dependency management, retries with backoff, backfills, run history, alerting hooks and a UI. The DAG is thin: all logic lives in the `github_analytics` package, which is also runnable from the CLI and unit-testable without Airflow.
- **Why unlogged staging tables and `COPY`?** Staging is rebuilt from files, so WAL durability is wasted. `COPY` is the fastest bulk path in PostgreSQL.

## 12. Failure handling

| Failure | Behavior |
|---|---|
| 5xx, 408, connection reset, timeout | Exponential backoff with jitter, up to `GITHUB_MAX_RETRIES`. Then the task fails, and Airflow's own retries (also exponential) take over. |
| Primary rate limit (`X-RateLimit-Remaining: 0`) | Sleep until `X-RateLimit-Reset`. If that's longer than `GITHUB_MAX_RATE_LIMIT_WAIT_SECONDS`, raise `RateLimitError` instead of holding a worker for an hour. |
| Secondary rate limit (`Retry-After`) | Sleep for the indicated time, then retry |
| 401/403 permission, other 4xx | Fail immediately (not retryable) |
| 404 repository | `AirflowFailException`: no retry |
| Empty repository (409 on commits) | Treated as zero commits |
| Page cap hit (`GITHUB_MAX_PAGES`) | Logged. **The watermark is not advanced** past unread data. |
| Schema/contract drift | Invalid records are quarantined. Above the threshold, `AirflowFailException`. |
| Database failure mid-load | The transaction rolls back: no partial facts, watermarks unchanged |
| Task retried / run re-executed | The raw run directory is reset before re-extract; all loads are upserts (**idempotent**) |
| Quality gate fails | `AirflowFailException`, the report is skipped, and the run is marked `failed` in `etl.pipeline_runs` |

## 13. Scalability

No benchmarks were run. This is a design discussion.

- **~1K records:** the current design. One DAG run per day, pandas in memory, PostgreSQL on a small instance.
- **~1M records** (hundreds of repositories, years of history):
  - Use a GitHub App installation token (higher rate limits).
  - Use dynamic task mapping in Airflow to extract repositories in parallel. Watermarks are already per repository.
  - Stream the raw JSONL into the transform in chunks instead of whole DataFrames.
  - Range-partition the fact tables by month on the event date.
  - Materialize the weekly aggregates as views refreshed after each load.
- **~100M+ records** (enterprise-wide):
  - Move to GraphQL for nested fetches, or to GitHub webhooks and event streaming instead of polling.
  - Land raw data in object storage (S3/GCS) and load a cloud warehouse (Snowflake/BigQuery) with the same star schema.
  - Use dbt for the SQL layer.
  - Use the CeleryExecutor or KubernetesExecutor for Airflow.
  - Add BRIN indexes on event timestamps and pre-aggregated rollup tables per repository and week.

## Repository layout

```text
project-2-github-engineering-analytics/
├── dags/github_analytics_dag.py
├── src/github_analytics/
│   ├── extract/     client.py (HTTP, retries, rate limits, pagination), extractors.py, raw_store.py
│   ├── transform/   validate.py (contract checks, quarantine), transformers.py
│   ├── load/        postgres.py (COPY + upserts + watermarks + run log), quality.py
│   ├── analytics/   report.py
│   ├── utils/       config, logging, sql helpers
│   ├── pipeline.py  step functions shared by the DAG and CLI
│   └── cli.py
├── sql/             schema.sql, dimensions.sql, facts.sql, analytics.sql
├── config/          pipeline.yml
├── tests/           unit/, integration/, fixtures/ (synthetic GitHub payloads)
├── docker/          postgres-init.sql
└── docs/            interview-notes.md, example-output.md
```

More: [docs/interview-notes.md](docs/interview-notes.md)
