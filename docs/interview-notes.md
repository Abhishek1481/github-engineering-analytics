# Interview notes: GitHub Engineering Activity Analytics

## 1. 30-second explanation

"I built an Airflow-orchestrated pipeline that pulls commits, pull requests, reviews and issues from the GitHub API and models them as a star schema in PostgreSQL. It's incremental: each repository and entity has a watermark, advanced only in the same transaction that loads the data, so failed runs just re-extract. It handles pagination, rate limits and retries, lands raw JSON for replay, validates the API contract, and runs SQL quality checks before publishing metrics like PR cycle time, review turnaround and weekly throughput. I ran it against the live GitHub API. The second run made 5 requests instead of 14 because of the watermarks."

## 2. Two-minute explanation

- **Extract.** A small `requests`-based client.
  - Pagination follows `Link: rel="next"`.
  - Rate limiting reads `X-RateLimit-Remaining`/`Reset` and honors `Retry-After`. It won't block longer than a configured maximum; past that it fails and lets Airflow retry later.
  - Transient 5xx and network errors are retried with jittered exponential backoff.
  - Each entity uses the best incremental hook GitHub offers: `since` for commits and issues, and `sort=updated desc` with an early stop for PRs. Reviews are fetched only for PRs changed in the window.
- **Raw zone.** Untouched payloads as JSONL per run, plus a manifest with counts, windows and proposed watermarks. That's my replay and audit layer.
- **Validate and transform.** Contract checks quarantine bad records and fail the run above a 5% threshold. The transform builds typed DataFrames. Contributors come from every actor; commit authors without a GitHub account are keyed by a hashed email, so I don't store addresses. The output is Parquet.
- **Load.** One transaction: truncate the unlogged staging tables, `COPY` in, run SQL upserts for dimensions then facts, and advance the watermarks. Surrogate keys are resolved by joining on natural keys. PR and issue facts are accumulating snapshots, updated in place, with a guard so an older snapshot can't overwrite a newer one.
- **Quality and analytics.** Ten SQL checks (referential completeness, temporal sanity). Error-level failures stop the DAG. Then ten analytics queries using CTEs, window functions and `PERCENTILE_CONT` write CSV and Markdown reports.
- **Airflow.** A thin DAG calling the same functions as the CLI. XCom carries only summaries. `max_active_runs=1` protects watermark ordering. Non-retryable errors raise `AirflowFailException`.

## 3. Architecture questions

**Why incremental loading, and how do you make it safe?**
Full reloads waste API quota: 5,000 requests per hour, shared by everything using the token. The watermark is `max(updated_at)` seen, per repository and entity, advanced **in the load transaction**. The next window starts at `watermark - 60 minutes` to cover clock skew and late updates, and the upserts make re-reading harmless.

**What if a page limit truncates a listing?**
The extractor records `truncated=True` and keeps the old watermark, so data is never skipped. A fixed page cap on a descending listing could otherwise advance the watermark past pages never read. There's a test for it.

**How do you handle GitHub rate limits?**
- Primary limit (remaining = 0): sleep until the reset time.
- Secondary limit (`Retry-After`): sleep for the indicated duration.
- If the wait exceeds a threshold, raise, because holding an Airflow worker slot for an hour is worse than letting the task retry.
- Authenticated tokens get 5,000 requests per hour; a GitHub App gets more.

**Why a star schema and not just flat tables?**
Business processes (commit, PR, review, issue) become facts at a clear grain, and descriptive context (repository, person, date) becomes shared dimensions. Queries become simple joins, metrics are consistent, and new facts can reuse the dimensions.

**Why is `fact_pull_request` updated instead of append-only?**
It's an accumulating snapshot: one row per PR with milestone timestamps (created, closed, merged). That's the natural grain for cycle-time metrics. For full history of state changes you'd add a transaction fact (PR events), fed by the timeline API or webhooks.

**Why PostgreSQL?**
The data volume is modest, the team likely already runs PostgreSQL, and it has what's needed: `COPY`, `ON CONFLICT` upserts, generated columns, partial indexes and ordered-set aggregates. At much larger scale the same model moves to Snowflake or BigQuery.

**Why Airflow and not cron?**
Dependencies, retries with backoff, backfills, run history, alerting callbacks and visibility. The DAG stays thin, so the logic is testable without Airflow.

**What happens if the DAG fails halfway?**
Each step is idempotent. Extract resets the run directory, load is one transaction and uses upserts, and watermarks only move on a successful load. Retrying a task or re-running the DAG is safe. `on_failure_callback` marks the run `failed` in `etl.pipeline_runs`.

## 4. SQL questions

1. **Median time to merge per repository.** `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cycle_time_hours)` with `GROUP BY` repository and `WHERE is_merged`. The mean is skewed by long-lived PRs, so report the median and p90.
2. **Time to first review.** A CTE that takes `MIN(submitted_at)` per PR, joined back to PRs. Use `LEFT JOIN` so unreviewed PRs count in the denominator (`pct_reviewed`).
3. **Week-over-week change.** `commits - LAG(commits) OVER (ORDER BY week_start_date)`.
4. **4-week moving average.** `AVG(x) OVER (ORDER BY week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)`. Know the difference between `ROWS` and `RANGE` here.
5. **Running open-issue backlog.** `UNION ALL` of +1 (opened) and −1 (closed) events, then `SUM() OVER (ORDER BY week)`.
6. **PRs opened vs merged per week.** Two aggregates combined with a `FULL OUTER JOIN` on week, because some weeks have only one kind of event.
7. **Top contributors without double counting.** Aggregate each fact in its own CTE, then join to the dimension. Joining the facts first would multiply rows. Then `DENSE_RANK()`.
8. **Idempotent upsert that won't regress.** `ON CONFLICT … DO UPDATE … WHERE EXCLUDED.updated_at >= target.updated_at`.
9. **Why use `IS DISTINCT FROM` in the dimension upsert?** It's null-safe comparison, and it skips no-op updates. That keeps `last_updated_at` meaningful and reduces table bloat.
10. **Which indexes would you add for "PRs per repository per week"?** A composite `(repository_key, created_date_key)`. The date dimension carries `week_start_date`, indexed.

## 5. Python questions

- **API handling:** A `requests.Session` reuses connections. Timeouts are set on every call, because the default is none. Headers set the API version and `Accept`. The token is excluded from the dataclass repr.
- **Pagination as a generator:** `Pager.__iter__` yields items page by page, so memory is O(page) during extraction. The `truncated` attribute is exposed after iteration, a common pattern for "generator plus metadata".
- **Retries:** A custom loop rather than a decorator, because the decision depends on the response: rate-limit headers vs status code vs exception type. `sleep` and `clock` are injected, so the tests are instant and deterministic.
- **Exceptions:** A hierarchy (`GitHubAPIError` → `NotFoundError`, `RateLimitError`) maps to Airflow semantics: retryable vs `AirflowFailException`.
- **JSON parsing:** The validation layer checks types explicitly. `bool` is a subclass of `int`, so `isinstance(True, int)` passes a naive id check.
- **pandas pitfalls:** An integer column containing `None` silently becomes `float64`, so `12` becomes `12.0`, which PostgreSQL `BIGINT` rejects. The fix is the nullable `Int64` dtype. The integration test caught this bug.
- **Memory efficiency:** Raw files are streamed line by line (`RawStore.read` is a generator). DataFrames are built per entity. The next step for large volumes is chunked Parquet with streaming `COPY`.
- **Concurrency:** Extraction is I/O bound, so parallelize per repository, with a thread pool or Airflow dynamic task mapping, while sharing one rate-limit budget. That argues for a token bucket, or for separate tokens per worker.

## 6. Scenario questions

**GitHub starts returning 502s for 20 minutes.**
Client retries with backoff (about 1 + 2 + 4 + 8 + 16 s) are exhausted, and the task fails. Airflow retries with exponential backoff (2, 4, 8 minutes). Watermarks haven't moved, so the eventual success loads everything. Nothing is duplicated.

**You need to add 300 repositories.**
- Switch to a GitHub App token.
- Use dynamic task mapping per repository with a pool that limits concurrency.
- Set a sensible initial lookback, then let incremental runs take over.
- Consider GraphQL to fetch PRs with reviews in one query.

**Analysts say PR cycle time looks wrong for one repository.**
- Check `etl.pipeline_runs` and the quality results.
- Query `fact_pull_request` for that repository, looking for `merged_at < created_at` (blocked by a check) or PRs merged outside the window.
- Remember the metric's definition: created → merged, which includes draft time. Consider excluding drafts or measuring from "ready for review" using the timeline API.

**The API adds a new PR state or renames a field.**
Validation quarantines the affected records. If more than 5% are affected the run fails fast with a clear reason, instead of loading bad data silently. Update the contract and transform, then re-run the transform from the raw zone. No API calls are needed.

**The database was restored from a backup that's a day old.**
The watermarks were restored along with the data, so they're consistent with it. The next run automatically re-extracts the missing day. That's the benefit of storing watermarks next to the data instead of in Airflow Variables.
