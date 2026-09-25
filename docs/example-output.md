# Example output (captured from a real run)

Captured on 2026-09-25 (UTC) by running `gh-analytics run` on the build machine against the
**live GitHub REST API** (repository `psf/requests`, 21-day initial lookback, unauthenticated)
and a local PostgreSQL 16 database. Nothing below was hand-edited except:

- temporary paths were shortened to `<tmp>`;
- GitHub logins in the *Contributor activity* table were replaced with `contributor_NN`
  (the pipeline stores real public logins; they are redacted here only for the README).

The numbers describe one small 21-day window of one public repository. They are a functional
demonstration, not a benchmark or a statement about that project.

## Run 1: first (initial) extraction

```text
$ gh-analytics run --run-id live_demo_1
INFO    github_analytics.pipeline: pipeline started run_id=live_demo_1
WARNING github_analytics.extract.client: GITHUB_TOKEN not set: unauthenticated requests are limited to 60/hour
INFO    github_analytics.load.postgres: schema initialised
INFO    github_analytics.extract.extractors: extracting repository repository=psf/requests
INFO    github_analytics.extract.extractors: extraction complete run_id=live_demo_1 repositories=1 commits=1 pull_requests=10 reviews=19 issues=16 api_requests=14
INFO    github_analytics.pipeline: transform complete run_id=live_demo_1 repositories=1 contributors=29 commits=1 pull_requests=10 reviews=19 issues=16
INFO    github_analytics.load.postgres: load committed run_id=live_demo_1 repositories=1 contributors=29 commits=1 pull_requests=10 reviews=19 issues=16 watermarks_updated=3
INFO    github_analytics.analytics.report: report written path=<tmp>/p2reports/live_demo_1/report.md queries=10
INFO    github_analytics.pipeline: pipeline completed run_id=live_demo_1
```

Quality checks from run 1:

```text
INFO    github_analytics.load.quality: quality check check=staged_prs_not_loaded violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=staged_commits_not_loaded violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=staged_issues_not_loaded violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=pr_merged_before_created violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=pr_merged_but_open violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=issue_closed_before_created violations=0 severity=error
INFO    github_analytics.load.quality: quality check check=closed_issue_without_close_time violations=0 severity=warn
INFO    github_analytics.load.quality: quality check check=reviews_without_pull_request violations=0 severity=warn
INFO    github_analytics.load.quality: quality check check=commits_without_author violations=0 severity=warn
INFO    github_analytics.load.quality: quality check check=future_dated_activity violations=0 severity=warn
```

## Run 2: incremental extraction a few minutes later

The stored watermarks meant run 2 asked GitHub only for changes since
`watermark - 60 min overlap`: **5 API requests instead of 14**. The overlap re-read one commit, one
pull request and one issue, and the idempotent upserts left the fact tables unchanged
(`fact_pull_request` stayed at 10 rows, `fact_review` at 19, `fact_issue` at 16).

```text
$ gh-analytics run --run-id live_demo_2
INFO    github_analytics.pipeline: pipeline started run_id=live_demo_2
WARNING github_analytics.extract.client: GITHUB_TOKEN not set: unauthenticated requests are limited to 60/hour
INFO    github_analytics.load.postgres: schema initialised
INFO    github_analytics.extract.extractors: extracting repository repository=psf/requests
INFO    github_analytics.extract.extractors: extraction complete run_id=live_demo_2 repositories=1 commits=1 pull_requests=1 reviews=0 issues=1 api_requests=5
INFO    github_analytics.pipeline: transform complete run_id=live_demo_2 repositories=1 contributors=4 commits=1 pull_requests=1 reviews=0 issues=1
INFO    github_analytics.load.postgres: load committed run_id=live_demo_2 repositories=1 contributors=4 commits=1 pull_requests=1 reviews=0 issues=1 watermarks_updated=3
INFO    github_analytics.analytics.report: report written path=<tmp>/p2reports/live_demo_2/report.md queries=10
INFO    github_analytics.pipeline: pipeline completed run_id=live_demo_2
```

`etl.pipeline_runs` after both runs:

```text
('live_demo_1', 'succeeded', {'issues': 16, 'commits': 1, 'reviews': 19, 'repositories': 1, 'pull_requests': 10})
('live_demo_2', 'succeeded', {'issues': 1, 'commits': 1, 'reviews': 0, 'repositories': 1, 'pull_requests': 1})
```

## Generated report (`reports/latest/report.md`)

Run `live_demo_2` generated 2026-09-25 00:55 UTC.

## Weekly commit volume

| week_start_date | commits | active_authors | merge_commits | change_vs_prev_week |
|---|---|---|---|---|
| 2026-09-21 | 1 | 1 | 0 |  |

## Weekly pr volume

| week_start_date | prs_opened | prs_merged |
|---|---|---|
| 2019-09-30 | 1 | 0 |
| 2024-03-25 | 1 | 0 |
| 2025-07-07 | 1 | 0 |
| 2025-09-01 | 1 | 0 |
| 2025-09-08 | 2 | 2 |
| 2025-10-20 | 2 | 0 |
| 2026-09-07 | 1 | 0 |
| 2026-09-21 | 1 | 1 |

## Pr cycle time by repository

| repository | merged_prs | avg_hours_to_merge | median_hours_to_merge | p90_hours_to_merge |
|---|---|---|---|---|
| psf/requests | 3 | 9.3 | 7.8 | 14.9 |

## Avg time to merge trend

| week_start_date | merged_prs | avg_hours_to_merge | moving_avg_4w |
|---|---|---|---|
| 2025-09-08 | 2 | 12.2 | 12.2 |
| 2026-09-21 | 1 | 3.4 | 7.8 |

## Review activity

| repository | prs | prs_reviewed | pct_reviewed | total_reviews | approvals | change_requests | median_hours_to_first_review |
|---|---|---|---|---|---|---|---|
| psf/requests | 10 | 7 | 70.0 | 19 | 3 | 2 | 16.6 |

## Contributor activity

| activity_rank | contributor | commits | prs_opened | prs_merged | reviews_given |
|---|---|---|---|---|---|
| 1 | contributor_01 | 0 | 0 | 0 | 9 |
| 2 | contributor_02 | 0 | 1 | 0 | 2 |
| 2 | contributor_03 | 0 | 0 | 0 | 3 |
| 3 | contributor_04 | 0 | 1 | 0 | 1 |
| 3 | contributor_05 | 0 | 1 | 0 | 1 |
| 4 | contributor_06 | 0 | 0 | 0 | 1 |
| 4 | contributor_07 | 0 | 0 | 0 | 1 |
| 4 | contributor_08 | 0 | 1 | 0 | 0 |
| 4 | contributor_09 | 0 | 1 | 0 | 0 |
| 4 | contributor_10 | 0 | 1 | 0 | 0 |
| 4 | contributor_11 | 0 | 0 | 0 | 1 |

## Repository activity

| repository | language | stars | commits | commit_authors | pull_requests | issues | last_commit_at |
|---|---|---|---|---|---|---|---|
| psf/requests | Python | 54342 | 1 | 1 | 10 | 16 | 2026-09-21 20:22 |

## Issues open vs closed

| week_start_date | opened | closed | net_open_change_cumulative |
|---|---|---|---|
| 2019-02-18 | 1 | 0 | 1 |
| 2021-12-13 | 1 | 0 | 2 |
| 2022-04-04 | 1 | 0 | 3 |
| 2026-06-29 | 1 | 0 | 4 |
| 2026-07-06 | 1 | 0 | 5 |
| 2026-08-03 | 1 | 0 | 6 |
| 2026-08-24 | 1 | 0 | 7 |
| 2026-08-31 | 2 | 1 | 8 |
| 2026-09-07 | 4 | 3 | 9 |
| 2026-09-14 | 1 | 0 | 10 |
| 2026-09-21 | 2 | 0 | 12 |

## Issue resolution by repository

| repository | open_issues | closed_issues | avg_days_to_close | closed_not_planned |
|---|---|---|---|---|
| psf/requests | 12 | 4 | 0.0 | 2 |

## Engineering activity trends

| week_start_date | commits | prs_opened | reviews | issues_opened | cumulative_commits |
|---|---|---|---|---|---|
| 2019-09-30 | 0 | 1 | 0 | 0 | 0 |
| 2021-12-13 | 0 | 0 | 0 | 1 | 0 |
| 2022-04-04 | 0 | 0 | 0 | 1 | 0 |
| 2024-03-25 | 0 | 1 | 0 | 0 | 0 |
| 2024-04-15 | 0 | 0 | 1 | 0 | 0 |
| 2024-10-28 | 0 | 0 | 2 | 0 | 0 |
| 2025-07-07 | 0 | 1 | 0 | 0 | 0 |
| 2025-08-04 | 0 | 0 | 2 | 0 | 0 |
| 2025-08-18 | 0 | 0 | 2 | 0 | 0 |
| 2025-09-01 | 0 | 1 | 0 | 0 | 0 |
| 2025-09-08 | 0 | 2 | 3 | 0 | 0 |
| 2025-10-20 | 0 | 2 | 2 | 0 | 0 |
| 2025-11-03 | 0 | 0 | 1 | 0 | 0 |
| 2026-06-29 | 0 | 0 | 0 | 1 | 0 |
| 2026-07-06 | 0 | 0 | 0 | 1 | 0 |

_6 more rows in the CSV._

