-- Engineering analytics over the dw star schema.
-- Each query is introduced by "-- name: <id>"; `gh-analytics report` runs them
-- all and writes CSV + Markdown to REPORTS_DIR.

-- name: weekly_commit_volume
-- Commits per ISO week with distinct authors and week-over-week change.
WITH weekly AS (
    SELECT
        d.week_start_date,
        COUNT(*)                                   AS commits,
        COUNT(DISTINCT f.author_key)               AS active_authors,
        COUNT(*) FILTER (WHERE f.is_merge_commit)  AS merge_commits
    FROM dw.fact_commit f
    JOIN dw.dim_date d ON d.date_key = f.committed_date_key
    GROUP BY d.week_start_date
)
SELECT
    week_start_date,
    commits,
    active_authors,
    merge_commits,
    commits - LAG(commits) OVER (ORDER BY week_start_date) AS change_vs_prev_week
FROM weekly
ORDER BY week_start_date;

-- name: weekly_pr_volume
-- Pull requests opened vs merged per week (a full outer join of two event streams).
WITH opened AS (
    SELECT d.week_start_date, COUNT(*) AS prs_opened
    FROM dw.fact_pull_request p
    JOIN dw.dim_date d ON d.date_key = p.created_date_key
    GROUP BY d.week_start_date
),
merged AS (
    SELECT d.week_start_date, COUNT(*) AS prs_merged
    FROM dw.fact_pull_request p
    JOIN dw.dim_date d ON d.date_key = p.merged_date_key
    GROUP BY d.week_start_date
)
SELECT
    COALESCE(o.week_start_date, m.week_start_date) AS week_start_date,
    COALESCE(o.prs_opened, 0)                      AS prs_opened,
    COALESCE(m.prs_merged, 0)                      AS prs_merged
FROM opened o
FULL OUTER JOIN merged m ON m.week_start_date = o.week_start_date
ORDER BY 1;

-- name: pr_cycle_time_by_repository
-- Distribution of open-to-merge time for merged PRs, per repository.
SELECT
    r.full_name                                                           AS repository,
    COUNT(*)                                                              AS merged_prs,
    ROUND(AVG(p.cycle_time_hours), 1)                                     AS avg_hours_to_merge,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.cycle_time_hours)::NUMERIC, 1)
                                                                          AS median_hours_to_merge,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY p.cycle_time_hours)::NUMERIC, 1)
                                                                          AS p90_hours_to_merge
FROM dw.fact_pull_request p
JOIN dw.dim_repository r ON r.repository_key = p.repository_key
WHERE p.is_merged
GROUP BY r.full_name
ORDER BY merged_prs DESC;

-- name: avg_time_to_merge_trend
-- Weekly average hours-to-merge with a 4-week moving average to smooth noise.
WITH weekly AS (
    SELECT d.week_start_date, AVG(p.cycle_time_hours) AS avg_hours, COUNT(*) AS merged_prs
    FROM dw.fact_pull_request p
    JOIN dw.dim_date d ON d.date_key = p.merged_date_key
    WHERE p.is_merged
    GROUP BY d.week_start_date
)
SELECT
    week_start_date,
    merged_prs,
    ROUND(avg_hours, 1) AS avg_hours_to_merge,
    ROUND(AVG(avg_hours) OVER (ORDER BY week_start_date
                               ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 1)
                        AS moving_avg_4w
FROM weekly
ORDER BY week_start_date;

-- name: review_activity
-- Review turnaround: time from PR creation to its first review, and review mix.
WITH first_review AS (
    SELECT
        v.pull_request_key,
        MIN(v.submitted_at) AS first_review_at,
        COUNT(*)            AS reviews,
        COUNT(*) FILTER (WHERE v.review_state = 'APPROVED')          AS approvals,
        COUNT(*) FILTER (WHERE v.review_state = 'CHANGES_REQUESTED') AS change_requests
    FROM dw.fact_review v
    GROUP BY v.pull_request_key
)
SELECT
    r.full_name                                                           AS repository,
    COUNT(p.pull_request_key)                                             AS prs,
    COUNT(fr.pull_request_key)                                            AS prs_reviewed,
    ROUND(100.0 * COUNT(fr.pull_request_key) / NULLIF(COUNT(p.pull_request_key), 0), 1)
                                                                          AS pct_reviewed,
    COALESCE(SUM(fr.reviews), 0)                                          AS total_reviews,
    COALESCE(SUM(fr.approvals), 0)                                        AS approvals,
    COALESCE(SUM(fr.change_requests), 0)                                  AS change_requests,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM fr.first_review_at - p.created_at) / 3600.0))::NUMERIC, 1)
                                                                          AS median_hours_to_first_review
FROM dw.fact_pull_request p
JOIN dw.dim_repository r ON r.repository_key = p.repository_key
LEFT JOIN first_review fr ON fr.pull_request_key = p.pull_request_key
GROUP BY r.full_name
ORDER BY prs DESC;

-- name: contributor_activity
-- Per-person activity across commits, PRs and reviews, ranked (bots excluded).
WITH commits AS (
    SELECT author_key AS contributor_key, COUNT(*) AS commits
    FROM dw.fact_commit GROUP BY author_key
),
prs AS (
    SELECT author_key AS contributor_key, COUNT(*) AS prs_opened,
           COUNT(*) FILTER (WHERE is_merged) AS prs_merged
    FROM dw.fact_pull_request GROUP BY author_key
),
reviews AS (
    SELECT reviewer_key AS contributor_key, COUNT(*) AS reviews_given
    FROM dw.fact_review GROUP BY reviewer_key
),
activity AS (
    SELECT
        c.contributor_key,
        COALESCE(c.login, c.display_name) AS contributor,
        COALESCE(cm.commits, 0)          AS commits,
        COALESCE(p.prs_opened, 0)        AS prs_opened,
        COALESCE(p.prs_merged, 0)        AS prs_merged,
        COALESCE(rv.reviews_given, 0)    AS reviews_given
    FROM dw.dim_contributor c
    LEFT JOIN commits cm ON cm.contributor_key = c.contributor_key
    LEFT JOIN prs p      ON p.contributor_key  = c.contributor_key
    LEFT JOIN reviews rv ON rv.contributor_key = c.contributor_key
    WHERE NOT c.is_bot
)
SELECT
    DENSE_RANK() OVER (ORDER BY commits + prs_opened + reviews_given DESC) AS activity_rank,
    contributor,
    commits,
    prs_opened,
    prs_merged,
    reviews_given
FROM activity
WHERE commits + prs_opened + reviews_given > 0
ORDER BY activity_rank, contributor
LIMIT 25;

-- name: repository_activity
-- Repository scorecard: volume, recency and contributor breadth.
SELECT
    r.full_name                                                  AS repository,
    r.language,
    r.stargazers_count                                           AS stars,
    (SELECT COUNT(*) FROM dw.fact_commit f WHERE f.repository_key = r.repository_key)
                                                                 AS commits,
    (SELECT COUNT(DISTINCT f.author_key) FROM dw.fact_commit f
      WHERE f.repository_key = r.repository_key)                 AS commit_authors,
    (SELECT COUNT(*) FROM dw.fact_pull_request p WHERE p.repository_key = r.repository_key)
                                                                 AS pull_requests,
    (SELECT COUNT(*) FROM dw.fact_issue i WHERE i.repository_key = r.repository_key)
                                                                 AS issues,
    (SELECT MAX(f.committed_at) FROM dw.fact_commit f
      WHERE f.repository_key = r.repository_key)                 AS last_commit_at
FROM dw.dim_repository r
ORDER BY commits DESC;

-- name: issues_open_vs_closed
-- Issue backlog: opened vs closed per week and the running open count.
WITH events AS (
    SELECT created_date_key AS date_key, 1 AS opened, 0 AS closed FROM dw.fact_issue
    UNION ALL
    SELECT closed_date_key, 0, 1 FROM dw.fact_issue WHERE closed_date_key IS NOT NULL
),
weekly AS (
    SELECT d.week_start_date, SUM(e.opened) AS opened, SUM(e.closed) AS closed
    FROM events e
    JOIN dw.dim_date d ON d.date_key = e.date_key
    GROUP BY d.week_start_date
)
SELECT
    week_start_date,
    opened,
    closed,
    SUM(opened - closed) OVER (ORDER BY week_start_date) AS net_open_change_cumulative
FROM weekly
ORDER BY week_start_date;

-- name: issue_resolution_by_repository
-- Current open/closed split and time to close.
SELECT
    r.full_name                                                  AS repository,
    COUNT(*) FILTER (WHERE i.state = 'open')                     AS open_issues,
    COUNT(*) FILTER (WHERE i.state = 'closed')                   AS closed_issues,
    ROUND(AVG(i.time_to_close_hours) FILTER (WHERE i.state = 'closed') / 24.0, 1)
                                                                 AS avg_days_to_close,
    COUNT(*) FILTER (WHERE i.state_reason = 'not_planned')       AS closed_not_planned
FROM dw.fact_issue i
JOIN dw.dim_repository r ON r.repository_key = i.repository_key
GROUP BY r.full_name
ORDER BY open_issues DESC;

-- name: engineering_activity_trends
-- One weekly time series combining all activity with running totals.
WITH weeks AS (
    SELECT DISTINCT d.week_start_date
    FROM dw.dim_date d
    WHERE d.full_date BETWEEN (SELECT MIN(created_at)::DATE FROM dw.fact_pull_request)
                          AND CURRENT_DATE
),
c AS (SELECT d.week_start_date, COUNT(*) n FROM dw.fact_commit f
      JOIN dw.dim_date d ON d.date_key = f.committed_date_key GROUP BY 1),
p AS (SELECT d.week_start_date, COUNT(*) n FROM dw.fact_pull_request f
      JOIN dw.dim_date d ON d.date_key = f.created_date_key GROUP BY 1),
v AS (SELECT d.week_start_date, COUNT(*) n FROM dw.fact_review f
      JOIN dw.dim_date d ON d.date_key = f.submitted_date_key GROUP BY 1),
i AS (SELECT d.week_start_date, COUNT(*) n FROM dw.fact_issue f
      JOIN dw.dim_date d ON d.date_key = f.created_date_key GROUP BY 1)
SELECT
    w.week_start_date,
    COALESCE(c.n, 0) AS commits,
    COALESCE(p.n, 0) AS prs_opened,
    COALESCE(v.n, 0) AS reviews,
    COALESCE(i.n, 0) AS issues_opened,
    SUM(COALESCE(c.n, 0)) OVER (ORDER BY w.week_start_date) AS cumulative_commits
FROM weeks w
LEFT JOIN c ON c.week_start_date = w.week_start_date
LEFT JOIN p ON p.week_start_date = w.week_start_date
LEFT JOIN v ON v.week_start_date = w.week_start_date
LEFT JOIN i ON i.week_start_date = w.week_start_date
WHERE COALESCE(c.n, 0) + COALESCE(p.n, 0) + COALESCE(v.n, 0) + COALESCE(i.n, 0) > 0
ORDER BY w.week_start_date;
