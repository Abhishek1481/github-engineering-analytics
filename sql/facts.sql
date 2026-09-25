-- Fact loads (idempotent upserts). Natural keys from staging are resolved to
-- surrogate keys by joining the dimensions loaded in dimensions.sql.
-- Order matters: fact_review references fact_pull_request.

INSERT INTO dw.fact_commit (
    repository_key, commit_sha, author_key, committer_key, authored_date_key,
    committed_date_key, authored_at, committed_at, message_headline, parent_count, html_url
)
SELECT
    r.repository_key,
    c.commit_sha,
    a.contributor_key,
    cm.contributor_key,
    TO_CHAR(c.authored_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    TO_CHAR(c.committed_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    c.authored_at,
    c.committed_at,
    c.message_headline,
    c.parent_count,
    c.html_url
FROM stg.commits c
JOIN dw.dim_repository r       ON r.full_name = c.repository_full_name
LEFT JOIN dw.dim_contributor a  ON a.contributor_nk = c.author_nk
LEFT JOIN dw.dim_contributor cm ON cm.contributor_nk = c.committer_nk
ON CONFLICT (repository_key, commit_sha) DO UPDATE SET
    author_key       = EXCLUDED.author_key,
    committer_key    = EXCLUDED.committer_key,
    message_headline = EXCLUDED.message_headline,
    loaded_at        = now();

INSERT INTO dw.fact_pull_request (
    pull_request_key, repository_key, author_key, pr_number, title, state, is_draft,
    created_at, updated_at, closed_at, merged_at, created_date_key, closed_date_key,
    merged_date_key, base_ref, html_url
)
SELECT
    p.github_pr_id,
    r.repository_key,
    a.contributor_key,
    p.pr_number,
    p.title,
    p.state,
    p.is_draft,
    p.created_at,
    p.updated_at,
    p.closed_at,
    p.merged_at,
    TO_CHAR(p.created_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    TO_CHAR(p.closed_at  AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    TO_CHAR(p.merged_at  AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    p.base_ref,
    p.html_url
FROM stg.pull_requests p
JOIN dw.dim_repository r      ON r.full_name = p.repository_full_name
LEFT JOIN dw.dim_contributor a ON a.contributor_nk = p.author_nk
ON CONFLICT (pull_request_key) DO UPDATE SET
    title            = EXCLUDED.title,
    state            = EXCLUDED.state,
    is_draft         = EXCLUDED.is_draft,
    updated_at       = EXCLUDED.updated_at,
    closed_at        = EXCLUDED.closed_at,
    merged_at        = EXCLUDED.merged_at,
    closed_date_key  = EXCLUDED.closed_date_key,
    merged_date_key  = EXCLUDED.merged_date_key,
    loaded_at        = now()
-- An older snapshot must never overwrite a newer one (e.g. out-of-order reruns).
WHERE EXCLUDED.updated_at >= dw.fact_pull_request.updated_at;

INSERT INTO dw.fact_review (
    review_key, pull_request_key, repository_key, reviewer_key, review_state,
    submitted_at, submitted_date_key
)
SELECT
    v.github_review_id,
    p.pull_request_key,
    r.repository_key,
    a.contributor_key,
    v.review_state,
    v.submitted_at,
    TO_CHAR(v.submitted_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER
FROM stg.reviews v
JOIN dw.dim_repository r     ON r.full_name = v.repository_full_name
JOIN dw.fact_pull_request p  ON p.repository_key = r.repository_key
                            AND p.pr_number = v.pr_number
LEFT JOIN dw.dim_contributor a ON a.contributor_nk = v.reviewer_nk
ON CONFLICT (review_key) DO UPDATE SET
    review_state = EXCLUDED.review_state,   -- a review can later be DISMISSED
    loaded_at    = now();

INSERT INTO dw.fact_issue (
    issue_key, repository_key, author_key, issue_number, title, state, state_reason,
    created_at, updated_at, closed_at, created_date_key, closed_date_key, comments_count,
    labels
)
SELECT
    i.github_issue_id,
    r.repository_key,
    a.contributor_key,
    i.issue_number,
    i.title,
    i.state,
    i.state_reason,
    i.created_at,
    i.updated_at,
    i.closed_at,
    TO_CHAR(i.created_at AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    TO_CHAR(i.closed_at  AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER,
    COALESCE(i.comments_count, 0),
    COALESCE(i.labels, '{}')
FROM stg.issues i
JOIN dw.dim_repository r      ON r.full_name = i.repository_full_name
LEFT JOIN dw.dim_contributor a ON a.contributor_nk = i.author_nk
ON CONFLICT (issue_key) DO UPDATE SET
    title           = EXCLUDED.title,
    state           = EXCLUDED.state,
    state_reason    = EXCLUDED.state_reason,
    updated_at      = EXCLUDED.updated_at,
    closed_at       = EXCLUDED.closed_at,
    closed_date_key = EXCLUDED.closed_date_key,
    comments_count  = EXCLUDED.comments_count,
    labels          = EXCLUDED.labels,
    loaded_at       = now()
WHERE EXCLUDED.updated_at >= dw.fact_issue.updated_at;
