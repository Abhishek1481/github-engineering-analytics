-- Dimension loads (idempotent). Runs inside the load transaction after the
-- staging tables have been filled.

-- dim_date: static calendar covering GitHub's lifetime plus headroom.
INSERT INTO dw.dim_date (
    date_key, full_date, year, quarter, month, month_name, iso_year, iso_week,
    week_start_date, day_of_week, day_name, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(ISOYEAR FROM d)::SMALLINT,
    EXTRACT(WEEK FROM d)::SMALLINT,
    DATE_TRUNC('week', d)::DATE,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM GENERATE_SERIES('2008-01-01'::DATE, '2035-12-31'::DATE, INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- dim_repository: SCD Type 1 upsert keyed on the immutable GitHub id.
-- The WHERE clause skips no-op updates so last_updated_at stays meaningful.
INSERT INTO dw.dim_repository (
    github_repo_id, full_name, owner_login, name, description, language, is_private,
    is_fork, is_archived, default_branch, created_at, pushed_at, stargazers_count,
    forks_count, open_issues_count, html_url
)
SELECT DISTINCT ON (github_repo_id)
    github_repo_id, full_name, owner_login, name, description, language, is_private,
    is_fork, is_archived, default_branch, created_at, pushed_at, stargazers_count,
    forks_count, open_issues_count, html_url
FROM stg.repositories
ORDER BY github_repo_id
ON CONFLICT (github_repo_id) DO UPDATE SET
    full_name         = EXCLUDED.full_name,
    owner_login       = EXCLUDED.owner_login,
    name              = EXCLUDED.name,
    description       = EXCLUDED.description,
    language          = EXCLUDED.language,
    is_private        = EXCLUDED.is_private,
    is_fork           = EXCLUDED.is_fork,
    is_archived       = EXCLUDED.is_archived,
    default_branch    = EXCLUDED.default_branch,
    pushed_at         = EXCLUDED.pushed_at,
    stargazers_count  = EXCLUDED.stargazers_count,
    forks_count       = EXCLUDED.forks_count,
    open_issues_count = EXCLUDED.open_issues_count,
    html_url          = EXCLUDED.html_url,
    last_updated_at   = now()
WHERE (dw.dim_repository.full_name, dw.dim_repository.description, dw.dim_repository.language,
       dw.dim_repository.is_archived, dw.dim_repository.pushed_at,
       dw.dim_repository.stargazers_count, dw.dim_repository.forks_count,
       dw.dim_repository.open_issues_count)
  IS DISTINCT FROM
      (EXCLUDED.full_name, EXCLUDED.description, EXCLUDED.language, EXCLUDED.is_archived,
       EXCLUDED.pushed_at, EXCLUDED.stargazers_count, EXCLUDED.forks_count,
       EXCLUDED.open_issues_count);

-- dim_contributor: upsert; never overwrite a known GitHub id with NULL.
INSERT INTO dw.dim_contributor (contributor_nk, github_user_id, login, display_name,
                                user_type, is_bot)
SELECT DISTINCT ON (contributor_nk)
    contributor_nk, github_user_id, login, display_name, user_type, is_bot
FROM stg.contributors
WHERE contributor_nk IS NOT NULL
ORDER BY contributor_nk, (github_user_id IS NOT NULL) DESC
ON CONFLICT (contributor_nk) DO UPDATE SET
    github_user_id  = COALESCE(EXCLUDED.github_user_id, dw.dim_contributor.github_user_id),
    login           = COALESCE(EXCLUDED.login, dw.dim_contributor.login),
    display_name    = EXCLUDED.display_name,
    user_type       = EXCLUDED.user_type,
    is_bot          = EXCLUDED.is_bot,
    last_updated_at = now()
WHERE (dw.dim_contributor.display_name, dw.dim_contributor.user_type, dw.dim_contributor.is_bot,
       dw.dim_contributor.github_user_id)
  IS DISTINCT FROM
      (EXCLUDED.display_name, EXCLUDED.user_type, EXCLUDED.is_bot,
       COALESCE(EXCLUDED.github_user_id, dw.dim_contributor.github_user_id));
