-- GitHub engineering analytics: PostgreSQL schema (idempotent).
--   stg  transient staging tables, truncated and reloaded by every run
--   dw   star schema: conformed dimensions + one fact table per business process
--   etl  pipeline metadata: watermarks and run log (lineage / observability)

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS etl;

-- ---------------------------------------------------------------- staging
-- UNLOGGED: staging is rebuilt from raw files, so WAL durability is wasted work.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.repositories (
    github_repo_id     BIGINT,
    full_name          TEXT,
    owner_login        TEXT,
    name               TEXT,
    description        TEXT,
    language           TEXT,
    is_private         BOOLEAN,
    is_fork            BOOLEAN,
    is_archived        BOOLEAN,
    default_branch     TEXT,
    created_at         TIMESTAMPTZ,
    pushed_at          TIMESTAMPTZ,
    stargazers_count   INTEGER,
    forks_count        INTEGER,
    open_issues_count  INTEGER,
    html_url           TEXT
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.contributors (
    contributor_nk  TEXT,
    github_user_id  BIGINT,
    login           TEXT,
    display_name    TEXT,
    user_type       TEXT,
    is_bot          BOOLEAN
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.commits (
    repository_full_name  TEXT,
    commit_sha            TEXT,
    author_nk             TEXT,
    committer_nk          TEXT,
    authored_at           TIMESTAMPTZ,
    committed_at          TIMESTAMPTZ,
    message_headline      TEXT,
    parent_count          INTEGER,
    html_url              TEXT
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.pull_requests (
    github_pr_id          BIGINT,
    repository_full_name  TEXT,
    pr_number             INTEGER,
    author_nk             TEXT,
    title                 TEXT,
    state                 TEXT,
    is_draft              BOOLEAN,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ,
    closed_at             TIMESTAMPTZ,
    merged_at             TIMESTAMPTZ,
    base_ref              TEXT,
    html_url              TEXT
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.reviews (
    github_review_id      BIGINT,
    repository_full_name  TEXT,
    pr_number             INTEGER,
    reviewer_nk           TEXT,
    review_state          TEXT,
    submitted_at          TIMESTAMPTZ
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.issues (
    github_issue_id       BIGINT,
    repository_full_name  TEXT,
    issue_number          INTEGER,
    author_nk             TEXT,
    title                 TEXT,
    state                 TEXT,
    state_reason          TEXT,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ,
    closed_at             TIMESTAMPTZ,
    comments_count        INTEGER,
    labels                TEXT[]
);

-- ------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key         INTEGER PRIMARY KEY,          -- YYYYMMDD
    full_date        DATE    NOT NULL UNIQUE,
    year             SMALLINT NOT NULL,
    quarter          SMALLINT NOT NULL,
    month            SMALLINT NOT NULL,
    month_name       TEXT    NOT NULL,
    iso_year         SMALLINT NOT NULL,
    iso_week         SMALLINT NOT NULL,
    week_start_date  DATE    NOT NULL,             -- Monday
    day_of_week      SMALLINT NOT NULL,            -- 1 = Monday (ISO)
    day_name         TEXT    NOT NULL,
    is_weekend       BOOLEAN NOT NULL
);

-- Type 1 (overwrite): current repository attributes are what analysts need.
CREATE TABLE IF NOT EXISTS dw.dim_repository (
    repository_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    github_repo_id     BIGINT  NOT NULL UNIQUE,
    full_name          TEXT    NOT NULL UNIQUE,
    owner_login        TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    description        TEXT,
    language           TEXT,
    is_private         BOOLEAN NOT NULL,
    is_fork            BOOLEAN NOT NULL,
    is_archived        BOOLEAN NOT NULL,
    default_branch     TEXT,
    created_at         TIMESTAMPTZ,
    pushed_at          TIMESTAMPTZ,
    stargazers_count   INTEGER,
    forks_count        INTEGER,
    open_issues_count  INTEGER,
    html_url           TEXT,
    first_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_contributor (
    contributor_key  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contributor_nk   TEXT    NOT NULL UNIQUE,   -- gh:<login> or git:<sha256(email)[:16]>
    github_user_id   BIGINT,
    login            TEXT,
    display_name     TEXT,
    user_type        TEXT    NOT NULL,          -- User | Bot | Organization | Unlinked
    is_bot           BOOLEAN NOT NULL,
    first_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------ facts
CREATE TABLE IF NOT EXISTS dw.fact_commit (
    repository_key      INTEGER NOT NULL REFERENCES dw.dim_repository,
    commit_sha          CHAR(40) NOT NULL,
    author_key          INTEGER REFERENCES dw.dim_contributor,
    committer_key       INTEGER REFERENCES dw.dim_contributor,
    authored_date_key   INTEGER NOT NULL REFERENCES dw.dim_date,
    committed_date_key  INTEGER NOT NULL REFERENCES dw.dim_date,
    authored_at         TIMESTAMPTZ NOT NULL,
    committed_at        TIMESTAMPTZ NOT NULL,
    message_headline    TEXT,
    parent_count        SMALLINT NOT NULL,
    is_merge_commit     BOOLEAN GENERATED ALWAYS AS (parent_count > 1) STORED,
    html_url            TEXT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_key, commit_sha)
);

CREATE TABLE IF NOT EXISTS dw.fact_pull_request (
    pull_request_key   BIGINT PRIMARY KEY,           -- GitHub PR id (globally unique)
    repository_key     INTEGER NOT NULL REFERENCES dw.dim_repository,
    author_key         INTEGER REFERENCES dw.dim_contributor,
    pr_number          INTEGER NOT NULL,
    title              TEXT,
    state              TEXT    NOT NULL CHECK (state IN ('open', 'closed')),
    is_draft           BOOLEAN NOT NULL,
    is_merged          BOOLEAN GENERATED ALWAYS AS (merged_at IS NOT NULL) STORED,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    closed_at          TIMESTAMPTZ,
    merged_at          TIMESTAMPTZ,
    created_date_key   INTEGER NOT NULL REFERENCES dw.dim_date,
    closed_date_key    INTEGER REFERENCES dw.dim_date,
    merged_date_key    INTEGER REFERENCES dw.dim_date,
    cycle_time_hours   NUMERIC(12, 2) GENERATED ALWAYS AS
        (ROUND((EXTRACT(EPOCH FROM (merged_at - created_at)) / 3600.0)::NUMERIC, 2)) STORED,
    base_ref           TEXT,
    html_url           TEXT,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_key, pr_number)
);

CREATE TABLE IF NOT EXISTS dw.fact_review (
    review_key          BIGINT PRIMARY KEY,          -- GitHub review id
    pull_request_key    BIGINT  NOT NULL REFERENCES dw.fact_pull_request,
    repository_key      INTEGER NOT NULL REFERENCES dw.dim_repository,
    reviewer_key        INTEGER REFERENCES dw.dim_contributor,
    review_state        TEXT    NOT NULL,            -- APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED
    submitted_at        TIMESTAMPTZ NOT NULL,
    submitted_date_key  INTEGER NOT NULL REFERENCES dw.dim_date,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.fact_issue (
    issue_key            BIGINT PRIMARY KEY,         -- GitHub issue id
    repository_key       INTEGER NOT NULL REFERENCES dw.dim_repository,
    author_key           INTEGER REFERENCES dw.dim_contributor,
    issue_number         INTEGER NOT NULL,
    title                TEXT,
    state                TEXT    NOT NULL CHECK (state IN ('open', 'closed')),
    state_reason         TEXT,
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    closed_at            TIMESTAMPTZ,
    created_date_key     INTEGER NOT NULL REFERENCES dw.dim_date,
    closed_date_key      INTEGER REFERENCES dw.dim_date,
    comments_count       INTEGER NOT NULL DEFAULT 0,
    labels               TEXT[]  NOT NULL DEFAULT '{}',
    time_to_close_hours  NUMERIC(12, 2) GENERATED ALWAYS AS
        (ROUND((EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600.0)::NUMERIC, 2)) STORED,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_key, issue_number)
);

-- ---------------------------------------------------------------- indexes
-- Analytical access paths: "per repository over time" and "per person".
CREATE INDEX IF NOT EXISTS ix_fact_commit_repo_date   ON dw.fact_commit (repository_key, committed_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_commit_author      ON dw.fact_commit (author_key);
CREATE INDEX IF NOT EXISTS ix_fact_pr_repo_created    ON dw.fact_pull_request (repository_key, created_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_pr_author          ON dw.fact_pull_request (author_key);
CREATE INDEX IF NOT EXISTS ix_fact_pr_merged          ON dw.fact_pull_request (merged_date_key) WHERE merged_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_fact_review_pr          ON dw.fact_review (pull_request_key, submitted_at);
CREATE INDEX IF NOT EXISTS ix_fact_review_reviewer    ON dw.fact_review (reviewer_key);
CREATE INDEX IF NOT EXISTS ix_fact_issue_repo_created ON dw.fact_issue (repository_key, created_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_issue_open         ON dw.fact_issue (repository_key) WHERE state = 'open';
CREATE INDEX IF NOT EXISTS ix_dim_date_week           ON dw.dim_date (week_start_date);

-- -------------------------------------------------------------------- etl
CREATE TABLE IF NOT EXISTS etl.extraction_watermarks (
    repository_full_name  TEXT        NOT NULL,
    entity                TEXT        NOT NULL,
    watermark_at          TIMESTAMPTZ NOT NULL,
    updated_by_run        TEXT        NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_full_name, entity)
);

CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id          TEXT PRIMARY KEY,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    repositories    TEXT[],
    extracted       JSONB,
    loaded          JSONB,
    quality_checks  JSONB,
    error_message   TEXT
);
