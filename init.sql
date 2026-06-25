-- ============================================================
-- VietnamWorks Data Pipeline - PostgreSQL Schema
-- Phase 4: DbLoader writes vào đây
-- Dùng star schema theo style đã làm với Olist
-- ============================================================

-- NOTE: Script này chạy trong container pipeline-postgres
--       (port 5433, user=pipeline, db=vietnamworks)
--       KHÔNG phải airflow-postgres (port 5432, user=airflow)

-- =====================
-- DIMENSION TABLES
-- =====================

CREATE TABLE IF NOT EXISTS dim_company (
    company_id      SERIAL PRIMARY KEY,
    company_name    VARCHAR(255) NOT NULL,
    company_slug    VARCHAR(255),
    industry        VARCHAR(255),
    company_size    VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(company_slug)
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    district        VARCHAR(100),
    province        VARCHAR(100),
    country         VARCHAR(100) DEFAULT 'Vietnam',
    UNIQUE(city, district)
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id     SERIAL PRIMARY KEY,
    category_name   VARCHAR(255) NOT NULL,
    parent_category VARCHAR(255),
    UNIQUE(category_name)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             INT,
    month           INT,
    quarter         INT,
    year            INT,
    UNIQUE(full_date)
);

-- =====================
-- FACT TABLE
-- =====================

CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id              SERIAL PRIMARY KEY,
    -- Source tracking
    source_url          TEXT NOT NULL,
    raw_file_id         VARCHAR(50),        -- SeaweedFS file ID
    scraped_at          TIMESTAMP,
    -- Dimensions FK
    company_id          INT REFERENCES dim_company(company_id),
    location_id         INT REFERENCES dim_location(location_id),
    category_id         INT REFERENCES dim_category(category_id),
    post_date_id        INT REFERENCES dim_date(date_id),
    deadline_date_id    INT REFERENCES dim_date(date_id),
    -- Job facts
    job_title           VARCHAR(500),
    salary_min          BIGINT,
    salary_max          BIGINT,
    salary_currency     VARCHAR(10) DEFAULT 'VND',
    salary_negotiable   BOOLEAN DEFAULT FALSE,
    experience_years    INT,
    job_level           VARCHAR(100),
    job_type            VARCHAR(100),   -- Full-time, Part-time, etc.
    required_skills     TEXT[],
    job_description     TEXT,
    job_requirements    TEXT,
    benefits            TEXT,
    is_hot              BOOLEAN DEFAULT FALSE,
    views_count         INT DEFAULT 0,
    -- Pipeline metadata
    pipeline_run_id     VARCHAR(100),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- =====================
-- PIPELINE TRACKING
-- =====================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR(100) PRIMARY KEY,
    dag_id          VARCHAR(255),
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    status          VARCHAR(50) DEFAULT 'running',
    jobs_seeded     INT DEFAULT 0,
    jobs_scraped    INT DEFAULT 0,
    jobs_parsed     INT DEFAULT 0,
    jobs_loaded     INT DEFAULT 0,
    error_message   TEXT
);

-- =====================
-- INDEXES
-- =====================

CREATE INDEX idx_fact_jobs_company ON fact_jobs(company_id);
CREATE INDEX idx_fact_jobs_location ON fact_jobs(location_id);
CREATE INDEX idx_fact_jobs_category ON fact_jobs(category_id);
CREATE INDEX idx_fact_jobs_post_date ON fact_jobs(post_date_id);
CREATE INDEX idx_fact_jobs_scraped_at ON fact_jobs(scraped_at);

-- =====================
-- VIEWS tiện theo dõi
-- =====================

CREATE VIEW v_jobs_full AS
SELECT
    f.job_id,
    f.job_title,
    c.company_name,
    c.industry,
    l.city,
    cat.category_name,
    f.salary_min,
    f.salary_max,
    f.salary_negotiable,
    f.experience_years,
    f.job_type,
    f.job_level,
    f.required_skills,
    d.full_date AS post_date,
    f.source_url,
    f.loaded_at
FROM fact_jobs f
LEFT JOIN dim_company c ON f.company_id = c.company_id
LEFT JOIN dim_location l ON f.location_id = l.location_id
LEFT JOIN dim_category cat ON f.category_id = cat.category_id
LEFT JOIN dim_date d ON f.post_date_id = d.date_id;

COMMENT ON TABLE fact_jobs IS 'Fact table chứa tất cả job listings từ VietnamWorks';
COMMENT ON TABLE dim_company IS 'Dimension table cho thông tin công ty';
COMMENT ON VIEW v_jobs_full IS 'View tổng hợp join tất cả dimensions - dùng để query trong PgAdmin';