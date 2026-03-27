-- ============================================================
-- Stats Bluenoser — Database Schema
-- ============================================================
-- Nova Scotia Economic Data Platform
-- Run: psql "$DATABASE_URL" -f db/schema.sql
-- ============================================================

-- ============================================================
-- DATA LAYER — Statistics Canada and other source data
-- ============================================================

-- Sources: where data comes from
CREATE TABLE IF NOT EXISTS sources (
    source_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    api_base_url TEXT,
    api_type TEXT,
    licence_url TEXT,
    config_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tables/Cubes: data tables we track from each source
CREATE TABLE IF NOT EXISTS data_tables (
    table_id SERIAL PRIMARY KEY,
    source_id INT REFERENCES sources(source_id),
    source_pid TEXT NOT NULL,
    title TEXT NOT NULL,
    frequency TEXT,
    subject_code TEXT,
    survey_code TEXT,
    last_source_update TIMESTAMPTZ,
    metadata_json JSONB,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_id, source_pid)
);

-- Series: individual time series within a table
CREATE TABLE IF NOT EXISTS series (
    series_id SERIAL PRIMARY KEY,
    table_id INT REFERENCES data_tables(table_id),
    vector_id TEXT,
    coordinate TEXT,
    geo_name TEXT,
    geo_code TEXT,
    unit_of_measure TEXT,
    scalar_factor INT DEFAULT 0,
    description TEXT,
    dimension_labels JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(table_id, vector_id)
);

-- Data points: the actual time-series values
CREATE TABLE IF NOT EXISTS data_points (
    series_id INT REFERENCES series(series_id),
    ref_period DATE NOT NULL,
    value NUMERIC,
    status_code TEXT,
    symbol_code TEXT,
    decimal_precision INT,
    release_date TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (series_id, ref_period)
);

-- Revisions: track when a source revises a previously published value
CREATE TABLE IF NOT EXISTS revisions (
    revision_id SERIAL PRIMARY KEY,
    series_id INT REFERENCES series(series_id),
    ref_period DATE NOT NULL,
    previous_value NUMERIC,
    new_value NUMERIC,
    detected_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CONTENT LAYER — AI-generated and human-reviewed analysis
-- ============================================================

-- Topics: categorization matching NS Finance's 24-category taxonomy
CREATE TABLE IF NOT EXISTS topics (
    topic_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE
);

-- Releases: analytical content (AI-generated and/or human-written)
CREATE TABLE IF NOT EXISTS releases (
    release_id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    topic_id INT REFERENCES topics(topic_id),
    ref_period TEXT,
    geography_scope TEXT,
    body_markdown TEXT NOT NULL,
    body_html TEXT,
    charts_json JSONB,
    source_table_pids TEXT[],
    ai_model TEXT,
    ai_generated BOOLEAN DEFAULT TRUE,
    reviewed BOOLEAN DEFAULT FALSE,
    published BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    significance_score NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Link releases to the series they reference
CREATE TABLE IF NOT EXISTS release_series (
    release_id INT REFERENCES releases(release_id),
    series_id INT REFERENCES series(series_id),
    PRIMARY KEY (release_id, series_id)
);

-- Newsletter sends: log of what was emailed and when
CREATE TABLE IF NOT EXISTS newsletter_sends (
    send_id SERIAL PRIMARY KEY,
    subject TEXT NOT NULL,
    body_html TEXT,
    release_ids INT[],
    recipient_count INT,
    sent_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_data_points_period ON data_points(ref_period DESC);
CREATE INDEX IF NOT EXISTS idx_data_points_series_period ON data_points(series_id, ref_period DESC);
CREATE INDEX IF NOT EXISTS idx_series_vector ON series(vector_id);
CREATE INDEX IF NOT EXISTS idx_series_geo ON series(geo_name);
CREATE INDEX IF NOT EXISTS idx_series_table ON series(table_id);
CREATE INDEX IF NOT EXISTS idx_releases_published ON releases(published_at DESC) WHERE published = TRUE;
CREATE INDEX IF NOT EXISTS idx_releases_topic ON releases(topic_id);
CREATE INDEX IF NOT EXISTS idx_data_tables_pid ON data_tables(source_pid);
CREATE INDEX IF NOT EXISTS idx_data_tables_source ON data_tables(source_id);
CREATE INDEX IF NOT EXISTS idx_revisions_series ON revisions(series_id, ref_period);
