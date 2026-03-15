-- migrations/005_normalized_imports.sql

-- Drop old fact tables
DROP TABLE IF EXISTS fact_observations;
DROP TABLE IF EXISTS fact_row_context;

-- Drop normalized_data column and its index from raw_imports
DROP INDEX IF EXISTS raw_imports_norm_gin;
ALTER TABLE raw_imports DROP COLUMN IF EXISTS normalized_data;

-- Drop field_map from import_sources
ALTER TABLE import_sources DROP COLUMN IF EXISTS field_map;

-- Per-source canonical field type definitions
CREATE TABLE import_schema (
    id          SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    field_name  TEXT NOT NULL,
    field_type  TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, field_name)
);

CREATE INDEX import_schema_source_idx ON import_schema (source_name);

-- One row per CSV row: dim FKs + non-dimension typed fields as JSONB
CREATE TABLE normalized_imports (
    id              BIGSERIAL PRIMARY KEY,
    raw_import_id   BIGINT REFERENCES raw_imports(id) ON DELETE SET NULL,
    location_id     BIGINT REFERENCES dim_location(id) ON DELETE SET NULL,
    time_id         BIGINT REFERENCES dim_time(id) ON DELETE SET NULL,
    source_name     TEXT NOT NULL,
    ingest_run_id   UUID NOT NULL,
    normalized_data JSONB NOT NULL DEFAULT '{}',
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX norm_imports_source_idx   ON normalized_imports (source_name);
CREATE INDEX norm_imports_run_idx      ON normalized_imports (ingest_run_id);
CREATE INDEX norm_imports_location_idx ON normalized_imports (location_id);
CREATE INDEX norm_imports_time_idx     ON normalized_imports (time_id);
CREATE INDEX norm_imports_data_gin     ON normalized_imports USING GIN (normalized_data);
