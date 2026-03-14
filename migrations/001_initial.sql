-- Source registry: one row per CSV data source.
CREATE TABLE import_sources (
    id          SERIAL PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    description TEXT,
    field_map   JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw CSV rows stored verbatim alongside normalized forms.
CREATE TABLE raw_imports (
    id              BIGSERIAL PRIMARY KEY,
    source_id       INT REFERENCES import_sources (id) ON DELETE SET NULL,
    source_file     TEXT,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_data        JSONB NOT NULL,
    normalized_data JSONB
);

CREATE INDEX raw_imports_source_idx ON raw_imports (source_id);
CREATE INDEX raw_imports_raw_gin    ON raw_imports USING GIN (raw_data);
CREATE INDEX raw_imports_norm_gin   ON raw_imports USING GIN (normalized_data);

-- Location dimension (all geographic fields nullable).
CREATE TABLE dim_location (
    id         BIGSERIAL PRIMARY KEY,
    state_code CHAR(2),
    state_name TEXT,
    country    CHAR(3) NOT NULL DEFAULT 'USA',
    zip_code   TEXT,
    fips_code  TEXT,
    latitude   DOUBLE PRECISION,
    longitude  DOUBLE PRECISION
);

CREATE UNIQUE INDEX dim_location_uq ON dim_location (state_code, country, zip_code) NULLS NOT DISTINCT;

-- Time dimension (year required; quarter/month/day nullable and independent).
CREATE TABLE dim_time (
    id         BIGSERIAL PRIMARY KEY,
    year       SMALLINT NOT NULL,
    quarter    SMALLINT CHECK (quarter BETWEEN 1 AND 4),
    month      SMALLINT CHECK (month   BETWEEN 1 AND 12),
    day        SMALLINT CHECK (day     BETWEEN 1 AND 31),
    date_floor DATE
);

CREATE UNIQUE INDEX dim_time_uq ON dim_time (year, quarter, month, day) NULLS NOT DISTINCT;

-- Fact table: one row per metric value per location+time+source.
CREATE TABLE fact_observations (
    id            BIGSERIAL PRIMARY KEY,
    raw_import_id BIGINT REFERENCES raw_imports  (id) ON DELETE SET NULL,
    location_id   BIGINT REFERENCES dim_location (id) ON DELETE SET NULL,
    time_id       BIGINT REFERENCES dim_time     (id) ON DELETE SET NULL,
    source_name   TEXT,
    metric_name   TEXT    NOT NULL,
    metric_value  NUMERIC,
    attributes    JSONB
);

CREATE INDEX fact_metric_idx    ON fact_observations (metric_name);
CREATE INDEX fact_source_idx    ON fact_observations (source_name, metric_name);
CREATE INDEX fact_location_idx  ON fact_observations (location_id);
CREATE INDEX fact_time_idx      ON fact_observations (time_id);
CREATE INDEX fact_attrs_gin     ON fact_observations USING GIN (attributes);
