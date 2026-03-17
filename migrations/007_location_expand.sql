-- migrations/007_location_expand.sql
-- Expand dim_location to support zip-code-level CSV data.
-- Re-adds state_code / state_name (dropped in 002) and adds new columns.

ALTER TABLE dim_location
    ADD COLUMN IF NOT EXISTS city             TEXT,
    ADD COLUMN IF NOT EXISTS state_code       TEXT,
    ADD COLUMN IF NOT EXISTS state_name       TEXT,
    ADD COLUMN IF NOT EXISTS zcta             TEXT,
    ADD COLUMN IF NOT EXISTS parent_zcta      TEXT,
    ADD COLUMN IF NOT EXISTS population       FLOAT8,
    ADD COLUMN IF NOT EXISTS density          FLOAT8,
    ADD COLUMN IF NOT EXISTS county_weights   JSONB,
    ADD COLUMN IF NOT EXISTS county_names_all TEXT,
    ADD COLUMN IF NOT EXISTS county_fips_all  TEXT,
    ADD COLUMN IF NOT EXISTS imprecise        BOOLEAN,
    ADD COLUMN IF NOT EXISTS military         BOOLEAN,
    ADD COLUMN IF NOT EXISTS timezone         TEXT;
