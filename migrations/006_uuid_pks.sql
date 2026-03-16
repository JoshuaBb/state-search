-- migrations/006_uuid_pks.sql
-- Requires Postgres 13+ (gen_random_uuid() is built-in; no pgcrypto needed)

-- Step 1: drop both FK constraints before altering referenced columns
ALTER TABLE normalized_imports DROP CONSTRAINT IF EXISTS normalized_imports_location_id_fkey;
ALTER TABLE normalized_imports DROP CONSTRAINT IF EXISTS normalized_imports_time_id_fkey;

-- Step 2: drop content-based unique indexes (replaced by UUID PK uniqueness)
DROP INDEX IF EXISTS dim_location_uq;   -- was (county, country, zip_code)
DROP INDEX IF EXISTS dim_time_uq;       -- was (year, quarter, month, day)

-- Step 3: migrate dim_location PK bigint → uuid
ALTER TABLE dim_location ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE dim_location ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 4: migrate dim_time PK bigint → uuid
ALTER TABLE dim_time ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE dim_time ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 5: migrate normalized_imports FK columns to uuid
--         existing bigint values cannot be cast to uuid — NULL them out
--         (historical dimension links are discarded by this migration)
ALTER TABLE normalized_imports ALTER COLUMN location_id TYPE uuid USING NULL::uuid;
ALTER TABLE normalized_imports ALTER COLUMN time_id     TYPE uuid USING NULL::uuid;

-- Step 6: re-add FK constraints
ALTER TABLE normalized_imports
    ADD CONSTRAINT normalized_imports_location_id_fkey
    FOREIGN KEY (location_id) REFERENCES dim_location(id);
ALTER TABLE normalized_imports
    ADD CONSTRAINT normalized_imports_time_id_fkey
    FOREIGN KEY (time_id) REFERENCES dim_time(id);

-- Step 7: migrate normalized_imports PK bigint → uuid (after FKs are re-established)
ALTER TABLE normalized_imports ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE normalized_imports ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 8: dim_file_log — append-only log of dim ingest runs
CREATE TABLE dim_file_log (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name  TEXT        NOT NULL,
    target       TEXT        NOT NULL,  -- serde snake_case: "dim_location" | "dim_time"
    file_path    TEXT        NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count    BIGINT      NOT NULL
);
