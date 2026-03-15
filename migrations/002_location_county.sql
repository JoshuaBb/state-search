-- Replace state_code/state_name with county on dim_location.
ALTER TABLE dim_location
    DROP COLUMN state_code,
    DROP COLUMN state_name,
    ADD COLUMN county TEXT;

DROP INDEX IF EXISTS dim_location_uq;
CREATE UNIQUE INDEX dim_location_uq ON dim_location (county, country, zip_code) NULLS NOT DISTINCT;
