# Migration History

Migrations live in `migrations/` and are applied automatically on API startup via `sqlx::migrate!()`. They run in filename order and are idempotent (SQLx tracks applied migrations in `_sqlx_migrations`).

---

## 001 — Initial schema

**File:** `001_initial.sql`

Establishes the core star schema:

- **`import_sources`** — registry of CSV data sources (name, description, field_map).
- **`raw_imports`** — stores every original CSV row verbatim as `raw_data` JSONB alongside a `normalized_data` JSONB column for the post-mapping form. GIN indexes on both JSONB columns.
- **`dim_location`** — location dimension with `state_code`, `state_name`, `country`, `zip_code`, `fips_code`, lat/lon. Unique on `(state_code, country, zip_code) NULLS NOT DISTINCT`.
- **`dim_time`** — time dimension with `year` (required), `quarter`, `month`, `day`, `date_floor`. Unique on `(year, quarter, month, day) NULLS NOT DISTINCT`.
- **`fact_observations`** — one row per metric value per location + time + source. Columns: `metric_name`, `metric_value` (NUMERIC), `source_name`, FKs to `dim_location`, `dim_time`, `raw_imports`, plus `attributes` JSONB.

---

## 002 — Location: replace state columns with county

**File:** `002_location_county.sql`

The initial schema modeled locations at the state level (`state_code`, `state_name`). This migration generalizes it to support sub-state granularity:

- Drops `state_code` and `state_name` from `dim_location`.
- Adds `county TEXT`.
- Rebuilds the unique index on `(county, country, zip_code) NULLS NOT DISTINCT`.

State-level data is now represented by a `county` value of `NULL` (or the state name if provided via the `county` canonical field).

---

## 003 — Observations: ingest run tracking

**File:** `003_observation_ingest_run.sql`

Adds partial-failure recovery to `fact_observations`:

- `ingest_run_id UUID` — every observation written in a single ingest run shares the same UUID.
- `inserted_at TIMESTAMPTZ` — row creation timestamp.
- Index on `ingest_run_id` for fast cleanup queries.

If an ingest run fails mid-file, all observations for that run can be deleted by `ingest_run_id` without affecting data from other runs. This avoids long-lived file-level transactions on large imports.

See [docs/decisions/002-ingest-run-id.md](decisions/002-ingest-run-id.md) for the reasoning.

---

## 004 — Row context table

**File:** `004_row_context.sql`

Adds `fact_row_context` — a table for storing per-row metadata (attributes JSONB) keyed by source name. Also adds a `context_id` FK on `fact_observations` pointing to `fact_row_context`.

This was an intermediate design step for separating observation context from metric values. It was superseded by migration 005.

---

## 005 — Normalized imports (current schema)

**File:** `005_normalized_imports.sql`

Major schema refactor replacing the observation model:

**Removed:**
- `fact_observations` — dropped entirely.
- `fact_row_context` — dropped entirely.
- `normalized_data` column from `raw_imports` — redundant after this migration.
- `field_map` column from `import_sources` — moved to `config/sources.toml`.

**Added:**
- **`import_schema`** — per-source field type definitions (`source_name`, `field_name`, `field_type`, `description`). Seeded by the ingest pipeline on first run for each source.
- **`normalized_imports`** — one row per ingested CSV row with dimension FKs resolved (`location_id`, `time_id`) and all remaining (non-dimension) fields stored in `normalized_data` JSONB. Tagged with `ingest_run_id` for cleanup. GIN index on `normalized_data` for JSON queries.

This design stores the full row in one place rather than exploding it into per-metric rows, which simplifies queries and reduces row counts for wide CSVs.

See [docs/decisions/001-star-schema.md](decisions/001-star-schema.md) for the architectural rationale.

---

## Current schema summary

| Table | Description |
|-------|-------------|
| `import_sources` | Source registry (name, description) |
| `raw_imports` | Original CSV rows as JSONB |
| `dim_location` | Location dimension (county, country, zip, FIPS, lat/lon) |
| `dim_time` | Time dimension (year, quarter, month, day) |
| `import_schema` | Per-source field type definitions |
| `normalized_imports` | Normalized rows with dim FKs + remaining data as JSONB |
