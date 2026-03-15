# Normalized Imports Pipeline Redesign

**Date:** 2026-03-15
**Status:** Draft

## Problem

The current pipeline has several interrelated design issues:

1. **`raw_imports` stores both raw and normalized data** — `raw_data` and `normalized_data` live in the same row, making the audit boundary unclear and creating redundancy.
2. **No explicit type schema** — type coercion is implicit in the Rust `FieldValue` transform chain; there is no queryable record of what type each canonical field should be.
3. **Data redundancy between `fact_observations` and `fact_row_context`** — non-metric string fields (context) and metric numeric fields are split across two tables but both derive from the same CSV row, creating a join requirement for any full-row reconstruction.
4. **`fact_observations` is a tall table** — one row per metric-value pair forces analytics tools to pivot; DuckDB and BI tools work better against wide, typed rows.
5. **`sources.toml` mixes concerns** — field rename (`field_map`) is separate from type info, requiring two places to maintain per field.

## Goals

- Clean audit boundary: `raw_imports` = verbatim CSV rows, `normalized_imports` = post-transform typed rows.
- Explicit, per-source type schema seeded from source configuration.
- Single normalized row per CSV row — no tall metric decomposition.
- Strict schema enforcement: rows with unknown or uncastable fields fail rather than silently skipping.
- Replayability: re-normalize from `raw_imports` at any time by re-ingesting with an updated schema.
- DuckDB/BI tools query `normalized_imports` directly; a future replication process may materialize analytics tables from the same data.

## Out of Scope

- Backfilling existing `raw_imports` rows into `normalized_imports`.
- Materialized analytics tables (future replication process).
- Arrow-native dual-write pipeline.
- Dimension Parquet refresh scheduling.
- API route changes beyond `import_sources.field_map` column removal.

---

## Schema Changes

### Tables Removed

- `fact_observations` — dropped entirely.
- `fact_row_context` — dropped entirely.
- `raw_imports.normalized_data` column — dropped (raw-only going forward).
- `raw_imports_norm_gin` index — dropped (was on `normalized_data`).
- `import_sources.field_map` column — dropped (field definitions move to `sources.yml` + `import_schema`).

### `import_sources` Table

`import_sources` is kept (the API routes `GET /api/sources`, `POST /api/sources`, `GET /api/sources/:name` continue to work). The `field_map JSONB` column is dropped since field definitions now live in `sources.yml` and are seeded into `import_schema`. The `name`, `description`, `created_at` columns remain. The API no longer returns `field_map` data.

### Tables Added

#### `import_schema`

Per-source type definitions for canonical field names. The pipeline seeds this from `sources.yml` at run start and validates all expected fields are present before processing any rows.

```sql
CREATE TABLE import_schema (
    id          SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    field_name  TEXT NOT NULL,   -- canonical name (e.g. "year", "state_code")
    field_type  TEXT NOT NULL,   -- postgres type string: "numeric", "smallint", "text", etc.
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, field_name)
);

CREATE INDEX import_schema_source_idx ON import_schema (source_name);
```

#### `normalized_imports`

One row per CSV row. Stores dimension FKs for indexed geo/time queries in Postgres, plus all non-dimension canonical fields as a typed JSONB blob.

```sql
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
```

`normalized_data` holds all **non-dimension** canonical fields — fields that are not location or time dimensions (i.e. not in the existing `LOCATION_FIELDS`/`TIME_FIELDS` skip-sets). Dimension fields declared in `sources.yml` are used for dim upserts and FK assignment but are not written into `normalized_data`. See the "Dimension Fields in `fields` Block" note below.

### Migration: `005_normalized_imports.sql`

```sql
-- Drop old fact tables
DROP TABLE IF EXISTS fact_observations;
DROP TABLE IF EXISTS fact_row_context;

-- Drop normalized_data column and its index from raw_imports
DROP INDEX IF EXISTS raw_imports_norm_gin;
ALTER TABLE raw_imports DROP COLUMN IF EXISTS normalized_data;

-- Drop field_map from import_sources (field definitions move to sources.yml + import_schema)
ALTER TABLE import_sources DROP COLUMN IF EXISTS field_map;

-- Add new tables
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
```

No data migration is required. Existing `raw_imports` rows remain as the audit trail. Re-ingest from CSV to populate `normalized_imports`.

---

## Source Configuration: `sources.yml`

`config/sources.toml` is replaced by `config/sources.yml`. The `field_map` and type schema are combined into a single `fields` block per source, eliminating the dual-maintenance problem. Transform rules remain per-field. A new `derived` block handles fields looked up from dimension tables rather than read from the CSV.

The `config` crate supports YAML via the `config` crate's `yaml` feature or via direct `serde_yaml` deserialization (matching the existing pattern where `sources.toml` is loaded separately with `toml::from_str`). Switch to `serde_yaml::from_str`.

```yaml
sources:
  - name: co_public_drinking_water
    description: Colorado public drinking water quality data
    files:
      - data/co_drinking_water_2024.csv

    fields:
      # YAML key = source CSV column name
      # canonical = renamed-to canonical field name
      # type = postgres type string (stored in import_schema)
      # format = optional, only valid for date/datetime types
      # on_failure = optional, field-level failure policy (ignore | skip_row | skip_dataset)
      # rules = optional list of transform rules (kind + optional on_failure)
      ReportYear:
        canonical: year
        type: smallint
        rules:
          - kind: trim
      StateCode:
        canonical: state_code
        type: text
        rules:
          - kind: trim
          - kind: uppercase
      AnalyteName:
        canonical: analyte_name
        type: text
      AverageValue:
        canonical: average_value
        type: numeric
      Units:
        canonical: units
        type: text
      SampleDate:
        canonical: sample_date
        type: date
        format: month_day_year
        on_failure: skip_row

    derived:
      # Fields not in the CSV — derived from dimension table lookups after dim upserts.
      # If the prerequisite dim FK is null (dim upsert failed), the row fails before
      # derived resolution is attempted.
      # If the lookup finds no matching row, the row fails.
      county_name:
        from: dim_location
        match: fips_code     # canonical field in the transformed row to match on
        output: county       # column to retrieve from the dim table
        type: text
```

### `fields` block semantics

Each YAML key is the **source CSV column name**. Each entry defines:
- `canonical` — the canonical field name used in `import_schema` and `normalized_data`.
- `type` — postgres type string, seeded into `import_schema`.
- `format` — optional; only valid for `date`/`datetime` types. Validated at `build_resolved_field_map` time (same as today).
- `on_failure` — optional field-level failure policy (`ignore`, `skip_row`, `skip_dataset`). Same semantics as the existing `FieldDef.on_failure`.
- `rules` — optional list of transform rules (`kind` + optional `on_failure`), same as the existing `RuleDef` list.

The pipeline derives the resolved field map from this structure. The YAML key maps to `FieldDef.source` (the CSV column). `FieldDef.canonical` replaces the old HashMap key.

The `FieldType` enum in `config.rs` is **replaced** by a `String` (`type` field) that maps directly to a Postgres type name for storage in `import_schema`. The internal Rust coercion type is inferred from this string at resolve time, replacing the old `FieldType` enum variants.

### `derived` block semantics

Defines fields resolved from dimension tables after dim upserts. Each entry:
- `from` — dimension table to query (`dim_location` or `dim_time`).
- `match` — canonical field already in the transformed row to use as the lookup key.
- `output` — column to retrieve from the dimension table.
- `type` — postgres type string, seeded into `import_schema` for this canonical field.

Derived fields land in `normalized_data` alongside directly-mapped non-dimension fields.

### Dimension Fields in `fields` Block

Dimension fields (those in the existing `LOCATION_FIELDS`/`TIME_FIELDS` skip-sets: `state_code`, `state_name`, `country`, `zip_code`, `fips_code`, `latitude`, `longitude`, `year`, `quarter`, `month`, `day`) **must** be declared in the `fields` block because:
1. Their `canonical` + `type` pairs need to be seeded into `import_schema`.
2. The transform pipeline needs their rules.

However, they are **excluded from `normalized_data`** — the existing skip-set logic is preserved in the new `cast_row` / data collection step. They are consumed by `resolve_location_id` / `resolve_time_id` and stored as FK references on `normalized_imports` instead.

---

## Pipeline Changes

### Startup: Schema Seed and Validation

Before processing any CSV rows:

1. Load `sources.yml` for the target source.
2. Collect all `fields` (by `canonical` + `type`) and all `derived` entries (by derived key + `type`) into a flat list of `NewImportSchemaField`.
3. Upsert all entries into `import_schema` for that source: insert missing rows; on conflict update `field_type` to the value from `sources.yml` (schema changes in config propagate automatically). Do not overwrite `created_at` or `description`.
4. Query `import_schema` for the source and verify the full expected set of canonical field names is present.
5. If any expected field is absent after the upsert, the run fails immediately with a clear error — no CSV rows are processed.

### Per-Row Processing (`process_record`)

The per-row pipeline changes as follows:

**Before (current):**
```
1. row_to_canonical
2. INSERT raw_imports (raw_data + normalized_data)
3. apply_all_transforms + with_passthrough
4. resolve_location_id + resolve_time_id (tokio::join!)
5. extract_context → INSERT fact_row_context → context_id
6. extract_metrics
7. build_observations (→ batched INSERT fact_observations)
```

**After:**
```
1. row_to_canonical
2. INSERT raw_imports (raw_data only) RETURNING id → raw_import_id
3. apply_all_transforms (with_passthrough removed — schema is strict)
4. validate_completeness (all required schema fields present → row fails if any missing)
5. resolve_location_id + resolve_time_id (tokio::join!, unchanged)
6. check dim prerequisites (if any derived field requires dim_location and location_id is None → row fails)
7. resolve_derived_fields (dim lookups → row fails on no match)
8. collect_normalized_data (cast FieldValues to schema types; skip dimension fields)
9. produce NewNormalizedImport → batched INSERT normalized_imports
```

Steps 3–9 produce one `NewNormalizedImport` per row. The `batched_channel_sink` flushes these in batches to `normalized_imports`.

#### Step 2: Capturing `raw_import_id`

The current `INSERT raw_imports` is a fire-and-forget `.execute()`. This must change to `.fetch_one()` with `RETURNING id`:

```sql
INSERT INTO raw_imports (source_id, source_file, raw_data)
VALUES ($1, $2, $3)
RETURNING id
```

The returned `i64` is passed as `raw_import_id` on `NewNormalizedImport`.

#### Step 3: `apply_all_transforms` and `RowOutcome`

`apply_all_transforms` is unchanged in structure — it folds over `resolved` (the resolved field map derived from `fields`). `with_passthrough` is removed: since the resolved map now covers all declared source columns, no unmapped columns exist to pass through.

`RowOutcome::Skip` (from `OnFailure::SkipRow`) and `RowOutcome::Abort` (from `OnFailure::SkipDataset`) remain fully in effect. Per-field and per-rule `on_failure` values from `sources.yml` still drive these outcomes. The existing `SkipDataset` → abort-entire-file behavior is preserved.

#### Step 4: `validate_completeness`

Checks that all canonical fields declared in `import_schema` for this source — including dimension fields — are present in the transformed row after step 3. This fires when a declared field's source column was missing from the CSV headers, not when an unknown field is present (since `with_passthrough` removal makes that impossible).

Dimension fields (e.g. `year`, `state_code`) participate in completeness validation the same as non-dimension fields. If a dimension field's source column is absent, the row fails here before reaching the dim upsert step — a cleaner failure message than a null FK from a missing input.

Derived fields are **excluded** from completeness validation — they are not in the CSV and are resolved at step 7.

If any required canonical field is absent: row fails (treated as `SkipRow`), logged with row number and missing field name. Ingest continues.

#### Step 6: Dim Prerequisite Check

Before calling `resolve_derived_fields`, check whether each `derived` entry's prerequisite dim FK is non-null:
- If a derived field uses `from: dim_location` and `location_id` is `None` (dim upsert failed silently at step 5): row fails immediately with a clear message ("location dim upsert failed at row N; cannot resolve derived field X"). Ingest continues.
- Same for `from: dim_time` and `time_id`.

This prevents a misleading "no matching dim row" failure message that would obscure the real cause.

#### Step 8: `collect_normalized_data`

Builds the `normalized_data` JSONB map from the transformed row and the derived fields already resolved in step 7:
- Iterates canonical fields from `import_schema` for this source.
- Skips dimension fields (those in `LOCATION_FIELDS` / `TIME_FIELDS` skip-sets) — these are represented as dim FKs on the row.
- Skips derived fields — these were already written into `normalized_data` by step 7.
- For each remaining non-dimension, non-derived field, casts the `FieldValue` to the declared Postgres type and inserts into the JSONB map.
- Merges with the derived fields already in `normalized_data` to produce the final blob.
- Type cast failure (value cannot be represented as declared type): row fails (treated as `SkipRow`), logged with row number, field name, value, and declared type. Ingest continues.

### Failure Cleanup

`delete_by_ingest_run` now targets `normalized_imports` instead of `fact_observations`. The same partial-failure rollback pattern applies: if `process_rows` returns an error, all `normalized_imports` rows for that `ingest_run_id` are deleted.

### Export SQL

The post-ingest DuckDB export SQL is updated to join `normalized_imports` with dimensions. Dimension fields are read from the joined dim tables (not from `normalized_data`). Year is sourced from `dim_time` for `PARTITION_BY`.

```sql
COPY (
    SELECT
        n.source_name,
        n.ingest_run_id,
        l.state_code,
        l.county,
        l.fips_code,
        t.year,
        t.quarter,
        t.month,
        -- Flatten source-specific normalized_data fields
        (n.normalized_data->>'analyte_name')             AS analyte_name,
        (n.normalized_data->>'average_value')::numeric   AS average_value,
        (n.normalized_data->>'units')                    AS units
    FROM pg.normalized_imports n
    LEFT JOIN pg.dim_location l ON l.id = n.location_id
    LEFT JOIN pg.dim_time     t ON t.id = n.time_id
    WHERE n.ingest_run_id = 'XXXX-...'
      AND n.source_name   = 'co_public_drinking_water'
) TO 'exports/co_public_drinking_water/year=2024/part-0001.parquet'
   (FORMAT PARQUET, PARTITION_BY (year));
```

The `attributes` list on `SourceConfig` is removed. `generate_export_sql` in `export.rs` now reads the non-dimension canonical fields from the source's `import_schema` (or equivalently from the `fields` block in config) to generate the `normalized_data->>'key' AS key` projection. The `write_export_script` call in `mod.rs` no longer passes `&source.attributes` — `export.rs` derives the projection from the loaded schema instead.

---

## Config: `SourceConfig` Changes (`core/src/config.rs`)

### Updated `FieldDef`

The old `FieldDef` had `source: String` (CSV column name) as an inner field, with the canonical name as the HashMap key. The new structure inverts this — the YAML key is the CSV column, and `canonical` is the inner field:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub canonical: String,         // replaces the old HashMap key
    pub field_type: String,        // postgres type string ("smallint", "text", "numeric", etc.)
                                   // replaces the FieldType enum
    pub format: Option<String>,    // retained — only valid for date/datetime
    #[serde(default)]
    pub on_failure: Option<OnFailure>,  // retained — field-level failure policy
    #[serde(default)]
    pub rules: Vec<RuleDef>,       // retained — unchanged
}
```

The `FieldType` enum is removed from `config.rs`. The `field_type: String` maps to Postgres type names (`"smallint"`, `"text"`, `"numeric"`, `"date"`, `"timestamptz"`, etc.). The `build_resolved_field_map` function maps these strings to the appropriate `FieldValue` coercion target, replacing the old `FieldType` variant dispatch.

### Updated `DerivedFieldDef` (new)

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct DerivedFieldDef {
    pub from: String,                    // dim table: "dim_location" | "dim_time"
    #[serde(rename = "match")]
    pub match_field: String,             // canonical field to look up by
                                         // YAML key is `match` (readable); renamed to avoid
                                         // collision with Rust keyword
    pub output: String,                  // dim column to retrieve
    pub field_type: String,              // postgres type string (seeded into import_schema)
}
```

### Updated `SourceConfig`

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    pub fields: HashMap<String, FieldDef>,          // CSV col → FieldDef; replaces field_map
    #[serde(default)]
    pub derived: HashMap<String, DerivedFieldDef>,  // canonical_name → DerivedFieldDef
    // `attributes: Vec<String>` removed
    // `field_map` removed
}
```

### Config loading

`AppConfig::load()` in `config.rs` switches `sources.toml` loading from `toml::from_str` to `serde_yaml::from_str` (or equivalent). The file path changes from `config/sources.toml` to `config/sources.yml`. The `serde_yaml` crate is added to the workspace.

### `build_resolved_field_map`

Updated to iterate `source.fields` (HashMap<String, FieldDef>) where the key is the CSV column (`FieldDef.source` in the old system). `FieldDef.canonical` becomes the canonical key in `ResolvedFieldMap`. `ResolvedFieldMap` structure is otherwise unchanged.

---

## Models (`core/src/models/`)

### New: `normalized_import.rs`

```rust
pub struct NewNormalizedImport {
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<i64>,
    pub time_id:         Option<i64>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
}

pub struct NormalizedImport {
    pub id:              i64,
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<i64>,
    pub time_id:         Option<i64>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
    pub inserted_at:     DateTime<Utc>,
}
```

### New: `import_schema_field.rs`

```rust
pub struct ImportSchemaField {
    pub id:          i32,
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
}

pub struct NewImportSchemaField {
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
}
```

### Removed

- `models/observation.rs` — `Observation`, `NewObservation`
- `models/row_context.rs` — `RowContext`, `NewRowContext`

---

## Repositories (`core/src/repositories/`)

### New: `normalized_import.rs`

```rust
/// Bulk insert normalized import rows (UNNEST pattern, matching current bulk_create).
pub async fn bulk_create(db: &Db, rows: Vec<NewNormalizedImport>) -> Result<()>

/// Delete all normalized imports for a given ingest run (partial-failure cleanup).
pub async fn delete_by_ingest_run(db: &Db, ingest_run_id: Uuid) -> Result<u64>
```

### New: `import_schema.rs`

```rust
/// Upsert schema field definitions for a source (seed from sources.yml).
/// ON CONFLICT (source_name, field_name) DO UPDATE SET field_type = EXCLUDED.field_type
/// — field_type is always overwritten so config changes propagate without manual DB edits.
/// created_at and description are not updated on conflict.
pub async fn upsert_fields(db: &Db, fields: Vec<NewImportSchemaField>) -> Result<()>

/// Fetch all field definitions for a source, keyed by canonical field name → type string.
pub async fn load_for_source(db: &Db, source_name: &str) -> Result<HashMap<String, String>>
```

### Removed

- `repositories/observation.rs`
- `repositories/row_context.rs`

---

## Pipeline Modules (`ingest/src/pipeline/`)

### New: `schema.rs`

```rust
/// Seed import_schema from the source's fields/derived definitions, then validate
/// the full set is present. Returns the loaded schema: canonical_name → type string.
pub(super) async fn seed_and_validate(
    source: &SourceConfig,
    db: &Db,
) -> anyhow::Result<HashMap<String, String>>

/// Check all required schema fields are present in the transformed row.
/// Fires when a declared source column was absent from the CSV headers.
pub(super) fn validate_completeness(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    row_num: u64,
) -> Result<(), RowFailure>

/// Build normalized_data JSONB from the transformed row, casting values to declared types.
/// Dimension fields (LOCATION_FIELDS / TIME_FIELDS) are excluded.
pub(super) fn collect_normalized_data(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    row_num: u64,
) -> Result<serde_json::Value, RowFailure>
```

### New: `derived.rs`

```rust
/// Resolve derived fields from dimension tables and insert results into normalized_data.
/// Prerequisites: location_id / time_id must be non-null for fields requiring those dims.
/// Returns Err(RowFailure) on missing prerequisite or failed lookup.
pub(super) async fn resolve_derived(
    normalized_data: &mut serde_json::Value,
    source: &SourceConfig,
    location_id: Option<i64>,
    time_id: Option<i64>,
    db: &Db,
    row_num: u64,
) -> Result<(), RowFailure>
```

### Updated: `record.rs`

`process_record` returns `Option<NewNormalizedImport>` instead of `Vec<NewObservation>`. The function signature gains `schema: &HashMap<String, String>` (loaded at run start). Steps 5–7 (extract_context, extract_metrics, build_observations) are removed.

### Updated: `mod.rs`

- `run()` calls `schema::seed_and_validate` before `process_rows` and passes the returned schema map.
- `process_rows` flushes `Vec<NewNormalizedImport>` batches via `NormalizedImportRepository::bulk_create`.
- Failure cleanup calls `NormalizedImportRepository::delete_by_ingest_run`.
- `write_export_script` no longer passes `&source.attributes`; `generate_export_sql` in `export.rs` derives the column projection from the loaded schema instead.

### Removed

- `pipeline/context.rs`
- `pipeline/observations.rs`

---

## Trade-offs

| Concern | New design | Old design |
|---------|-----------|------------|
| Audit boundary | Clear: raw = verbatim, normalized = typed | Blurred: `normalized_data` alongside `raw_data` |
| Schema enforcement | Strict: missing/uncastable fields fail rows | Lenient: unknown fields fall through to metrics or context |
| Analytics query shape | Wide row per CSV row | Tall (one row per metric) + join to context |
| Replayability | Re-ingest CSV against updated schema | Same |
| Type explicitness | `import_schema` table + `sources.yml` | Implicit in Rust `FieldType` enum and transform chain |
| Config maintenance | Single `fields` block per source column | Separate `field_map` + `attributes` |
| DB writes per row | 3 (raw_import RETURNING id, dim upserts, normalized_import batch) | 4 (raw_import, dim upserts, fact_row_context, fact_obs batch) |
| Schema changes | Require `sources.yml` update + re-ingest | Require code change |
| `SkipDataset` behavior | Preserved — still aborts the file on transform failure | Same |
| Passthrough columns | Removed — undeclared columns are rejected | Any unmapped column passes through silently |
