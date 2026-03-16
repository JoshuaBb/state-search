# Dimension Loading & Exclude Columns — Design Spec

**Date:** 2026-03-15
**Status:** Approved

---

## Overview

Three new features for the ingest pipeline:

1. **Dimension-only sources** — `sources.yml` sources that upsert directly into `dim_location` or `dim_time` instead of producing fact observations.
2. **`exclude_columns`** — a source-level list of CSV columns to drop before field mapping; errors at startup if any listed column is absent from the CSV header.
3. **`unique_key`** — a source-level list of **canonical** field names used to derive a deterministic UUIDv5 primary key, enabling idempotent upserts.

---

## 1. Config Changes (`core/src/config.rs`)

### New enum: `DimTarget`

```rust
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DimTarget {
    DimLocation,
    DimTime,
}
```

Serializes to/from `"dim_location"` / `"dim_time"`. This exact string is stored in `dim_file_log.target`.

To get the string value in code: `serde_json::to_value(target).unwrap().as_str().unwrap().to_string()`.

### Updated `SourceConfig`

```rust
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    pub files: Vec<String>,
    pub fields: HashMap<String, FieldDef>,
    pub derived: HashMap<String, DerivedFieldDef>,

    // New fields (all with #[serde(default)]):
    pub target: Option<DimTarget>,       // None = existing fact mode
    pub exclude_columns: Vec<String>,    // raw CSV column names to drop before mapping
    pub unique_key: Vec<String>,         // canonical names (post-mapping)
}
```

**`unique_key` entries are canonical names** (e.g. `state_code`, `fips_code`), not raw CSV column names.

For dim sources: `unique_key` controls the UUID of the dim table row.
For fact sources: `unique_key` controls the UUID of the `normalized_imports.id` row.

### Example `sources.yml`

```yaml
sources:
  - name: us_locations
    target: dim_location
    unique_key:
      - fips_code
      - county
    exclude_columns:
      - internal_notes  # raw CSV column names
      - legacy_id
    files:
      - data/locations.csv
    fields:
      county_name:
        canonical: county
        type: text
      county_fips:
        canonical: fips_code
        type: text
```

---

## 2. Deterministic UUID

**Scope:** `unique_key` in `sources.yml` determines the UUID for:
- `dim_location.id` / `dim_time.id` when `target` is set (dim-only sources)
- `normalized_imports.id` when `target` is absent (fact sources)

**Dim rows created during fact ingestion** (in `dimensions.rs`) use a **hardcoded internal key**, independent of the source's `unique_key` config:
- `dim_location`: (`county`, `country`, `zip_code`, `fips_code`, `latitude`, `longitude`) — these are the six columns that exist in `dim_location` after migration 002 (no `state_code`/`state_name`); null fields render as empty string in the key
- `dim_time`: (`year`, `quarter`, `month`, `day`)

This keeps dim rows idempotent in the fact path without requiring every fact source to configure `unique_key`.

**UUID derivation (shared logic, used in both paths):**

Lives in `ingest/src/pipeline/uuid.rs` (new module, exported from `pipeline::uuid`).

Namespace: **`Uuid::NAMESPACE_OID`** — fixed constant, ensures identical output across environments and re-runs.

Byte layout: pipe-separated UTF-8 string `"col1=val1|col2=val2"` where columns are sorted alphabetically. Null or absent fields render as empty string (e.g. `"col1=|col2=val2"`). The string is passed directly to `Uuid::new_v5` as bytes via `.as_bytes()`.

**`FieldValue` serialization for the key string** (must be stable across runs and environments):

| Variant | Key string representation |
|---------|--------------------------|
| `Null` / absent | `""` (empty string) |
| `Str(s)` | `s` as-is |
| `Bool(b)` | `"true"` or `"false"` |
| Integer variants (`I8`…`I64`, `U8`…`U64`) | decimal, e.g. `"2024"` |
| `F32(f)` | `f.to_bits().to_string()` (bit-level repr avoids locale/precision variance) |
| `F64(f)` | `f.to_bits().to_string()` |
| `Date(d)` | ISO 8601: `d.to_string()` (chrono `%Y-%m-%d`) |
| `DateTime(dt)` | RFC 3339: `dt.to_rfc3339()` |

**Float encoding consistency:** the existing `location_cache_key` in `dimensions.rs` already uses `f64::to_bits()` for lat/lon. `derive_uuid` must use the same `to_bits()` representation for `F32`/`F64` variants so the cache key (deduplication within a run) and the UUID (idempotency across runs) encode floats identically.

```rust
// ingest/src/pipeline/uuid.rs
pub fn derive_uuid(key_cols: &[&str], row: &HashMap<String, FieldValue>) -> Uuid {
    let mut pairs: Vec<(&str, String)> = key_cols.iter()
        .map(|col| {
            let val = match row.get(*col) {
                Some(FieldValue::Null) | None => String::new(),  // null → empty string
                Some(v) => v.to_string(),
            };
            (*col, val)
        })
        .collect();
    pairs.sort_by_key(|(k, _)| *k);  // alphabetical for stability
    let key_str = pairs.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("|");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key_str.as_bytes())
}
```

**Example:** for a row with `county="Denver"`, `country="USA"`, `zip_code=NULL`, the key string is `"county=Denver|country=USA|zip_code="` — producing a fixed, reproducible UUID.

- Empty `unique_key` on a fact source → `Uuid::new_v4()` for `normalized_imports.id`.
- **Warning** logged at startup for dim sources with an empty `unique_key` (no deduplication possible; every row gets a random UUID).

**Startup validation of `unique_key`:** every entry must be a canonical name declared in `source.fields.values().canonical` or `source.derived.keys()`. There is no implicit set of built-in valid names — if a canonical name (e.g. `county`) is not declared in the source's `fields` map, it is not a valid `unique_key` entry for that source. This applies equally to dim sources and fact sources.

---

## 3. Schema Migrations (`migrations/006_uuid_pks.sql`)

The project uses **Postgres 17**. `gen_random_uuid()` is built-in from Postgres 13+ — no extension required. A comment in the migration file states the minimum version.

### Current state (after 005_normalized_imports.sql)

- `dim_location` columns: `id BIGSERIAL PK`, `county TEXT`, `country CHAR(3)`, `zip_code TEXT`, `fips_code TEXT`, `latitude DOUBLE PRECISION`, `longitude DOUBLE PRECISION`
  - **Note:** `state_code` and `state_name` were present in migration 001 but were **dropped** in migration 002 and are **not being re-added** in this migration. The final column set does not include them.
- `dim_location_uq` unique index on `(county, country, zip_code)` ← **this is dropped**
- `dim_time` has `dim_time_uq` on `(year, quarter, month, day)` ← **this is dropped**
- `normalized_imports.id`: `BIGSERIAL PK`
- `normalized_imports.location_id`: `BIGINT FK → dim_location.id`
- `normalized_imports.time_id`: `BIGINT FK → dim_time.id`
- `normalized_imports.raw_import_id`: `BIGINT FK → raw_imports.id` ← **unchanged**

### `dim_location` and `dim_time` — PK bigint → uuid

**Migration ordering:** drop both FK constraints on `normalized_imports` first, then alter both parent tables, then alter the FK columns on `normalized_imports`, then re-add both FKs.

```sql
-- Step 1: drop both FKs before altering any referenced column
ALTER TABLE normalized_imports DROP CONSTRAINT IF EXISTS normalized_imports_location_id_fkey;
ALTER TABLE normalized_imports DROP CONSTRAINT IF EXISTS normalized_imports_time_id_fkey;

-- Step 2: drop old content-based unique indexes
DROP INDEX IF EXISTS dim_location_uq;   -- was (county, country, zip_code)
DROP INDEX IF EXISTS dim_time_uq;       -- was (year, quarter, month, day)

-- Step 3: alter dim_location PK
ALTER TABLE dim_location ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE dim_location ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 4: alter dim_time PK
ALTER TABLE dim_time ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE dim_time ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Step 5: alter FK columns on normalized_imports (existing bigint values cannot be cast to uuid;
--         NULL them out since historical dimension links are discarded by this migration)
ALTER TABLE normalized_imports ALTER COLUMN location_id TYPE uuid USING NULL::uuid;
ALTER TABLE normalized_imports ALTER COLUMN time_id     TYPE uuid USING NULL::uuid;

-- Step 6: re-add both FKs
ALTER TABLE normalized_imports
    ADD CONSTRAINT normalized_imports_location_id_fkey
    FOREIGN KEY (location_id) REFERENCES dim_location(id);
ALTER TABLE normalized_imports
    ADD CONSTRAINT normalized_imports_time_id_fkey
    FOREIGN KEY (time_id) REFERENCES dim_time(id);
```

New upserts:
- `dim_location`: `ON CONFLICT (id) DO UPDATE SET county=EXCLUDED.county, country=EXCLUDED.country, zip_code=EXCLUDED.zip_code, fips_code=EXCLUDED.fips_code, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude`
- `dim_time`: `ON CONFLICT (id) DO UPDATE SET year=EXCLUDED.year, quarter=EXCLUDED.quarter, month=EXCLUDED.month, day=EXCLUDED.day, date_floor=EXCLUDED.date_floor`

### `normalized_imports` — PK bigint → uuid

`raw_import_id` (FK → `raw_imports.id` bigint) is **unchanged** — `raw_imports.id` is not migrated.

```sql
ALTER TABLE normalized_imports ALTER COLUMN id TYPE uuid USING gen_random_uuid();
ALTER TABLE normalized_imports ALTER COLUMN id SET DEFAULT gen_random_uuid();
```

The `bulk_create` INSERT will now explicitly supply `id` from application code; the `DEFAULT gen_random_uuid()` is a fallback only.

### New table: `dim_file_log`

```sql
-- Requires Postgres 13+ (gen_random_uuid built-in, no pgcrypto needed)
CREATE TABLE dim_file_log (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name  TEXT        NOT NULL,
    target       TEXT        NOT NULL,  -- serde snake_case: "dim_location" | "dim_time"
    file_path    TEXT        NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count    BIGINT      NOT NULL
);
```

Append-only: one new row per successful dim ingest run.

---

## 4. Model Changes (`core/src/models/`)

### `location.rs`

- `Location.id`: `i64` → `Uuid`
- `NewLocation` gains `id: Uuid` (caller always provides; derived via `derive_uuid` or `new_v4()`)
- **`#[derive(Default)]` is removed from `NewLocation`** — the nil UUID is not a valid default. All construction sites in `dimensions.rs` and `dim.rs` must supply an explicit `id`.

```rust
pub struct NewLocation {
    pub id:        Uuid,
    pub county:    Option<String>,
    pub country:   Option<String>,
    pub zip_code:  Option<String>,
    pub fips_code: Option<String>,
    pub latitude:  Option<f64>,
    pub longitude: Option<f64>,
}
```

### `time.rs`

Same pattern: `TimePeriod.id: Uuid`, `NewTimePeriod` gains `id: Uuid`. **`#[derive(Default)]` removed from `NewTimePeriod`** for the same reason.

### `normalized_import.rs`

- `NormalizedImport.id`: `i64` → `Uuid`
- `NewNormalizedImport` gains `id: Uuid` (caller always provides)
- `NewNormalizedImport.location_id`: `Option<i64>` → `Option<Uuid>`
- `NewNormalizedImport.time_id`: `Option<i64>` → `Option<Uuid>`

### `dim_file_log.rs` (new)

```rust
pub struct NewDimFileLog {
    pub source_name: String,
    pub target:      String,  // serde snake_case of DimTarget: "dim_location" | "dim_time"
    pub file_path:   String,
    pub row_count:   i64,
}
```

---

## 5. Repository Changes (`core/src/repositories/`)

### `location.rs`

Both `upsert` **and** `upsert_with_tx` updated:
- Accept `NewLocation` (with `id: Uuid`)
- SQL: `INSERT INTO dim_location (id, county, ...) VALUES ($1, $2, ...) ON CONFLICT (id) DO UPDATE SET county=EXCLUDED.county, ...`
- Return type: `Uuid` (was `i64`)

### `time.rs`

Same pattern — both variants, return `Uuid`.

### `normalized_import.rs`

`bulk_create` updated:
- Add `id` to column list (position 1)
- `location_id` cast: `::bigint[]` → `::uuid[]`
- `time_id` cast: `::bigint[]` → `::uuid[]`
- Add `ON CONFLICT (id) DO NOTHING` (idempotent re-insert when `unique_key` set)

```sql
INSERT INTO normalized_imports
    (id, raw_import_id, location_id, time_id, source_name, ingest_run_id, normalized_data)
SELECT * FROM UNNEST(
    $1::uuid[], $2::bigint[], $3::uuid[], $4::uuid[], $5::text[], $6::uuid[], $7::jsonb[]
)
ON CONFLICT (id) DO NOTHING
```

`NewNormalizedImport` gains `id: Uuid` field (pushed first into the UNNEST arrays).

### `dim_file_log.rs` (new)

- `insert(pool, &NewDimFileLog) -> Result<()>`
- `exists_for_file(pool, source_name: &str, file_path: &str) -> Result<bool>` — matches on both `source_name` AND `file_path`; the same physical file under different source names is treated as distinct.

---

## 6. Pipeline Changes (`ingest/`)

### CLI dispatch (`main.rs`)

Both `Command::Reload` and `Command::Run` (no `--file`) currently call `pipeline.already_ingested(file)` then `pipeline.run(source, file)`. Both are updated to dispatch based on `source.target`:

```rust
// Helper used in both Reload and Run (no --file) arms:
async fn should_skip(pipeline: &IngestPipeline<'_>, source: &SourceConfig, file: &str)
    -> anyhow::Result<bool>
{
    if source.target.is_some() {
        // dim sources: check dim_file_log
        pipeline.dim_already_ingested(file).await
    } else {
        // fact sources: check raw_imports (existing behavior)
        pipeline.already_ingested(file).await
    }
}

async fn dispatch(pipeline: &IngestPipeline<'_>, source: &SourceConfig, file: &str)
    -> anyhow::Result<u64>
{
    if source.target.is_some() {
        pipeline.run_dim(source, file).await
    } else {
        pipeline.run(source, file).await
    }
}
```

`Command::Run { file: Some(path) }` (explicit `--file`): bypasses skip check for both fact and dim (existing behavior preserved).

`IngestPipeline` gains `dim_already_ingested(file: &str) -> Result<bool>` which calls `DimFileLogRepository::exists_for_file`.

### `exclude_columns` validation and stripping

Runs once after the CSV reader opens and reads headers, **before** field mapping.

**Validation** — case-insensitive (lowercase both sides):
```rust
let header_set: HashSet<String> = headers.iter().map(|h| h.to_lowercase()).collect();
for col in &source.exclude_columns {
    if !header_set.contains(&col.to_lowercase()) {
        anyhow::bail!("exclude_columns: '{}' not in CSV headers for source '{}'", col, source.name);
    }
}
```

**Stripping** — operates on the **raw (pre-lowercase) CSV headers** and `StringRecord` values, filtered together by index before any lowercasing occurs. This preserves header/value alignment because `csv::StringRecord` is index-based:

```rust
let excluded_lower: HashSet<String> = source.exclude_columns.iter()
    .map(|c| c.to_lowercase())
    .collect();

// Filter raw headers + raw record values by index BEFORE row_to_canonical lowercases them
let (filtered_headers, filtered_values): (Vec<&str>, Vec<&str>) = headers.iter()
    .zip(record.iter())
    .filter(|(h, _)| !excluded_lower.contains(&h.to_lowercase()))
    .unzip();
```

The resulting `filtered_headers` and `filtered_values` are then passed into `row_to_canonical`, which lowercases headers as usual.

### `unique_key` validation (at startup)

```rust
let canonical_names: HashSet<&str> = source.fields.values()
    .map(|f| f.canonical.as_str())
    .chain(source.derived.keys().map(String::as_str))
    .collect();
for col in &source.unique_key {
    if !canonical_names.contains(col.as_str()) {
        anyhow::bail!("unique_key: '{}' is not a canonical field in source '{}'", col, source.name);
    }
}
```

### `LocationCache` type update

```rust
pub(super) type LocationCache = Arc<tokio::sync::RwLock<HashMap<String, Uuid>>>;
```

`Arc<tokio::sync::RwLock<...>>` wrapper **preserved**. Value type `i64` → `Uuid`.

Cache key: same pipe-separated content string as before (based on location field values).
Cache value: UUID derived via `derive_uuid` using the hardcoded internal key (all location fields).

### `derived.rs`

`resolve_derived`'s function signature changes: `location_id` and `time_id` parameters change from `Option<i64>` to `Option<Uuid>`, and their SQL bind parameters change from `i64` to `Uuid`. All call sites in `record.rs` are updated accordingly.

### Fact path changes summary

- `already_ingested()` on `raw_imports.source_file` — preserved unchanged.
- `seed_and_validate` — called as before.
- `NewLocation.id` — derived via `derive_uuid([county, country, zip_code, fips_code, latitude, longitude], ...)` using the hardcoded internal key.
- `NewTimePeriod.id` — derived via `derive_uuid([year, quarter, month, day], ...)`.
- `NewNormalizedImport.id` — derived via `derive_uuid(&source.unique_key, row)` if `unique_key` non-empty; else `Uuid::new_v4()`.
- `NewNormalizedImport.location_id` / `.time_id` — now `Option<Uuid>`.

### Dim-only pipeline (`run_dim` → `ingest/src/pipeline/dim.rs`)

Fully separate code path. `seed_and_validate` is **not called**. `already_ingested()` is **not called**. Only `dim_file_log.exists_for_file` is checked (warn, continue).

```
run_dim(source, file_path, db):
  1. exclude_columns validation (error on missing header)
  2. unique_key validation (error on missing canonical; warn if empty)
  3. check dim_file_log.exists_for_file(file_path) → warn if true, continue
  4. open CSV reader, read headers, apply exclude_columns strip
  5. batched_channel_sink:
       Output type dispatched by source.target:
         DimTarget::DimLocation → Vec<NewLocation>
         DimTarget::DimTime    → Vec<NewTimePeriod>
       Per row:
         → strip excluded columns (filtered header+record pair)
         → field mapping + transforms
         → derive UUID: derive_uuid(&source.unique_key, &transformed_row)
                        or new_v4() if unique_key is empty
         → build NewLocation / NewTimePeriod with id = derived UUID
       Flush callback: bulk upsert via repository (ON CONFLICT (id) DO UPDATE)
                       returns flushed count
  6. success: insert dim_file_log row (source_name, target as serde snake_case, file_path, row_count)
  7. failure: log error; dim_file_log row NOT inserted
              already-upserted rows are left in place (idempotent via UUID PK)
```

Uses `ROW_CONCURRENCY` and `OBS_BATCH_SIZE` constants unchanged. Does NOT write: `raw_imports`, `normalized_imports`, `import_schema`, export SQL.

---

## 7. Files Touched

| File | Change |
|------|--------|
| `core/src/config.rs` | Add `DimTarget`; add `target`, `exclude_columns`, `unique_key` to `SourceConfig` |
| `core/src/models/location.rs` | `Location.id: Uuid`; `NewLocation` gains `id: Uuid` |
| `core/src/models/time.rs` | Same pattern |
| `core/src/models/normalized_import.rs` | `NormalizedImport.id: Uuid`; `NewNormalizedImport` gains `id: Uuid`; `location_id`/`time_id`: `Option<i64>` → `Option<Uuid>` |
| `core/src/models/dim_file_log.rs` | New: `NewDimFileLog` |
| `core/src/repositories/location.rs` | `upsert` + `upsert_with_tx`: `NewLocation` with `id`, return `Uuid`, ON CONFLICT (id) |
| `core/src/repositories/time.rs` | Same |
| `core/src/repositories/normalized_import.rs` | `bulk_create`: add `id` column, `::uuid[]` for location_id/time_id, `ON CONFLICT (id) DO NOTHING`; `NewNormalizedImport` gains `id` |
| `core/src/repositories/dim_file_log.rs` | New: `insert`, `exists_for_file` |
| `core/src/repositories/mod.rs` | Export new repository |
| `ingest/src/pipeline/uuid.rs` | New: `derive_uuid` shared function |
| `ingest/src/pipeline/mod.rs` | Add `dim_already_ingested`; branch on `source.target` before `seed_and_validate` |
| `ingest/src/pipeline/record.rs` | Derive `normalized_imports.id` via `derive_uuid` or `new_v4()`; `location_id`/`time_id` are now `Option<Uuid>` |
| `ingest/src/pipeline/dimensions.rs` | `LocationCache` value `i64` → `Uuid`; derive dim row UUIDs via hardcoded internal key |
| `ingest/src/pipeline/derived.rs` | `location_id`/`time_id` SQL binds `i64` → `Uuid` |
| `ingest/src/pipeline/row.rs` | Exclude-columns stripping before `row_to_canonical` |
| `ingest/src/pipeline/dim.rs` | New: `run_dim` implementation |
| `ingest/src/main.rs` | Dispatch skip check and pipeline call based on `source.target`; both `Reload` and `Run` arms updated |
| `migrations/006_uuid_pks.sql` | UUID PKs for dim_location/dim_time/normalized_imports; drop old unique indexes; `dim_file_log` table |
| `config/sources.yml` | Add `target`, `unique_key`, `exclude_columns` to relevant sources |
