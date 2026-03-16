# Dimension Loading & Exclude Columns Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dimension-only source ingestion (`target: dim_location/dim_time`), per-column exclusion (`exclude_columns`), and deterministic UUID primary keys (`unique_key`) to the ingest pipeline.

**Architecture:** Three independent layers built bottom-up: (1) migration + config + UUID utility, (2) model/repository type changes in `core`, (3) pipeline wiring + new `dim.rs` path in `ingest`. Each layer compiles cleanly before the next begins.

**Tech Stack:** Rust (sqlx 0.8, tokio, uuid 1.x, serde_yaml), PostgreSQL 17, CSV ingestion pipeline.

**Spec:** `docs/superpowers/specs/2026-03-15-dim-loading-design.md`

---

## Chunk 1: Foundation

### Task 1: Schema migration

**Files:**
- Create: `migrations/006_uuid_pks.sql`

- [ ] **Step 1: Write the migration**

```sql
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
```

- [ ] **Step 2: Verify the migration file exists**

```bash
ls migrations/006_uuid_pks.sql
```

Expected: file listed.

- [ ] **Step 3: Commit**

```bash
git add migrations/006_uuid_pks.sql
git commit -m "feat(migrations): add UUID PKs for dim tables and dim_file_log"
```

---

### Task 2: Config — DimTarget enum + new SourceConfig fields

**Files:**
- Modify: `core/src/config.rs`

The existing `SourceConfig` struct in `core/src/config.rs` needs three new fields (`target`, `exclude_columns`, `unique_key`) and a new `DimTarget` enum. All fields have `#[serde(default)]` so existing `sources.yml` files continue to parse without changes.

- [ ] **Step 1: Write the failing test**

Add this test to the `#[cfg(test)]` block at the bottom of `core/src/config.rs`:

```rust
#[test]
fn source_config_with_dim_fields_deserializes() {
    let src = r#"
name: us_locations
target: dim_location
unique_key:
  - fips_code
  - county
exclude_columns:
  - internal_notes
fields:
  county_fips:
    canonical: fips_code
    type: text
"#;
    let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
    assert_eq!(sc.target, Some(DimTarget::DimLocation));
    assert_eq!(sc.unique_key, vec!["fips_code", "county"]);
    assert_eq!(sc.exclude_columns, vec!["internal_notes"]);
}

#[test]
fn source_config_without_dim_fields_still_parses() {
    let src = "name: existing_source\nfields: {}\nderived: {}\n";
    let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
    assert!(sc.target.is_none());
    assert!(sc.unique_key.is_empty());
    assert!(sc.exclude_columns.is_empty());
}

#[test]
fn dim_target_serializes_to_snake_case() {
    let t = DimTarget::DimLocation;
    let s = serde_json::to_value(t).unwrap();
    assert_eq!(s.as_str().unwrap(), "dim_location");

    let t2 = DimTarget::DimTime;
    let s2 = serde_json::to_value(t2).unwrap();
    assert_eq!(s2.as_str().unwrap(), "dim_time");
}

#[test]
fn source_config_with_dim_time_target_deserializes() {
    let src = "name: time_src\ntarget: dim_time\nfields: {}\nderived: {}\n";
    let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
    assert_eq!(sc.target, Some(DimTarget::DimTime));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cargo test -p state-search-core source_config_with_dim_fields 2>&1 | head -20
```

Expected: compile error — `DimTarget` not defined.

- [ ] **Step 3: Implement the changes**

In `core/src/config.rs`, add the `DimTarget` enum **above** `SourceConfig`, and add the three new fields to `SourceConfig`:

```rust
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DimTarget {
    DimLocation,
    DimTime,
}
```

`Serialize` is required because `serde_json::to_value(target)` is used to obtain the snake_case string stored in `dim_file_log.target`.

Add to `SourceConfig` (after the `derived` field):

```rust
    #[serde(default)]
    pub target: Option<DimTarget>,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    #[serde(default)]
    pub unique_key: Vec<String>,
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p state-search-core 2>&1 | tail -20
```

Expected: all tests pass (including the 4 new config tests).

- [ ] **Step 5: Verify the whole crate still compiles**

```bash
cargo build -p state-search-core
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add core/src/config.rs
git commit -m "feat(config): add DimTarget enum and target/unique_key/exclude_columns to SourceConfig"
```

---

### Task 3: `derive_uuid` utility module

**Files:**
- Modify: `Cargo.toml` (workspace root — add `v5` to uuid features)
- Create: `ingest/src/pipeline/uuid.rs`
- Modify: `ingest/src/pipeline/mod.rs` (add `mod uuid;`)

`derive_uuid` is a pure function — fully testable without a database. It implements the spec's key-string encoding: pipe-separated `col=val` pairs sorted alphabetically, with floats encoded as `to_bits().to_string()` for stability.

The `FieldValue` enum lives in `ingest/src/transforms/mod.rs`. Check its variants there. It has `Display` needed for the key string; we add the encoding via a helper `field_value_to_key_str`.

- [ ] **Step 0: Enable the `v5` UUID feature**

`Uuid::new_v5` requires the `v5` cargo feature. Open `Cargo.toml` (workspace root) and update the `uuid` dependency:

```toml
uuid = { version = "1", features = ["v4", "v5", "serde"] }
```

Verify it compiles:

```bash
cargo build -p state-search-core 2>&1 | head -5
```

Expected: no errors.

- [ ] **Step 1: Write the failing test**

Create `ingest/src/pipeline/uuid.rs` with tests first:

```rust
// ingest/src/pipeline/uuid.rs
use std::collections::HashMap;
use uuid::Uuid;
use crate::transforms::FieldValue;

/// Derive a deterministic UUIDv5 from a set of canonical key columns and their
/// post-transform values. Column names are sorted alphabetically; null values
/// render as empty string. Float variants use to_bits() for locale-stable encoding.
///
/// Namespace: Uuid::NAMESPACE_OID (fixed across all environments).
pub fn derive_uuid(key_cols: &[&str], row: &HashMap<String, FieldValue>) -> Uuid {
    todo!()
}

fn field_value_to_key_str(v: &FieldValue) -> String {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, FieldValue)]) -> HashMap<String, FieldValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn same_input_produces_same_uuid() {
        let r = row(&[("county", FieldValue::Str("Denver".into())), ("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        let id2 = derive_uuid(&["county", "country"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn column_order_does_not_matter() {
        let r = row(&[("county", FieldValue::Str("Denver".into())), ("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        let id2 = derive_uuid(&["country", "county"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn null_value_renders_as_empty_string() {
        let r = row(&[("county", FieldValue::Null), ("country", FieldValue::Str("USA".into()))]);
        let id = derive_uuid(&["county", "country"], &r);
        // Deterministic: same null → same UUID
        let id2 = derive_uuid(&["county", "country"], &r);
        assert_eq!(id, id2);
    }

    #[test]
    fn absent_column_renders_as_empty_string() {
        let r = row(&[("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        // Same as explicit null
        let r2 = row(&[("county", FieldValue::Null), ("country", FieldValue::Str("USA".into()))]);
        let id2 = derive_uuid(&["county", "country"], &r2);
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_values_produce_different_uuids() {
        let r1 = row(&[("county", FieldValue::Str("Denver".into()))]);
        let r2 = row(&[("county", FieldValue::Str("Boulder".into()))]);
        assert_ne!(derive_uuid(&["county"], &r1), derive_uuid(&["county"], &r2));
    }

    #[test]
    fn float_encoding_is_stable() {
        let r = row(&[("lat", FieldValue::F64(39.7392))]);
        let id1 = derive_uuid(&["lat"], &r);
        let id2 = derive_uuid(&["lat"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn known_key_string_produces_expected_uuid() {
        // "county=Denver|country=USA" with NAMESPACE_OID
        let r = row(&[
            ("county", FieldValue::Str("Denver".into())),
            ("country", FieldValue::Str("USA".into())),
        ]);
        let id = derive_uuid(&["county", "country"], &r);
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"country=USA|county=Denver");
        assert_eq!(id, expected);
    }

    #[test]
    fn f32_encoding_uses_to_bits() {
        // Two structurally identical f32 values must produce the same UUID.
        // Encoding uses to_bits() — not Display — for locale/precision stability.
        let r = row(&[("lng", FieldValue::F32(104.9903_f32))]);
        let id1 = derive_uuid(&["lng"], &r);
        let id2 = derive_uuid(&["lng"], &r);
        assert_eq!(id1, id2);
        // Verify it matches the expected key string directly.
        let expected = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("lng={}", 104.9903_f32.to_bits()).as_bytes(),
        );
        assert_eq!(id1, expected);
    }

    #[test]
    fn bool_encoding_uses_to_string() {
        let r_true  = row(&[("active", FieldValue::Bool(true))]);
        let r_false = row(&[("active", FieldValue::Bool(false))]);
        let id_true  = derive_uuid(&["active"], &r_true);
        let id_false = derive_uuid(&["active"], &r_false);
        assert_ne!(id_true, id_false);
        let expected_true = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"active=true");
        assert_eq!(id_true, expected_true);
    }

    #[test]
    fn integer_encoding_uses_to_string() {
        let r = row(&[("year", FieldValue::I32(2024))]);
        let id = derive_uuid(&["year"], &r);
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"year=2024");
        assert_eq!(id, expected);
    }

    #[test]
    fn date_encoding_uses_iso8601() {
        use chrono::NaiveDate;
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let r = row(&[("date_floor", FieldValue::Date(d))]);
        let id = derive_uuid(&["date_floor"], &r);
        // NaiveDate::to_string() returns YYYY-MM-DD
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"date_floor=2024-03-15");
        assert_eq!(id, expected);
    }
}
```

- [ ] **Step 2: Run to verify compile failure**

```bash
cargo test -p state-search-ingest uuid 2>&1 | head -20
```

Expected: compile error — `uuid.rs` module not declared.

- [ ] **Step 3: Declare the module in `mod.rs`**

In `ingest/src/pipeline/mod.rs`, add at the top with the other module declarations:

```rust
mod uuid;
pub(super) use uuid::derive_uuid;
```

- [ ] **Step 4: Run again — now tests should panic at `todo!()`**

```bash
cargo test -p state-search-ingest uuid 2>&1 | head -20
```

Expected: compile succeeds, tests panic with "not yet implemented".

- [ ] **Step 5: Implement `derive_uuid`**

Replace the `todo!()` bodies in `ingest/src/pipeline/uuid.rs`:

```rust
use std::collections::HashMap;
use uuid::Uuid;
use crate::transforms::FieldValue;

pub fn derive_uuid(key_cols: &[&str], row: &HashMap<String, FieldValue>) -> Uuid {
    let mut pairs: Vec<(&str, String)> = key_cols.iter()
        .map(|col| {
            let val = match row.get(*col) {
                Some(FieldValue::Null) | None => String::new(),
                Some(v)                       => field_value_to_key_str(v),
            };
            (*col, val)
        })
        .collect();
    pairs.sort_by_key(|(k, _)| *k);
    let key_str = pairs.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("|");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key_str.as_bytes())
}

fn field_value_to_key_str(v: &FieldValue) -> String {
    match v {
        FieldValue::Null        => String::new(),
        FieldValue::Str(s)      => s.clone(),
        FieldValue::Bool(b)     => b.to_string(),
        FieldValue::I8(x)       => x.to_string(),
        FieldValue::I16(x)      => x.to_string(),
        FieldValue::I32(x)      => x.to_string(),
        FieldValue::I64(x)      => x.to_string(),
        FieldValue::U8(x)       => x.to_string(),
        FieldValue::U16(x)      => x.to_string(),
        FieldValue::U32(x)      => x.to_string(),
        FieldValue::U64(x)      => x.to_string(),
        FieldValue::F32(f)      => f.to_bits().to_string(),
        FieldValue::F64(f)      => f.to_bits().to_string(),
        FieldValue::Date(d)     => d.to_string(),
        FieldValue::DateTime(dt) => dt.to_rfc3339(),
    }
}
```

- [ ] **Step 6: Run tests**

```bash
cargo test -p state-search-ingest uuid
```

Expected: all 11 tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml ingest/src/pipeline/uuid.rs ingest/src/pipeline/mod.rs
git commit -m "feat(ingest): add derive_uuid utility for deterministic UUIDv5 PKs"
```

---

## Chunk 2: Core models and repositories

### Task 4: Update models — location, time, normalized_import; add dim_file_log

**Files:**
- Modify: `core/src/models/location.rs`
- Modify: `core/src/models/time.rs`
- Modify: `core/src/models/normalized_import.rs`
- Create: `core/src/models/dim_file_log.rs`
- Modify: `core/src/models/mod.rs`

This task updates the model structs to use `Uuid` PKs and adds the `NewDimFileLog` model. Since these are data structs, we verify correctness by checking field types compile and existing tests still pass.

- [ ] **Step 1: Update `core/src/models/location.rs`**

Replace the entire file:

```rust
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Normalized location dimension.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Location {
    pub id:        Uuid,
    pub county:    Option<String>,
    pub country:   Option<String>,
    pub zip_code:  Option<String>,
    pub fips_code: Option<String>,
    pub latitude:  Option<f64>,
    pub longitude: Option<f64>,
}

/// `Default` is intentionally NOT derived — the nil UUID is not a valid id.
/// Callers must always provide an explicit `id` via `derive_uuid` or `Uuid::new_v4()`.
#[derive(Debug, Deserialize)]
pub struct NewLocation {
    pub id:        Uuid,
    pub county:    Option<String>,
    pub country:   Option<String>,
    pub zip_code:  Option<String>,
    pub fips_code: Option<String>,
    pub latitude:  Option<f64>,
    pub longitude: Option<f64>,
}

impl NewLocation {
    pub fn is_empty(&self) -> bool {
        self.county.is_none()
            && self.country.is_none()
            && self.zip_code.is_none()
            && self.fips_code.is_none()
    }
}
```

- [ ] **Step 2: Update `core/src/models/time.rs`**

Replace the entire file:

```rust
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Normalized time dimension.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TimePeriod {
    pub id:         Uuid,
    pub year:       i16,
    pub quarter:    Option<i16>,
    pub month:      Option<i16>,
    pub day:        Option<i16>,
    pub date_floor: Option<NaiveDate>,
}

/// `Default` is intentionally NOT derived — the nil UUID is not a valid id.
#[derive(Debug, Deserialize)]
pub struct NewTimePeriod {
    pub id:      Uuid,
    pub year:    i16,
    pub quarter: Option<i16>,
    pub month:   Option<i16>,
    pub day:     Option<i16>,
}

impl NewTimePeriod {
    pub fn date_floor(&self) -> Option<NaiveDate> {
        let month = self
            .month
            .or_else(|| self.quarter.map(|q| (q - 1) * 3 + 1))
            .unwrap_or(1) as u32;
        let day = self.day.unwrap_or(1) as u32;
        NaiveDate::from_ymd_opt(self.year as i32, month, day)
    }
}
```

- [ ] **Step 3: Update `core/src/models/normalized_import.rs`**

Replace the entire file:

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewNormalizedImport {
    pub id:              Uuid,
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<Uuid>,
    pub time_id:         Option<Uuid>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct NormalizedImport {
    pub id:              Uuid,
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<Uuid>,
    pub time_id:         Option<Uuid>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
    pub inserted_at:     DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_normalized_import_fields() {
        let _ = NewNormalizedImport {
            id:              Uuid::new_v4(),
            raw_import_id:   Some(1),
            location_id:     Some(Uuid::new_v4()),
            time_id:         Some(Uuid::new_v4()),
            source_name:     "s".to_string(),
            ingest_run_id:   Uuid::new_v4(),
            normalized_data: serde_json::json!({"analyte": "TTHM"}),
        };
    }
}
```

- [ ] **Step 4: Create `core/src/models/dim_file_log.rs`**

```rust
/// Log entry written after a successful dim-only ingest run (append-only).
#[derive(Debug, Clone)]
pub struct NewDimFileLog {
    pub source_name: String,
    pub target:      String,  // serde snake_case of DimTarget: "dim_location" | "dim_time"
    pub file_path:   String,
    pub row_count:   i64,
}
```

- [ ] **Step 5: Export from `core/src/models/mod.rs`**

Add `pub mod dim_file_log;` to the mod.rs. Open `core/src/models/mod.rs` and add:

```rust
pub mod dim_file_log;
```

- [ ] **Step 6: Check compilation**

```bash
cargo build -p state-search-core 2>&1
```

Expected: compile errors in the repository files that still use `i64` for `location_id`/`time_id` — these are fixed in Task 5. The model files themselves should be error-free. If errors mention `is_empty` or `date_floor`, verify those methods are still present.

- [ ] **Step 7: Run existing model tests**

```bash
cargo test -p state-search-core models 2>&1
```

Expected: `new_normalized_import_fields` passes with the new `Uuid` types.

- [ ] **Step 8: Commit**

```bash
git add core/src/models/location.rs core/src/models/time.rs \
        core/src/models/normalized_import.rs \
        core/src/models/dim_file_log.rs core/src/models/mod.rs
git commit -m "feat(core/models): migrate dim PKs to Uuid, add NewDimFileLog"
```

---

### Task 5: Update repositories — location, time, normalized_import; add dim_file_log

**Files:**
- Modify: `core/src/repositories/location.rs`
- Modify: `core/src/repositories/time.rs`
- Modify: `core/src/repositories/normalized_import.rs`
- Create: `core/src/repositories/dim_file_log.rs`
- Modify: `core/src/repositories/mod.rs`

The SQL changes are:
- Location/time: `ON CONFLICT (id) DO UPDATE` (was `ON CONFLICT (county/year/...)`)
- `bulk_create`: add `id` column, `::uuid[]` for location_id/time_id, `ON CONFLICT (id) DO NOTHING`
- New dim_file_log repo: `insert` and `exists_for_file`

Since these touch SQL, we cannot unit-test the queries without a DB. We verify via `cargo build`.

- [ ] **Step 1: Update `core/src/repositories/location.rs`**

Replace the entire file:

```rust
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::Result,
    models::location::{Location, NewLocation},
};

pub struct LocationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> LocationRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Upsert a location by its deterministic UUID PK and return the id.
    pub async fn upsert(&self, loc: NewLocation) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_location (id, county, country, zip_code, fips_code, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO UPDATE
                 SET county     = EXCLUDED.county,
                     country    = EXCLUDED.country,
                     zip_code   = EXCLUDED.zip_code,
                     fips_code  = EXCLUDED.fips_code,
                     latitude   = EXCLUDED.latitude,
                     longitude  = EXCLUDED.longitude
             RETURNING id",
        )
        .bind(loc.id)
        .bind(loc.county)
        .bind(loc.country)
        .bind(loc.zip_code)
        .bind(loc.fips_code)
        .bind(loc.latitude)
        .bind(loc.longitude)
        .fetch_one(self.pool)
        .await?;

        Ok(id)
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<Location>> {
        let row = sqlx::query_as::<_, Location>("SELECT * FROM dim_location WHERE id = $1")
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
        Ok(row)
    }

    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<Location>> {
        let rows = sqlx::query_as::<_, Location>(
            "SELECT * FROM dim_location ORDER BY county, zip_code LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    /// Upsert using an existing transaction.
    pub async fn upsert_with_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        loc: NewLocation,
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_location (id, county, country, zip_code, fips_code, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO UPDATE
                 SET county     = EXCLUDED.county,
                     country    = EXCLUDED.country,
                     zip_code   = EXCLUDED.zip_code,
                     fips_code  = EXCLUDED.fips_code,
                     latitude   = EXCLUDED.latitude,
                     longitude  = EXCLUDED.longitude
             RETURNING id",
        )
        .bind(loc.id)
        .bind(loc.county)
        .bind(loc.country)
        .bind(loc.zip_code)
        .bind(loc.fips_code)
        .bind(loc.latitude)
        .bind(loc.longitude)
        .fetch_one(&mut **tx)
        .await?;
        Ok(id)
    }
}
```

- [ ] **Step 2: Update `core/src/repositories/time.rs`**

Replace the entire file:

```rust
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::Result,
    models::time::{NewTimePeriod, TimePeriod},
};

pub struct TimeRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> TimeRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert(&self, t: NewTimePeriod) -> Result<Uuid> {
        let date_floor = t.date_floor();
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_time (id, year, quarter, month, day, date_floor)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
                 SET year       = EXCLUDED.year,
                     quarter    = EXCLUDED.quarter,
                     month      = EXCLUDED.month,
                     day        = EXCLUDED.day,
                     date_floor = EXCLUDED.date_floor
             RETURNING id",
        )
        .bind(t.id)
        .bind(t.year)
        .bind(t.quarter)
        .bind(t.month)
        .bind(t.day)
        .bind(date_floor)
        .fetch_one(self.pool)
        .await?;
        Ok(id)
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<TimePeriod>> {
        let row = sqlx::query_as::<_, TimePeriod>("SELECT * FROM dim_time WHERE id = $1")
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
        Ok(row)
    }

    pub async fn upsert_with_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        t: NewTimePeriod,
    ) -> Result<Uuid> {
        let date_floor = t.date_floor();
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_time (id, year, quarter, month, day, date_floor)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
                 SET year       = EXCLUDED.year,
                     quarter    = EXCLUDED.quarter,
                     month      = EXCLUDED.month,
                     day        = EXCLUDED.day,
                     date_floor = EXCLUDED.date_floor
             RETURNING id",
        )
        .bind(t.id)
        .bind(t.year)
        .bind(t.quarter)
        .bind(t.month)
        .bind(t.day)
        .bind(date_floor)
        .fetch_one(&mut **tx)
        .await?;
        Ok(id)
    }
}
```

- [ ] **Step 3: Update `core/src/repositories/normalized_import.rs`**

Replace the `bulk_create` method (keep `delete_by_ingest_run` unchanged). The change: add `id` as first column, update `location_id`/`time_id` to `::uuid[]`, add `ON CONFLICT (id) DO NOTHING`.

```rust
use sqlx::PgPool;
use uuid::Uuid;

use crate::{error::Result, models::normalized_import::NewNormalizedImport};

pub struct NormalizedImportRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> NormalizedImportRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn bulk_create(&self, rows: Vec<NewNormalizedImport>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let len = rows.len();
        let mut ids              = Vec::with_capacity(len);
        let mut raw_import_ids   = Vec::with_capacity(len);
        let mut location_ids     = Vec::with_capacity(len);
        let mut time_ids         = Vec::with_capacity(len);
        let mut source_names     = Vec::with_capacity(len);
        let mut ingest_run_ids   = Vec::with_capacity(len);
        let mut normalized_datas = Vec::with_capacity(len);

        for row in rows {
            ids             .push(row.id);
            raw_import_ids  .push(row.raw_import_id);
            location_ids    .push(row.location_id);
            time_ids        .push(row.time_id);
            source_names    .push(row.source_name);
            ingest_run_ids  .push(row.ingest_run_id);
            normalized_datas.push(row.normalized_data);
        }

        sqlx::query(
            "INSERT INTO normalized_imports
                 (id, raw_import_id, location_id, time_id, source_name, ingest_run_id, normalized_data)
             SELECT * FROM UNNEST(
                 $1::uuid[], $2::bigint[], $3::uuid[], $4::uuid[], $5::text[], $6::uuid[], $7::jsonb[]
             )
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&raw_import_ids)
        .bind(&location_ids)
        .bind(&time_ids)
        .bind(&source_names)
        .bind(&ingest_run_ids)
        .bind(&normalized_datas)
        .execute(self.pool)
        .await?;

        Ok(len as u64)
    }

    pub async fn delete_by_ingest_run(pool: &PgPool, ingest_run_id: Uuid) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM normalized_imports WHERE ingest_run_id = $1",
        )
        .bind(ingest_run_id)
        .execute(pool)
        .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_by_ingest_run_signature_exists() {
        let _ = NormalizedImportRepository::delete_by_ingest_run;
    }

    #[test]
    fn bulk_create_signature_exists() {
        let _ = NormalizedImportRepository::bulk_create;
    }
}
```

- [ ] **Step 4: Create `core/src/repositories/dim_file_log.rs`**

```rust
use sqlx::PgPool;

use crate::{error::Result, models::dim_file_log::NewDimFileLog};

pub struct DimFileLogRepository;

impl DimFileLogRepository {
    /// Append a log entry after a successful dim ingest run.
    pub async fn insert(pool: &PgPool, entry: &NewDimFileLog) -> Result<()> {
        sqlx::query(
            "INSERT INTO dim_file_log (source_name, target, file_path, row_count)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&entry.source_name)
        .bind(&entry.target)
        .bind(&entry.file_path)
        .bind(entry.row_count)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Returns true if this (source_name, file_path) pair has been previously ingested.
    /// The same physical file registered under a different source name is treated as distinct.
    pub async fn exists_for_file(
        pool: &PgPool,
        source_name: &str,
        file_path: &str,
    ) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dim_file_log WHERE source_name = $1 AND file_path = $2)",
        )
        .bind(source_name)
        .bind(file_path)
        .fetch_one(pool)
        .await?;
        Ok(exists)
    }
}
```

- [ ] **Step 5: Export from `core/src/repositories/mod.rs`**

Add `pub mod dim_file_log;`:

```rust
pub mod dim_file_log;
pub mod import;
pub mod import_schema;
pub mod location;
pub mod normalized_import;
pub mod time;
```

- [ ] **Step 6: Build core crate**

```bash
cargo build -p state-search-core 2>&1
```

Expected: no errors. The API crate (`find_by_id` callers) may produce errors — check `api/` for usages of `location_id: i64` or `time_id: i64` in handlers and update those call sites too if they appear.

- [ ] **Step 7: Check API crate compiles**

```bash
cargo build -p state-search-api 2>&1
```

Fix any type errors from the `i64` → `Uuid` changes in the API's location/time handlers. These are likely in `api/src/` handlers that call `find_by_id` or reference `Location.id`.

- [ ] **Step 8: Run existing repository tests**

```bash
cargo test -p state-search-core 2>&1
```

Expected: all existing tests pass.

- [ ] **Step 9: Commit**

```bash
git add core/src/repositories/location.rs core/src/repositories/time.rs \
        core/src/repositories/normalized_import.rs \
        core/src/repositories/dim_file_log.rs core/src/repositories/mod.rs
git commit -m "feat(core/repos): update location/time/normalized_import repos to UUID PKs, add dim_file_log repo"
```

---

## Chunk 3: Pipeline wiring

### Task 6: `row.rs` — add `strip_excluded_columns`

**Files:**
- Modify: `ingest/src/pipeline/row.rs`

Add a function that filters both the headers slice and a `StringRecord` together by index, returning a new `(Vec<String>, csv::StringRecord)` with excluded columns removed. The existing `row_to_canonical` signature is unchanged; callers will call `strip_excluded_columns` first when `exclude_columns` is non-empty.

- [ ] **Step 1: Write failing test**

Add to the `#[cfg(test)]` block in `ingest/src/pipeline/row.rs`:

```rust
#[test]
fn strip_excluded_columns_removes_by_name_case_insensitive() {
    use std::collections::HashSet;
    let headers = vec!["State".to_string(), "YEAR".to_string(), "InternalNotes".to_string()];
    let record = csv::StringRecord::from(vec!["CO", "2024", "ignore_me"]);
    let excluded: HashSet<String> = ["InternalNotes".to_lowercase()].into();

    let (h, r) = strip_excluded_columns(&headers, &record, &excluded);
    assert_eq!(h, vec!["State".to_string(), "YEAR".to_string()]);
    assert_eq!(r.iter().collect::<Vec<_>>(), vec!["CO", "2024"]);
}

#[test]
fn strip_excluded_columns_empty_set_returns_unchanged() {
    use std::collections::HashSet;
    let headers = vec!["State".to_string(), "Year".to_string()];
    let record = csv::StringRecord::from(vec!["CO", "2024"]);
    let excluded: HashSet<String> = HashSet::new();

    let (h, r) = strip_excluded_columns(&headers, &record, &excluded);
    assert_eq!(h.len(), 2);
    assert_eq!(r.len(), 2);
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p state-search-ingest strip_excluded 2>&1 | head -15
```

Expected: compile error — `strip_excluded_columns` not defined.

- [ ] **Step 3: Implement**

Add to `ingest/src/pipeline/row.rs` (before the tests block):

```rust
use std::collections::HashSet;

/// Filter headers and record fields together by index, removing excluded columns.
/// `excluded` keys must already be lowercased.
/// Preserves header/value alignment since csv::StringRecord is index-based.
pub(super) fn strip_excluded_columns(
    headers: &[String],
    record: &csv::StringRecord,
    excluded: &HashSet<String>,
) -> (Vec<String>, csv::StringRecord) {
    let (filtered_headers, filtered_values): (Vec<String>, Vec<&str>) = headers
        .iter()
        .zip(record.iter())
        .filter(|(h, _)| !excluded.contains(&h.to_lowercase()))
        .map(|(h, v)| (h.clone(), v))
        .unzip();
    (filtered_headers, csv::StringRecord::from(filtered_values))
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p state-search-ingest row 2>&1
```

Expected: all row tests pass including the two new ones.

- [ ] **Step 5: Commit**

```bash
git add ingest/src/pipeline/row.rs
git commit -m "feat(ingest/row): add strip_excluded_columns for pre-mapping column filtering"
```

---

### Task 7: `dimensions.rs` — update LocationCache and UUID derivation

**Files:**
- Modify: `ingest/src/pipeline/dimensions.rs`

Two changes:
1. `LocationCache` value type `i64` → `Uuid`
2. Dim row UUIDs are now derived via `derive_uuid` using hardcoded internal keys (all six location fields; year/quarter/month/day for time)

The hardcoded internal keys for the fact path:
- `dim_location`: `["county", "country", "fips_code", "latitude", "longitude", "zip_code"]` (alphabetical)
- `dim_time`: `["day", "month", "quarter", "year"]` (alphabetical)

- [ ] **Step 1: Write a compile-check test**

Add to `ingest/src/pipeline/dimensions.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn location_cache_holds_uuid_values() {
        let cache = new_location_cache();
        // If this compiles, the type is correct
        let _: LocationCache = cache;
    }
}
```

- [ ] **Step 2: Rewrite `dimensions.rs`**

Replace the full file:

```rust
use std::{collections::HashMap, sync::Arc};

use state_search_core::{
    models::{location::NewLocation, time::NewTimePeriod},
    repositories::{location::LocationRepository, time::TimeRepository},
    Db,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::transforms::FieldValue;
use super::row::{f64_from_field, i16_from_field, str_from_field};
use super::uuid::derive_uuid;

/// Shared cache mapping a location's content hash to its `dim_location` UUID PK.
pub(super) type LocationCache = Arc<tokio::sync::RwLock<HashMap<String, Uuid>>>;

pub(super) fn new_location_cache() -> LocationCache {
    Arc::new(tokio::sync::RwLock::new(HashMap::new()))
}

// Hardcoded internal unique-key columns for fact-path dim upserts (alphabetical for stable sort)
const LOC_KEY_COLS: &[&str] = &["county", "country", "fips_code", "latitude", "longitude", "zip_code"];
const TIME_KEY_COLS: &[&str] = &["day", "month", "quarter", "year"];

// ── Location ──────────────────────────────────────────────────────────────────

pub(super) async fn resolve_location_id(
    transformed: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
    db: &Db,
    row_num: u64,
    cache: &LocationCache,
) -> Option<Uuid> {
    let loc = resolve_location(transformed, default_country).ok()?;
    let key = location_cache_key(&loc);

    if let Some(&id) = cache.read().await.get(&key) {
        return Some(id);
    }

    match LocationRepository::new(db).upsert(loc).await {
        Ok(id) => {
            cache.write().await.insert(key, id);
            debug!(row = row_num, ?id, "resolved location");
            Some(id)
        }
        Err(e) => {
            warn!(row = row_num, error = %e, "could not resolve location");
            None
        }
    }
}

fn resolve_location(
    map: &HashMap<String, FieldValue>,
    default_country: Option<&str>,
) -> Result<NewLocation, ()> {
    // Build a temporary map including the default country so derive_uuid sees it
    let mut tmp = map.clone();
    if tmp.get("country").map_or(true, |v| matches!(v, FieldValue::Null)) {
        if let Some(c) = default_country {
            tmp.insert("country".to_string(), FieldValue::Str(c.to_string()));
        }
    }

    let loc = NewLocation {
        id:        derive_uuid(LOC_KEY_COLS, &tmp),
        county:    str_from_field(map, "county"),
        country:   str_from_field(map, "country")
                       .or_else(|| default_country.map(str::to_string)),
        zip_code:  str_from_field(map, "zip_code"),
        fips_code: str_from_field(map, "fips_code"),
        latitude:  f64_from_field(map, "latitude"),
        longitude: f64_from_field(map, "longitude"),
    };
    if loc.is_empty() { Err(()) } else { Ok(loc) }
}

fn location_cache_key(loc: &NewLocation) -> String {
    // Cache key uses the UUID itself — guaranteed unique and consistent
    loc.id.to_string()
}

// ── Time ──────────────────────────────────────────────────────────────────────

pub(super) async fn resolve_time_id(
    transformed: &HashMap<String, FieldValue>,
    db: &Db,
    row_num: u64,
) -> Option<Uuid> {
    match resolve_time(transformed) {
        Ok(t) => match TimeRepository::new(db).upsert(t).await {
            Ok(id) => {
                debug!(row = row_num, ?id, "resolved time");
                Some(id)
            }
            Err(e) => {
                warn!(row = row_num, error = %e, "could not resolve time");
                None
            }
        },
        Err(_) => None,
    }
}

fn resolve_time(map: &HashMap<String, FieldValue>) -> Result<NewTimePeriod, ()> {
    let year = i16_from_field(map, "year").ok_or(())?;
    Ok(NewTimePeriod {
        id:      derive_uuid(TIME_KEY_COLS, map),
        year,
        quarter: i16_from_field(map, "quarter").filter(|&q| (1..=4).contains(&q)),
        month:   i16_from_field(map, "month")  .filter(|&m| (1..=12).contains(&m)),
        day:     i16_from_field(map, "day")    .filter(|&d| (1..=31).contains(&d)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_cache_holds_uuid_values() {
        let cache = new_location_cache();
        let _: LocationCache = cache;
    }
}
```

- [ ] **Step 3: Build**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: errors in `record.rs` and `derived.rs` (they still reference `Option<i64>` for location/time IDs). Those are fixed in Tasks 8–9.

- [ ] **Step 4: Run the compile-check test**

```bash
cargo test -p state-search-ingest dimensions 2>&1
```

Expected: `location_cache_holds_uuid_values` passes.

- [ ] **Step 5: Commit**

```bash
git add ingest/src/pipeline/dimensions.rs
git commit -m "feat(ingest/dimensions): migrate LocationCache to Uuid, derive dim UUIDs via derive_uuid"
```

---

### Task 8: `derived.rs` — update `location_id`/`time_id` parameter types

**Files:**
- Modify: `ingest/src/pipeline/derived.rs`

Change `location_id: Option<i64>` and `time_id: Option<i64>` to `Option<Uuid>` in the `resolve_derived` signature and its SQL binds.

- [ ] **Step 1: Update `derived.rs`**

Change the function signature and binds:

```rust
use state_search_core::{config::SourceConfig, Db};
use tracing::warn;
use uuid::Uuid;

pub(super) async fn resolve_derived(
    normalized_data: &mut serde_json::Value,
    source: &SourceConfig,
    location_id: Option<Uuid>,
    time_id: Option<Uuid>,
    db: &Db,
    row_num: u64,
) -> Result<(), String> {
```

The rest of the function body is unchanged — the `.bind(id)` calls bind a `Uuid` now (sqlx handles this automatically since `Uuid` implements `sqlx::Encode<Postgres>`).

- [ ] **Step 2: Build**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: errors only in `record.rs` (call site mismatch). `derived.rs` itself should compile.

- [ ] **Step 3: Run derived tests**

```bash
cargo test -p state-search-ingest derived 2>&1
```

Expected: existing test passes.

- [ ] **Step 4: Commit**

```bash
git add ingest/src/pipeline/derived.rs
git commit -m "feat(ingest/derived): update location_id/time_id to Option<Uuid>"
```

---

### Task 9: `record.rs` — wire in UUID derivation and updated types

**Files:**
- Modify: `ingest/src/pipeline/record.rs`

Three changes:
1. Call `strip_excluded_columns` before `row_to_canonical` (when `exclude_columns` is non-empty)
2. `location_id`/`time_id` are now `Option<Uuid>` (from dimensions.rs)
3. Derive `normalized_imports.id` via `derive_uuid` when `unique_key` is non-empty; else `Uuid::new_v4()`

Also add startup validation helpers (called from `mod.rs` in Task 11):

- [ ] **Step 1: Rewrite `record.rs`**

```rust
use std::collections::{HashMap, HashSet};

use state_search_core::{
    config::OnFailure,
    models::normalized_import::NewNormalizedImport,
    Db,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::transforms::{chain::apply_chain, resolve::ResolvedFieldMap, FieldValue};
use super::{
    IngestError,
    derived::resolve_derived,
    dimensions::{resolve_location_id, resolve_time_id, LocationCache},
    row::{canonical_to_json, row_to_canonical, strip_excluded_columns},
    schema::{collect_normalized_data, validate_completeness},
    uuid::derive_uuid,
};

pub(super) enum RowOutcome {
    Proceed(HashMap<String, FieldValue>),
    Skip,
    Abort(anyhow::Error),
}

pub(super) async fn process_record(
    record: csv::StringRecord,
    row_num: u64,
    source: &state_search_core::config::SourceConfig,
    file_path: &str,
    ingest_run_id: Uuid,
    resolved: &ResolvedFieldMap,
    headers: &[String],
    default_country: Option<&str>,
    db: &Db,
    location_cache: &LocationCache,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
) -> anyhow::Result<Option<NewNormalizedImport>> {
    let source_name = source.name.as_str();

    // Strip excluded columns before any processing
    let (effective_headers, effective_record);
    let (h_ref, r_ref): (&[String], &csv::StringRecord) = if source.exclude_columns.is_empty() {
        (headers, &record)
    } else {
        let excluded: HashSet<String> = source.exclude_columns.iter()
            .map(|c| c.to_lowercase())
            .collect();
        let (fh, fr) = strip_excluded_columns(headers, &record, &excluded);
        effective_headers = fh;
        effective_record  = fr;
        (&effective_headers, &effective_record)
    };

    let raw = row_to_canonical(h_ref, r_ref);
    debug!(row = row_num, "parsed CSV row");

    let raw_import_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(Option::<i64>::None)
    .bind(file_path)
    .bind(&canonical_to_json(&raw))
    .fetch_one(db)
    .await?;

    let transformed = match apply_all_transforms(&raw, resolved, row_num, source_name, file_path) {
        RowOutcome::Proceed(t) => t,
        RowOutcome::Skip       => return Ok(None),
        RowOutcome::Abort(e)   => return Err(e),
    };

    if let Err(msg) = validate_completeness(&transformed, schema, derived_fields, row_num) {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    let (location_id, time_id) = tokio::join!(
        resolve_location_id(&transformed, default_country, db, row_num, location_cache),
        resolve_time_id(&transformed, db, row_num),
    );

    let mut normalized_data = serde_json::json!({});
    if let Err(msg) = resolve_derived(
        &mut normalized_data, source, location_id, time_id, db, row_num,
    ).await {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    let normalized_data = match collect_normalized_data(
        &transformed, schema, derived_fields, &normalized_data, row_num,
    ) {
        Ok(data) => data,
        Err(msg) => {
            warn!(row = row_num, "{}", msg);
            return Ok(None);
        }
    };

    // Derive normalized_imports.id: deterministic if unique_key set, else random.
    // location_id / time_id may appear in unique_key — inject them as Str values so
    // derive_uuid can find them by name.
    let row_id = if source.unique_key.is_empty() {
        Uuid::new_v4()
    } else {
        let key_set: HashSet<&str> = source.unique_key.iter().map(String::as_str).collect();
        let mut key_map: HashMap<String, FieldValue> = transformed.clone();
        if key_set.contains("location_id") {
            key_map.insert(
                "location_id".to_string(),
                FieldValue::Str(location_id.map(|id| id.to_string()).unwrap_or_default()),
            );
        }
        if key_set.contains("time_id") {
            key_map.insert(
                "time_id".to_string(),
                FieldValue::Str(time_id.map(|id| id.to_string()).unwrap_or_default()),
            );
        }
        let key_cols: Vec<&str> = source.unique_key.iter().map(String::as_str).collect();
        derive_uuid(&key_cols, &key_map)
    };

    debug!(row = row_num, "row ready");
    Ok(Some(NewNormalizedImport {
        id:              row_id,
        raw_import_id:   Some(raw_import_id),
        location_id,
        time_id,
        source_name:     source_name.to_string(),
        ingest_run_id,
        normalized_data,
    }))
}

fn apply_all_transforms(
    raw: &HashMap<String, FieldValue>,
    resolved: &ResolvedFieldMap,
    row_num: u64,
    source_name: &str,
    file_path: &str,
) -> RowOutcome {
    resolved
        .iter()
        .fold(RowOutcome::Proceed(HashMap::new()), |outcome, (canonical, resolved_field)| {
            let RowOutcome::Proceed(mut acc) = outcome else {
                return outcome;
            };

            let raw_val = raw
                .get(resolved_field.source_col.as_str())
                .cloned()
                .unwrap_or(FieldValue::Null);

            match apply_chain(raw_val, &resolved_field.chain) {
                Ok(v) => {
                    acc.insert(canonical.clone(), v);
                    RowOutcome::Proceed(acc)
                }
                Err(OnFailure::SkipRow) => {
                    warn!(row = row_num, field = canonical, source = source_name, "SkipRow: discarding row");
                    RowOutcome::Skip
                }
                Err(OnFailure::SkipDataset) => RowOutcome::Abort(
                    IngestError::DatasetSkipped {
                        file: file_path.to_string(),
                        reason: format!("transform failure on field '{}' at row {}", canonical, row_num),
                    }
                    .into(),
                ),
                Err(OnFailure::Ignore) => unreachable!(),
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_all_transforms_proceed_on_empty_resolved() {
        let raw = HashMap::new();
        let resolved = ResolvedFieldMap::new();
        match apply_all_transforms(&raw, &resolved, 1, "src", "f.csv") {
            RowOutcome::Proceed(m) => assert!(m.is_empty()),
            _ => panic!("expected Proceed"),
        }
    }
}
```

- [ ] **Step 2: Build**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: errors in `mod.rs` (caller of `process_record` expecting `i64` IDs) — fixed in Task 11. `record.rs` itself should compile.

- [ ] **Step 3: Commit**

```bash
git add ingest/src/pipeline/record.rs
git commit -m "feat(ingest/record): wire exclude_columns strip and deterministic UUID for normalized_imports.id"
```

---

## Chunk 4: Dim-only pipeline and CLI dispatch

### Task 10: `dim.rs` — new dim-only pipeline

**Files:**
- Create: `ingest/src/pipeline/dim.rs`

The dim pipeline processes CSV rows, derives UUIDs, and bulk-upserts into `dim_location` or `dim_time`. It does not write `raw_imports`, `normalized_imports`, or `import_schema`. After a successful run it writes a `dim_file_log` row.

- [ ] **Step 1: Write validation helpers with tests first**

Create `ingest/src/pipeline/dim.rs` with the validation logic and tests:

```rust
use std::collections::HashSet;

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::{DimTarget, SourceConfig},
    models::{
        dim_file_log::NewDimFileLog,
        location::NewLocation,
        time::NewTimePeriod,
    },
    repositories::{
        dim_file_log::DimFileLogRepository,
        location::LocationRepository,
        time::TimeRepository,
    },
    Db,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::transforms::{chain::apply_chain, resolve::build_resolved_field_map, FieldValue};
use super::{
    row::{row_to_canonical, strip_excluded_columns},
    uuid::derive_uuid,
    ROW_CONCURRENCY, OBS_BATCH_SIZE,
};

// ── Startup validation ────────────────────────────────────────────────────────

/// Validate that every entry in `exclude_columns` exists in the CSV headers
/// (case-insensitive). Returns Err with a descriptive message on failure.
pub(super) fn validate_exclude_columns(
    source_name: &str,
    exclude_columns: &[String],
    headers: &[String],
) -> anyhow::Result<()> {
    let header_set: HashSet<String> = headers.iter().map(|h| h.to_lowercase()).collect();
    for col in exclude_columns {
        if !header_set.contains(&col.to_lowercase()) {
            anyhow::bail!(
                "exclude_columns: '{}' not found in CSV headers for source '{}'",
                col, source_name
            );
        }
    }
    Ok(())
}

/// `location_id` and `time_id` are always valid unique_key entries — they are FK
/// columns resolved from dim tables during ingestion and injected into the key map.
const FK_KEY_COLUMNS: &[&str] = &["location_id", "time_id"];

/// Validate that every entry in `unique_key` is either a canonical name declared in
/// `fields` or `derived`, or one of the FK key columns (`location_id`, `time_id`).
/// Returns Err on the first invalid entry.
pub(super) fn validate_unique_key(source: &SourceConfig) -> anyhow::Result<()> {
    let canonical_names: HashSet<&str> = source.fields.values()
        .map(|f| f.canonical.as_str())
        .chain(source.derived.keys().map(String::as_str))
        .chain(FK_KEY_COLUMNS.iter().copied())
        .collect();
    for col in &source.unique_key {
        if !canonical_names.contains(col.as_str()) {
            anyhow::bail!(
                "unique_key: '{}' is not a declared canonical field in source '{}'",
                col, source.name
            );
        }
    }
    Ok(())
}

// ── Dim-only ingest run ───────────────────────────────────────────────────────

pub async fn run_dim(source: &SourceConfig, file_path: &str, db: &Db) -> anyhow::Result<u64> {
    let target = source.target.expect("run_dim called without a target");
    let source_name = source.name.as_str();

    if source.unique_key.is_empty() {
        warn!(source = source_name, "unique_key is empty — dim rows will get random UUIDs; re-runs will not be idempotent");
    }

    // Check for prior ingest of this file
    if DimFileLogRepository::exists_for_file(db, source_name, file_path).await? {
        warn!(source = source_name, file = file_path, "file has been previously ingested into {}; re-ingesting (upserts are idempotent)", target_str(target));
    }

    let resolved = build_resolved_field_map(source)
        .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

    let mut reader = csv::Reader::from_path(file_path)
        .with_context(|| format!("cannot open {file_path}"))?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    // Startup validations
    validate_exclude_columns(source_name, &source.exclude_columns, &headers)?;
    validate_unique_key(source)?;

    let excluded: HashSet<String> = source.exclude_columns.iter()
        .map(|c| c.to_lowercase())
        .collect();

    let total = match target {
        DimTarget::DimLocation => run_dim_location(source, file_path, &resolved, &headers, &excluded, &mut reader, db).await?,
        DimTarget::DimTime     => run_dim_time(source, file_path, &resolved, &headers, &excluded, &mut reader, db).await?,
    };

    // Record successful run
    DimFileLogRepository::insert(db, &NewDimFileLog {
        source_name: source_name.to_string(),
        target:      target_str(target).to_string(),
        file_path:   file_path.to_string(),
        row_count:   total as i64,
    }).await?;

    info!(source = source_name, file = file_path, target = target_str(target), rows = total, "dim ingest complete");
    Ok(total)
}

async fn run_dim_location(
    source: &SourceConfig,
    file_path: &str,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    headers: &[String],
    excluded: &HashSet<String>,
    reader: &mut csv::Reader<std::fs::File>,
    db: &Db,
) -> anyhow::Result<u64> {
    let source_name = source.name.as_str();
    let key_cols: Vec<String> = source.unique_key.clone();

    let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
        let rn = i as u64 + 1;
        let key_cols = key_cols.clone();
        async move {
            let record = result.map_err(anyhow::Error::from)?;
            let (h, r) = if excluded.is_empty() {
                (headers.to_vec(), record.clone())
            } else {
                strip_excluded_columns(headers, &record, excluded)
            };
            let raw = row_to_canonical(&h, &r);
            let transformed = match apply_transforms_dim(&raw, resolved, rn, source_name)? {
                Some(t) => t,
                None    => return Ok(vec![]),  // SkipRow — discard this row, continue stream
            };

            let id = if key_cols.is_empty() {
                Uuid::new_v4()
            } else {
                let cols: Vec<&str> = key_cols.iter().map(String::as_str).collect();
                derive_uuid(&cols, &transformed)
            };

            let loc = NewLocation {
                id,
                county:    field_as_str(&transformed, "county"),
                country:   field_as_str(&transformed, "country"),
                zip_code:  field_as_str(&transformed, "zip_code"),
                fips_code: field_as_str(&transformed, "fips_code"),
                latitude:  field_as_f64(&transformed, "latitude"),
                longitude: field_as_f64(&transformed, "longitude"),
            };
            Ok::<Vec<NewLocation>, anyhow::Error>(vec![loc])
        }
    });

    crate::stream::batched_channel_sink(
        work,
        ROW_CONCURRENCY,
        ROW_CONCURRENCY * 4,
        OBS_BATCH_SIZE,
        |batch: Vec<NewLocation>| async move {
            bulk_upsert_locations(batch, db).await.map_err(Into::into)
        },
        |total| info!(source = source_name, file = file_path, rows = total, "dim_location batch flushed"),
    )
    .await
}

async fn run_dim_time(
    source: &SourceConfig,
    file_path: &str,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    headers: &[String],
    excluded: &HashSet<String>,
    reader: &mut csv::Reader<std::fs::File>,
    db: &Db,
) -> anyhow::Result<u64> {
    let source_name = source.name.as_str();
    let key_cols: Vec<String> = source.unique_key.clone();

    let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
        let rn = i as u64 + 1;
        let key_cols = key_cols.clone();
        async move {
            let record = result.map_err(anyhow::Error::from)?;
            let (h, r) = if excluded.is_empty() {
                (headers.to_vec(), record.clone())
            } else {
                strip_excluded_columns(headers, &record, excluded)
            };
            let raw = row_to_canonical(&h, &r);
            let transformed = match apply_transforms_dim(&raw, resolved, rn, source_name)? {
                Some(t) => t,
                None    => return Ok(vec![]),  // SkipRow — discard this row, continue stream
            };

            let year = super::row::i16_from_field(&transformed, "year")
                .ok_or_else(|| anyhow::anyhow!("row {rn}: 'year' field missing or invalid for dim_time"))?;

            let id = if key_cols.is_empty() {
                Uuid::new_v4()
            } else {
                let cols: Vec<&str> = key_cols.iter().map(String::as_str).collect();
                derive_uuid(&cols, &transformed)
            };

            let t = NewTimePeriod {
                id,
                year,
                quarter: super::row::i16_from_field(&transformed, "quarter").filter(|&q| (1..=4).contains(&q)),
                month:   super::row::i16_from_field(&transformed, "month")  .filter(|&m| (1..=12).contains(&m)),
                day:     super::row::i16_from_field(&transformed, "day")    .filter(|&d| (1..=31).contains(&d)),
            };
            Ok::<Vec<NewTimePeriod>, anyhow::Error>(vec![t])
        }
    });

    crate::stream::batched_channel_sink(
        work,
        ROW_CONCURRENCY,
        ROW_CONCURRENCY * 4,
        OBS_BATCH_SIZE,
        |batch: Vec<NewTimePeriod>| async move {
            bulk_upsert_times(batch, db).await.map_err(Into::into)
        },
        |total| info!(source = source_name, file = file_path, rows = total, "dim_time batch flushed"),
    )
    .await
}

// ── Flush helpers ─────────────────────────────────────────────────────────────

async fn bulk_upsert_locations(locs: Vec<NewLocation>, db: &Db) -> state_search_core::error::Result<u64> {
    let n = locs.len() as u64;
    for loc in locs {
        LocationRepository::new(db).upsert(loc).await?;
    }
    Ok(n)
}

async fn bulk_upsert_times(times: Vec<NewTimePeriod>, db: &Db) -> state_search_core::error::Result<u64> {
    let n = times.len() as u64;
    for t in times {
        TimeRepository::new(db).upsert(t).await?;
    }
    Ok(n)
}

// ── Row processing helpers ────────────────────────────────────────────────────

/// Returns `Ok(None)` for `SkipRow` (caller should return `Ok(vec![])` to skip the row),
/// `Err` for `SkipDataset` (aborts the whole file), `Ok(Some(_))` for success.
fn apply_transforms_dim(
    raw: &std::collections::HashMap<String, FieldValue>,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    row_num: u64,
    source_name: &str,
) -> anyhow::Result<Option<std::collections::HashMap<String, FieldValue>>> {
    let mut out = std::collections::HashMap::new();
    for (canonical, rf) in resolved.iter() {
        let raw_val = raw.get(rf.source_col.as_str()).cloned().unwrap_or(FieldValue::Null);
        match apply_chain(raw_val, &rf.chain) {
            Ok(v) => { out.insert(canonical.clone(), v); }
            Err(state_search_core::config::OnFailure::SkipRow) => {
                warn!(row = row_num, field = canonical, source = source_name, "SkipRow in dim pipeline — discarding row");
                return Ok(None);
            }
            Err(state_search_core::config::OnFailure::SkipDataset) => {
                anyhow::bail!("row {row_num}: SkipDataset on field '{canonical}' in source '{source_name}'");
            }
            Err(state_search_core::config::OnFailure::Ignore) => unreachable!(),
        }
    }
    Ok(Some(out))
}

fn field_as_str(map: &std::collections::HashMap<String, FieldValue>, key: &str) -> Option<String> {
    match map.get(key)? {
        FieldValue::Str(s) => Some(s.clone()),
        _ => None,
    }
}

fn field_as_f64(map: &std::collections::HashMap<String, FieldValue>, key: &str) -> Option<f64> {
    super::row::f64_from_field(map, key)
}

fn target_str(t: DimTarget) -> &'static str {
    match t {
        DimTarget::DimLocation => "dim_location",
        DimTarget::DimTime     => "dim_time",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use state_search_core::config::{FieldDef, SourceConfig};
    use std::collections::HashMap;

    fn make_source(name: &str, unique_key: Vec<&str>, exclude_cols: Vec<&str>) -> SourceConfig {
        SourceConfig {
            name:            name.to_string(),
            description:     None,
            files:           vec![],
            fields:          HashMap::new(),
            derived:         HashMap::new(),
            target:          Some(DimTarget::DimLocation),
            unique_key:      unique_key.into_iter().map(str::to_string).collect(),
            exclude_columns: exclude_cols.into_iter().map(str::to_string).collect(),
        }
    }

    fn make_source_with_field(name: &str, canonical: &str) -> SourceConfig {
        let mut fields = HashMap::new();
        fields.insert("col".to_string(), FieldDef {
            canonical: canonical.to_string(),
            field_type: "text".to_string(),
            format: None,
            on_failure: None,
            rules: vec![],
        });
        SourceConfig {
            name:            name.to_string(),
            description:     None,
            files:           vec![],
            fields,
            derived:         HashMap::new(),
            target:          Some(DimTarget::DimLocation),
            unique_key:      vec![canonical.to_string()],
            exclude_columns: vec![],
        }
    }

    #[test]
    fn validate_exclude_columns_passes_when_present() {
        let headers = vec!["State".to_string(), "Year".to_string(), "Notes".to_string()];
        assert!(validate_exclude_columns("src", &["Notes".to_string()], &headers).is_ok());
    }

    #[test]
    fn validate_exclude_columns_case_insensitive() {
        let headers = vec!["State".to_string(), "NOTES".to_string()];
        assert!(validate_exclude_columns("src", &["notes".to_string()], &headers).is_ok());
    }

    #[test]
    fn validate_exclude_columns_fails_when_absent() {
        let headers = vec!["State".to_string()];
        let result = validate_exclude_columns("src", &["missing_col".to_string()], &headers);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing_col"));
    }

    #[test]
    fn validate_unique_key_passes_for_declared_canonical() {
        let source = make_source_with_field("src", "fips_code");
        assert!(validate_unique_key(&source).is_ok());
    }

    #[test]
    fn validate_unique_key_fails_for_undeclared_canonical() {
        let source = make_source("src", vec!["nonexistent_field"], vec![]);
        let result = validate_unique_key(&source);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent_field"));
    }

    #[test]
    fn validate_unique_key_empty_passes() {
        let source = make_source("src", vec![], vec![]);
        assert!(validate_unique_key(&source).is_ok());
    }

    #[test]
    fn validate_unique_key_allows_fk_columns() {
        // location_id and time_id are always valid regardless of declared fields
        let source = make_source("src", vec!["location_id", "time_id"], vec![]);
        assert!(validate_unique_key(&source).is_ok());
    }
}
```

- [ ] **Step 2: Declare the module**

In `ingest/src/pipeline/mod.rs`, add:

```rust
pub(super) mod dim;
```

- [ ] **Step 3: Build**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: `dim.rs` compiles. Remaining errors are in `mod.rs` from the fact-path type changes.

- [ ] **Step 4: Run dim tests**

```bash
cargo test -p state-search-ingest dim 2>&1
```

Expected: all 7 validation tests pass.

- [ ] **Step 5: Commit**

```bash
git add ingest/src/pipeline/dim.rs ingest/src/pipeline/mod.rs
git commit -m "feat(ingest/dim): add run_dim pipeline for dimension-only source ingestion"
```

---

### Task 11: `mod.rs` — add `dim_already_ingested`, wire type changes

**Files:**
- Modify: `ingest/src/pipeline/mod.rs`

The `IngestPipeline` gains `dim_already_ingested`. The `process_rows` / `process_record` call sites need updating for the `Option<Uuid>` ID types that now flow through.

- [ ] **Step 1: Update `ingest/src/pipeline/mod.rs`**

Add `dim_already_ingested` method to `IngestPipeline` and ensure the `run` method still compiles with updated types. The full updated `mod.rs`:

```rust
mod derived;
mod dim;
mod dimensions;
mod export;
mod record;
mod row;
mod schema;
mod uuid;

pub(super) use uuid::derive_uuid;

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::SourceConfig,
    repositories::normalized_import::NormalizedImportRepository,
    repositories::dim_file_log::DimFileLogRepository,
    Db,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::transforms::resolve::{build_resolved_field_map, ResolvedFieldMap};
use self::dimensions::{new_location_cache, LocationCache};
use self::export::generate_export_sql;
use self::record::process_record;
use self::row::infer_country_from_path;

pub(super) const ROW_CONCURRENCY: usize = 8;
pub(super) const OBS_BATCH_SIZE: usize = 500;
const EXPORT_BASE: &str = "exports";

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("dataset skipped due to transform failure in '{file}': {reason}")]
    DatasetSkipped { file: String, reason: String },
}

pub struct IngestPipeline<'a> {
    db: &'a Db,
}

impl<'a> IngestPipeline<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Check whether a fact source file has already been ingested (via raw_imports).
    pub async fn already_ingested(&self, file_path: &str) -> anyhow::Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_imports WHERE source_file = $1)",
        )
        .bind(file_path)
        .fetch_one(self.db)
        .await?;
        Ok(exists)
    }

    /// Check whether a dim source file has already been ingested (via dim_file_log).
    pub async fn dim_already_ingested(&self, source_name: &str, file_path: &str) -> anyhow::Result<bool> {
        DimFileLogRepository::exists_for_file(self.db, source_name, file_path).await
            .map_err(Into::into)
    }

    /// Run dim-only ingestion for a source with target set.
    pub async fn run_dim(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        dim::run_dim(source, file_path, self.db).await
    }

    pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        let ingest_run_id = Uuid::new_v4();

        info!(source = source_name, file = file_path, %ingest_run_id, "starting ingest");

        let schema = schema::seed_and_validate(source, self.db)
            .await
            .with_context(|| format!("schema seed/validate failed for source '{source_name}'"))?;

        let derived_fields: HashMap<String, String> = source.derived.iter()
            .map(|(k, v)| (k.clone(), v.field_type.clone()))
            .collect();

        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

        // Validate exclude_columns and unique_key at startup
        dim::validate_exclude_columns(source_name, &source.exclude_columns, &headers)?;
        dim::validate_unique_key(source)?;

        let default_country = infer_country_from_path(file_path);

        let result = self
            .process_rows(
                source, file_path, ingest_run_id,
                &resolved, &schema, &derived_fields,
                &headers, default_country.as_deref(),
                &mut reader,
            )
            .await;

        match result {
            Ok(total) => {
                info!(source = source_name, file = file_path, %ingest_run_id, rows = total, "ingest complete");
                self.write_export_script(source, ingest_run_id, &schema, &derived_fields);
                Ok(total)
            }
            Err(e) => {
                warn!(source = source_name, file = file_path, %ingest_run_id, error = %e,
                    "ingest failed — cleaning up partial normalized_imports");
                match NormalizedImportRepository::delete_by_ingest_run(self.db, ingest_run_id).await {
                    Ok(deleted) => info!(%ingest_run_id, deleted, "cleanup complete"),
                    Err(ce)     => warn!(%ingest_run_id, error = %ce, "cleanup failed"),
                }
                Err(e)
            }
        }
    }

    fn write_export_script(
        &self,
        source: &SourceConfig,
        ingest_run_id: Uuid,
        schema: &HashMap<String, String>,
        derived_fields: &HashMap<String, String>,
    ) {
        let source_name = source.name.as_str();
        let pg_connection = "host=localhost dbname=state_search";
        let sql = generate_export_sql(source_name, ingest_run_id, schema, derived_fields, pg_connection, EXPORT_BASE);
        let dir = format!("{EXPORT_BASE}/{source_name}");
        let path = format!("{dir}/export_{ingest_run_id}.sql");
        if let Err(e) = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, sql)) {
            warn!(source = source_name, %ingest_run_id, error = %e, "failed to write export script");
        } else {
            info!(source = source_name, %ingest_run_id, path, "export script written");
        }
    }

    async fn process_rows(
        &self,
        source: &SourceConfig,
        file_path: &str,
        ingest_run_id: Uuid,
        resolved: &ResolvedFieldMap,
        schema: &HashMap<String, String>,
        derived_fields: &HashMap<String, String>,
        headers: &[String],
        default_country: Option<&str>,
        reader: &mut csv::Reader<std::fs::File>,
    ) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        let db = self.db;
        let location_cache: LocationCache = new_location_cache();

        let work = stream::iter(reader.records().enumerate()).map(|(i, result): (usize, _)| {
            let rn = i as u64 + 1;
            let cache = Arc::clone(&location_cache);
            async move {
                let record = result.map_err(anyhow::Error::from)?;
                let maybe_row = process_record(
                    record, rn, source, file_path, ingest_run_id,
                    resolved, headers, default_country, db, &cache,
                    schema, derived_fields,
                )
                .await?;
                Ok::<Vec<_>, anyhow::Error>(maybe_row.into_iter().collect())
            }
        });

        crate::stream::batched_channel_sink(
            work,
            ROW_CONCURRENCY,
            ROW_CONCURRENCY * 4,
            OBS_BATCH_SIZE,
            |batch| async {
                NormalizedImportRepository::new(db).bulk_create(batch).await.map_err(Into::into)
            },
            |total| info!(source = source_name, file = file_path, rows = total, "batch flushed"),
        )
        .await
    }
}
```

- [ ] **Step 2: Build everything**

```bash
cargo build 2>&1
```

Expected: clean build. Fix any remaining type errors (e.g. API handlers that reference `location_id: i64`).

- [ ] **Step 3: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add ingest/src/pipeline/mod.rs
git commit -m "feat(ingest/pipeline): add dim_already_ingested, run_dim dispatch, startup validations"
```

---

### Task 12: `main.rs` — dispatch by `source.target`

**Files:**
- Modify: `ingest/src/main.rs`

Both `Reload` and `Run` (no `--file`) arms dispatch based on `source.target`:
- dim source: `dim_already_ingested` + `run_dim`
- fact source: `already_ingested` + `run` (existing behavior)

`Run --file`: bypasses skip check for both (existing behavior preserved).

- [ ] **Step 1: Write test**

Add a test to `ingest/src/main.rs` (or a separate test module) verifying the helper logic:

```rust
#[cfg(test)]
mod tests {
    use state_search_core::config::{DimTarget, SourceConfig};
    use std::collections::HashMap;

    fn dim_source() -> SourceConfig {
        SourceConfig {
            name: "locs".to_string(),
            description: None,
            files: vec![],
            fields: HashMap::new(),
            derived: HashMap::new(),
            target: Some(DimTarget::DimLocation),
            unique_key: vec![],
            exclude_columns: vec![],
        }
    }

    fn fact_source() -> SourceConfig {
        SourceConfig {
            name: "facts".to_string(),
            description: None,
            files: vec![],
            fields: HashMap::new(),
            derived: HashMap::new(),
            target: None,
            unique_key: vec![],
            exclude_columns: vec![],
        }
    }

    #[test]
    fn dim_source_has_target() {
        assert!(dim_source().target.is_some());
    }

    #[test]
    fn fact_source_has_no_target() {
        assert!(fact_source().target.is_none());
    }
}
```

- [ ] **Step 2: Run tests to confirm they pass**

```bash
cargo test -p state-search-ingest main 2>&1
```

- [ ] **Step 3: Update `ingest/src/main.rs`**

Replace the `Command::Reload` and `Command::Run` match arms:

```rust
Command::Reload => {
    let sources = &cfg.ingest.sources;
    if sources.is_empty() {
        info!("no sources configured, nothing to do");
        return Ok(());
    }
    let pipeline = pipeline::IngestPipeline::new(&pool);
    for source in sources {
        for file in &source.files {
            let skip = if source.target.is_some() {
                pipeline.dim_already_ingested(&source.name, file).await?
            } else {
                pipeline.already_ingested(file).await?
            };
            if skip {
                info!(source = source.name, file, "skipping (already ingested)");
                continue;
            }
            let count = if source.target.is_some() {
                pipeline.run_dim(source, file).await?
            } else {
                pipeline.run(source, file).await?
            };
            if count > 0 {
                println!("{}: {} rows inserted from {}", source.name, count, file);
            }
        }
    }
}

Command::Run { source: source_name, file } => {
    let source = cfg
        .ingest
        .sources
        .iter()
        .find(|s| s.name == source_name)
        .ok_or_else(|| anyhow::anyhow!(
            "source '{}' not found in config/sources.yml",
            source_name
        ))?;

    let pipeline = pipeline::IngestPipeline::new(&pool);

    match file {
        Some(path) => {
            // --file provided: always process, no skip check (both fact and dim)
            let count = if source.target.is_some() {
                pipeline.run_dim(source, &path).await?
            } else {
                pipeline.run(source, &path).await?
            };
            println!("inserted {count} rows");
        }
        None => {
            if source.files.is_empty() {
                info!(source = source_name, "source has no files configured, nothing to do");
                return Ok(());
            }
            for file in &source.files {
                let skip = if source.target.is_some() {
                    pipeline.dim_already_ingested(&source.name, file).await?
                } else {
                    pipeline.already_ingested(file).await?
                };
                if skip {
                    info!(source = source_name, file, "skipping (already ingested)");
                    continue;
                }
                let count = if source.target.is_some() {
                    pipeline.run_dim(source, file).await?
                } else {
                    pipeline.run(source, file).await?
                };
                if count > 0 {
                    println!("{}: {} rows inserted from {}", source.name, count, file);
                }
            }
        }
    }
}
```

- [ ] **Step 4: Final build**

```bash
cargo build 2>&1
```

Expected: clean build.

- [ ] **Step 5: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add ingest/src/main.rs
git commit -m "feat(ingest/main): dispatch Reload and Run by source.target (dim vs fact path)"
```

---

### Task 13: Update `config/sources.yml`

**Files:**
- Modify: `config/sources.yml`

Add `unique_key` and optionally `exclude_columns` to the existing `co_public_drinking_water` source. This makes its `normalized_imports.id` deterministic across re-runs.

- [ ] **Step 1: Update sources.yml**

Open `config/sources.yml` and add `unique_key` to the existing source. Good candidates for a stable natural key are fields that uniquely identify a row (year + quarter + pws_id_number + analyte_name):

```yaml
sources:
  - name: co_public_drinking_water
    unique_key:
      - location_id
      - time_id
    # exclude_columns: []  # add any columns to drop here
    description: Colorado public drinking water quality
    files:
      - data/usa/co/public_drinking_water/public_drinking_water_2026-03-14.csv
    fields:
      # ... existing fields unchanged ...
```

- [ ] **Step 2: Verify config parses**

```bash
cargo run -p state-search-ingest -- run --source co_public_drinking_water 2>&1 | head -5
```

Expected: starts up without config parse errors (may fail on missing file — that's fine).

- [ ] **Step 3: Commit**

```bash
git add config/sources.yml
git commit -m "config: add unique_key to co_public_drinking_water source"
```

---

## Final verification

- [ ] **Clean build**

```bash
cargo build 2>&1
```

Expected: zero warnings about unused variables or type errors.

- [ ] **All tests pass**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Migration is present**

```bash
ls migrations/
```

Expected: `006_uuid_pks.sql` listed alongside 001–005.
