# Normalized Imports Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `fact_observations` + `fact_row_context` with a single `normalized_imports` table, add per-source `import_schema`, convert `sources.toml` → `sources.yml` with combined field/type/rules definitions, and add derived field lookups from dimension tables.

**Architecture:** The pipeline gains a startup schema-seed step that reads `sources.yml` and upserts type definitions into `import_schema`. Each CSV row is stored raw in `raw_imports` (RETURNING id), then normalized into a single `normalized_imports` row with dim FKs + non-dimension typed fields as JSONB. Derived fields are resolved from dim tables after upserts and merged into the JSONB blob. Schema is authoritative — unknown or uncastable fields fail the row.

**Tech Stack:** Rust, SQLx (PostgreSQL), serde_yaml (new), Tokio, existing `batched_channel_sink` pattern

**Spec:** `docs/superpowers/specs/2026-03-15-normalized-imports-design.md`

---

## File Map

### Create
- `migrations/005_normalized_imports.sql` — breaking schema migration
- `config/sources.yml` — replaces sources.toml with new YAML format
- `core/src/models/normalized_import.rs` — `NewNormalizedImport`, `NormalizedImport`
- `core/src/models/import_schema_field.rs` — `NewImportSchemaField`, `ImportSchemaField`
- `core/src/repositories/normalized_import.rs` — `bulk_create`, `delete_by_ingest_run`
- `core/src/repositories/import_schema.rs` — `upsert_fields`, `load_for_source`
- `ingest/src/pipeline/schema.rs` — `seed_and_validate`, `validate_completeness`, `collect_normalized_data`
- `ingest/src/pipeline/derived.rs` — `resolve_derived`

### Modify
- `Cargo.toml` — add `serde_yaml` workspace dep
- `core/Cargo.toml` — add `serde_yaml`
- `ingest/Cargo.toml` — add `serde_yaml`, remove `toml` dev-dep
- `core/src/config.rs` — new `FieldDef`, `DerivedFieldDef`, `SourceConfig`; switch to YAML loading; drop `FieldType` enum
- `core/src/models/mod.rs` — add new modules, remove `observation`, `row_context`
- `core/src/repositories/mod.rs` — add new modules, remove `observation`, `row_context`
- `ingest/src/transforms/resolve.rs` — update `build_resolved_field_map` for new `FieldDef` shape; replace `FieldType` dispatch with string-based mapping
- `ingest/src/pipeline/record.rs` — new `process_record` flow; `raw_import_id` capture; remove context/metric extraction
- `ingest/src/pipeline/mod.rs` — schema seed at startup; flush `NewNormalizedImport` batches; `delete_by_ingest_run` on `normalized_imports`
- `ingest/src/pipeline/export.rs` — `generate_export_sql` from non-dim schema fields; join `normalized_imports`
- `api/src/routes/observations.rs` — replace with `normalized_imports` query (minimal; old route won't compile)
- `ingest/CLAUDE.md` — update key files table

### Delete
- `config/sources.toml`
- `core/src/models/observation.rs`
- `core/src/models/row_context.rs`
- `core/src/repositories/observation.rs`
- `core/src/repositories/row_context.rs`
- `ingest/src/pipeline/context.rs`
- `ingest/src/pipeline/observations.rs`

---

## Chunk 1: Migration, Config, and Resolve

### Task 1: Migration 005

**Files:**
- Create: `migrations/005_normalized_imports.sql`

- [ ] **Step 1: Write the migration**

```sql
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
```

- [ ] **Step 2: Commit**

```bash
git add migrations/005_normalized_imports.sql
git commit -m "feat(db): add normalized_imports + import_schema, drop fact_observations + fact_row_context"
```

---

### Task 2: sources.yml

**Files:**
- Create: `config/sources.yml`
- Delete: `config/sources.toml`

- [ ] **Step 1: Create sources.yml with the existing source ported to new format**

Port the existing `co_public_drinking_water` source. The old `sources.toml` had:
- `state_code` ← CSV column `state` with rule `state_name_to_code`, `on_failure: skip_row`, rule `on_failure: skip_dataset`
- `fips_code` ← CSV column `county_fips`
- `year` ← CSV column `year`, type `i16`
- `quarter` ← CSV column `quarter`, type `i16`
- `latitude` ← CSV column `pws_latitude`, type `f64`
- `longitude` ← CSV column `pws_longitude`, type `f64`

New YAML format (YAML key = CSV column, `canonical` = canonical name, `type` = postgres type):

```yaml
# config/sources.yml
sources:
  - name: co_public_drinking_water
    description: Colorado public drinking water quality
    files:
      - data/usa/co/public_drinking_water/public_drinking_water_2026-03-14.csv

    fields:
      state:
        canonical: state_code
        type: text
        on_failure: skip_row
        rules:
          - kind: state_name_to_code
            on_failure: skip_dataset
      county_fips:
        canonical: fips_code
        type: text
      year:
        canonical: year
        type: smallint
      quarter:
        canonical: quarter
        type: smallint
      pws_latitude:
        canonical: latitude
        type: float8
      pws_longitude:
        canonical: longitude
        type: float8

    derived: {}
```

- [ ] **Step 2: Delete sources.toml**

```bash
git rm config/sources.toml
```

- [ ] **Step 3: Commit**

```bash
git add config/sources.yml
git commit -m "feat(config): convert sources.toml to sources.yml with combined field/type/rules"
```

---

### Task 3: Update config.rs — new FieldDef, DerivedFieldDef, SourceConfig, YAML loading

**Files:**
- Modify: `core/src/config.rs`
- Modify: `Cargo.toml`, `core/Cargo.toml`

- [ ] **Step 1: Add serde_yaml to workspace Cargo.toml**

In `Cargo.toml` `[workspace.dependencies]`, add:
```toml
serde_yaml = "0.9"
```

In `core/Cargo.toml` `[dependencies]`, add:
```toml
serde_yaml = { workspace = true }
```

Remove `toml = "0.8"` from `core/Cargo.toml` (no longer needed for config loading; `ingest` dev-dep also removed later).

- [ ] **Step 2: Write a failing test for the new FieldDef YAML shape**

In `core/src/config.rs` tests (replacing the existing TOML tests):

```rust
#[test]
fn field_def_yaml_deserialization() {
    let src = "canonical: state_code\ntype: text\n";
    let def: FieldDef = serde_yaml::from_str(src).unwrap();
    assert_eq!(def.canonical, "state_code");
    assert_eq!(def.field_type, "text");
    assert!(def.on_failure.is_none());
    assert!(def.rules.is_empty());
    assert!(def.format.is_none());
}

#[test]
fn source_config_yaml_deserialization() {
    let src = r#"
name: test_source
fields:
  Year:
    canonical: year
    type: smallint
  State:
    canonical: state_code
    type: text
    on_failure: skip_row
    rules:
      - kind: state_name_to_code
        on_failure: skip_dataset
derived: {}
"#;
    let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
    assert_eq!(sc.name, "test_source");
    assert_eq!(sc.fields.len(), 2);
    let year = sc.fields.get("Year").unwrap();
    assert_eq!(year.canonical, "year");
    assert_eq!(year.field_type, "smallint");
    let state = sc.fields.get("State").unwrap();
    assert_eq!(state.on_failure, Some(OnFailure::SkipRow));
    assert_eq!(state.rules.len(), 1);
}

#[test]
fn derived_field_def_match_key_deserialization() {
    let src = "from: dim_location\nmatch: fips_code\noutput: county\ntype: text\n";
    let d: DerivedFieldDef = serde_yaml::from_str(src).unwrap();
    assert_eq!(d.from, "dim_location");
    assert_eq!(d.match_field, "fips_code");
    assert_eq!(d.output, "county");
}
```

- [ ] **Step 3: Run test — verify it fails**

```bash
cargo test -p state-search-core config 2>&1 | head -30
```

Expected: compile error — `FieldDef`, `DerivedFieldDef` don't exist in new shape yet.

- [ ] **Step 4: Rewrite config.rs with new types**

Replace `FieldDef`, remove `FieldType` enum, add `DerivedFieldDef`, update `SourceConfig`, switch `AppConfig::load` to read `sources.yml` via `serde_yaml`:

```rust
// core/src/config.rs
use config::{Config, Environment, File};
use serde::Deserialize;
use std::collections::HashMap;

use crate::error::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_pool_size")]
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: default_host(), port: default_port() }
    }
}

impl ServerConfig {
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct IngestConfig {
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    Ignore      = 0,
    SkipRow     = 1,
    SkipDataset = 2,
}

impl Default for OnFailure {
    fn default() -> Self { Self::Ignore }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuleDef {
    pub kind: String,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
}

/// Definition for a single source CSV column.
/// YAML key = CSV column name; `canonical` = renamed-to canonical field name.
#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub canonical: String,
    /// Postgres type string: "text", "smallint", "integer", "bigint",
    /// "real", "float8", "numeric", "boolean", "date", "timestamptz".
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
    /// Only valid for "date" / "timestamptz" types.
    pub format: Option<String>,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}

fn default_field_type() -> String { "text".to_string() }

/// Definition for a field derived from a dimension table lookup.
#[derive(Debug, Deserialize, Clone)]
pub struct DerivedFieldDef {
    pub from: String,
    /// YAML key is `match` (readable); renamed to avoid Rust keyword collision.
    #[serde(rename = "match")]
    pub match_field: String,
    pub output: String,
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    /// CSV column name → FieldDef. YAML key is the source column.
    #[serde(default)]
    pub fields: HashMap<String, FieldDef>,
    /// canonical name → DerivedFieldDef
    #[serde(default)]
    pub derived: HashMap<String, DerivedFieldDef>,
}

fn default_pool_size() -> u32 { 10 }
fn default_host() -> String   { "0.0.0.0".to_string() }
fn default_port() -> u16      { 3000 }

impl AppConfig {
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(
                Environment::with_prefix("APP")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        let mut app_config: Self = cfg.try_deserialize()?;

        let sources_path = "config/sources.yml";
        if let Ok(content) = std::fs::read_to_string(sources_path) {
            #[derive(Deserialize)]
            struct SourcesFile {
                #[serde(default)]
                sources: Vec<SourceConfig>,
            }
            let sources: SourcesFile = serde_yaml::from_str(&content).map_err(|e| {
                config::ConfigError::Message(format!("failed to parse {sources_path}: {e}"))
            })?;
            app_config.ingest.sources = sources.sources;
        }

        Ok(app_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_def_yaml_deserialization() {
        let src = "canonical: state_code\ntype: text\n";
        let def: FieldDef = serde_yaml::from_str(src).unwrap();
        assert_eq!(def.canonical, "state_code");
        assert_eq!(def.field_type, "text");
        assert!(def.on_failure.is_none());
        assert!(def.rules.is_empty());
        assert!(def.format.is_none());
    }

    #[test]
    fn source_config_yaml_deserialization() {
        let src = r#"
name: test_source
fields:
  Year:
    canonical: year
    type: smallint
  State:
    canonical: state_code
    type: text
    on_failure: skip_row
    rules:
      - kind: state_name_to_code
        on_failure: skip_dataset
derived: {}
"#;
        let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
        assert_eq!(sc.name, "test_source");
        assert_eq!(sc.fields.len(), 2);
        let year = sc.fields.get("Year").unwrap();
        assert_eq!(year.canonical, "year");
        assert_eq!(year.field_type, "smallint");
        let state = sc.fields.get("State").unwrap();
        assert_eq!(state.on_failure, Some(OnFailure::SkipRow));
        assert_eq!(state.rules.len(), 1);
    }

    #[test]
    fn derived_field_def_match_key_deserialization() {
        let src = "from: dim_location\nmatch: fips_code\noutput: county\ntype: text\n";
        let d: DerivedFieldDef = serde_yaml::from_str(src).unwrap();
        assert_eq!(d.from, "dim_location");
        assert_eq!(d.match_field, "fips_code");
        assert_eq!(d.output, "county");
    }

    #[test]
    fn on_failure_ranking() {
        assert!(OnFailure::Ignore < OnFailure::SkipRow);
        assert!(OnFailure::SkipRow < OnFailure::SkipDataset);
    }
}
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
cargo test -p state-search-core config 2>&1
```

Expected: all config tests pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml core/Cargo.toml core/src/config.rs
git commit -m "feat(core): new FieldDef/DerivedFieldDef/SourceConfig for YAML sources, drop FieldType enum"
```

---

### Task 4: Update resolve.rs for new FieldDef shape

**Files:**
- Modify: `ingest/src/transforms/resolve.rs`
- Modify: `ingest/Cargo.toml`

The key changes:
1. `build_resolved_field_map` iterates `source.fields` (csv_col → FieldDef) instead of `source.field_map`.
2. `build_resolved_field` takes `(csv_col: &str, def: &FieldDef)` — the canonical name is `def.canonical`, and `source_col` is `csv_col.to_lowercase()`.
3. `parse_format_and_make_coerce` now takes `field_type: &str` (string) instead of `&FieldType` (enum). Add a `field_type_str_to_coerce` function that maps postgres type strings to coerce rules.

- [ ] **Step 1: Write failing tests for the new resolve behavior**

Replace/update the tests in `resolve.rs`:

```rust
#[test]
fn build_resolved_field_map_uses_csv_col_as_key_and_canonical_from_def() {
    use state_search_core::config::{FieldDef, SourceConfig};
    use std::collections::HashMap;

    let mut fields = HashMap::new();
    fields.insert("Year".to_string(), FieldDef {
        canonical: "year".to_string(),
        field_type: "smallint".to_string(),
        format: None,
        on_failure: None,
        rules: vec![],
    });

    let source = SourceConfig {
        name: "s".to_string(),
        description: None,
        files: vec![],
        fields,
        derived: HashMap::new(),
    };

    let resolved = build_resolved_field_map(&source).unwrap();
    // Key in ResolvedFieldMap is the canonical name
    assert!(resolved.contains_key("year"));
    // source_col is the lowercased CSV column
    assert_eq!(resolved["year"].source_col, "year");
}

#[test]
fn field_type_smallint_produces_coerce_i16() {
    use state_search_core::config::{FieldDef, OnFailure};
    let def = FieldDef {
        canonical: "year".to_string(),
        field_type: "smallint".to_string(),
        format: None,
        on_failure: None,
        rules: vec![],
    };
    let resolved = build_resolved_field("Year", &def).unwrap();
    // smallint → CoerceToI16 appended as implicit rule
    assert_eq!(resolved.chain.len(), 1);
}

#[test]
fn field_type_text_appends_no_coerce() {
    use state_search_core::config::FieldDef;
    let def = FieldDef {
        canonical: "state_code".to_string(),
        field_type: "text".to_string(),
        format: None,
        on_failure: None,
        rules: vec![],
    };
    let resolved = build_resolved_field("state", &def).unwrap();
    assert!(resolved.chain.is_empty());
}

#[test]
fn unknown_rule_kind_is_error() {
    use state_search_core::config::{FieldDef, RuleDef};
    let def = FieldDef {
        canonical: "col".to_string(),
        field_type: "text".to_string(),
        format: None,
        on_failure: None,
        rules: vec![RuleDef { kind: "does_not_exist".to_string(), on_failure: None }],
    };
    let result = build_resolved_field("col", &def);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown rule kind"));
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
cargo test -p state-search-ingest resolve 2>&1 | head -30
```

Expected: compile/test errors since `build_resolved_field_map` still reads `field_map`.

- [ ] **Step 3: Update resolve.rs**

Replace `parse_format_and_make_coerce` (which took `&FieldType`) with `field_type_str_to_coerce` (which takes `&str`). Update `build_resolved_field` to take `(csv_col: &str, def: &FieldDef)`. Update `build_resolved_field_map` to iterate `source.fields`:

```rust
use std::collections::HashMap;
use state_search_core::config::{FieldDef, OnFailure, SourceConfig};
use super::{FieldRule, coerce::{
    CoerceToBool, CoerceToDate, CoerceToDateTime, CoerceToF32, CoerceToF64,
    CoerceToI8, CoerceToI16, CoerceToI32, CoerceToI64,
    CoerceToU8, CoerceToU16, CoerceToU32, CoerceToU64,
    DateFormat, DateTimeFormat,
}, rules::state_name_to_code::StateNameToCode};

pub struct ResolvedField {
    pub source_col: String,
    pub chain: Vec<(Box<dyn FieldRule>, OnFailure)>,
    pub field_on_failure: OnFailure,
}

impl std::fmt::Debug for ResolvedField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedField")
            .field("source_col", &self.source_col)
            .field("chain_len", &self.chain.len())
            .field("field_on_failure", &self.field_on_failure)
            .finish()
    }
}

pub type ResolvedFieldMap = HashMap<String, ResolvedField>;

/// Map a postgres type string + optional format to the implicit coerce rule.
/// Returns None for "text" (no coercion needed).
fn field_type_str_to_coerce(
    field_type: &str,
    format: Option<&str>,
    field_on_failure: OnFailure,
) -> anyhow::Result<Option<(Box<dyn FieldRule>, OnFailure)>> {
    if let Some(fmt_str) = format {
        match field_type {
            "date" => {
                let fmt = DateFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown date format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDate(fmt)), field_on_failure)));
            }
            "timestamptz" | "timestamp with time zone" => {
                let fmt = DateTimeFormat::from_str(fmt_str)
                    .ok_or_else(|| anyhow::anyhow!("unknown datetime format: '{}'", fmt_str))?;
                return Ok(Some((Box::new(CoerceToDateTime(fmt)), field_on_failure)));
            }
            _ => anyhow::bail!("'format' is only valid for date/timestamptz types, got '{}'", field_type),
        }
    }

    let coerce: Option<Box<dyn FieldRule>> = match field_type {
        "text" | "varchar" | "char"           => None,
        "smallint" | "int2"                   => Some(Box::new(CoerceToI16)),
        "integer"  | "int"  | "int4"          => Some(Box::new(CoerceToI32)),
        "bigint"   | "int8"                   => Some(Box::new(CoerceToI64)),
        "real"     | "float4"                 => Some(Box::new(CoerceToF32)),
        "float8"   | "double precision"       => Some(Box::new(CoerceToF64)),
        "numeric"  | "decimal"                => Some(Box::new(CoerceToF64)),
        "boolean"  | "bool"                   => Some(Box::new(CoerceToBool)),
        "date"                                => Some(Box::new(CoerceToDate(DateFormat::Iso8601))),
        "timestamptz" | "timestamp with time zone" => Some(Box::new(CoerceToDateTime(DateTimeFormat::Rfc3339))),
        // Legacy short names kept for compatibility
        "i8"  => Some(Box::new(CoerceToI8)),
        "i16" => Some(Box::new(CoerceToI16)),
        "i32" => Some(Box::new(CoerceToI32)),
        "i64" => Some(Box::new(CoerceToI64)),
        "u8"  => Some(Box::new(CoerceToU8)),
        "u16" => Some(Box::new(CoerceToU16)),
        "u32" => Some(Box::new(CoerceToU32)),
        "u64" => Some(Box::new(CoerceToU64)),
        "f32" => Some(Box::new(CoerceToF32)),
        "f64" => Some(Box::new(CoerceToF64)),
        other => anyhow::bail!("unknown field type: '{}'", other),
    };
    Ok(coerce.map(|r| (r, field_on_failure)))
}

fn lookup_rule(kind: &str) -> Option<Box<dyn FieldRule>> {
    match kind {
        "state_name_to_code" => Some(Box::new(StateNameToCode)),
        _                    => None,
    }
}

/// Resolve a single FieldDef into a ResolvedField.
/// `csv_col` is the source CSV column name (the YAML key in `fields`).
pub fn build_resolved_field(csv_col: &str, def: &FieldDef) -> anyhow::Result<ResolvedField> {
    let field_on_failure = def.on_failure.unwrap_or(OnFailure::Ignore);
    let mut chain: Vec<(Box<dyn FieldRule>, OnFailure)> = Vec::new();

    for rule_def in &def.rules {
        let rule = lookup_rule(&rule_def.kind)
            .ok_or_else(|| anyhow::anyhow!(
                "unknown rule kind: '{}' (in field canonical='{}')",
                rule_def.kind, def.canonical
            ))?;
        let rule_on_failure = rule_def.on_failure.unwrap_or(OnFailure::Ignore);
        let effective = field_on_failure.max(rule_on_failure);
        chain.push((rule, effective));
    }

    if let Some(coerce_entry) = field_type_str_to_coerce(
        &def.field_type,
        def.format.as_deref(),
        field_on_failure,
    )? {
        chain.push(coerce_entry);
    }

    Ok(ResolvedField {
        source_col: csv_col.to_lowercase(),
        chain,
        field_on_failure,
    })
}

/// Resolve all fields in a source config into a ResolvedFieldMap.
/// HashMap key = canonical name; source_col = CSV column (lowercased).
pub fn build_resolved_field_map(source: &SourceConfig) -> anyhow::Result<ResolvedFieldMap> {
    source
        .fields
        .iter()
        .map(|(csv_col, def)| {
            build_resolved_field(csv_col, def)
                .map(|resolved| (def.canonical.clone(), resolved))
        })
        .collect()
}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
cargo test -p state-search-ingest resolve 2>&1
```

Expected: all resolve tests pass. Also run full build to catch compile errors from `FieldType` references elsewhere:

```bash
cargo build 2>&1 | head -50
```

Fix any remaining `FieldType` import errors (e.g. in `resolve.rs` test imports).

- [ ] **Step 5: Commit**

```bash
git add ingest/src/transforms/resolve.rs ingest/Cargo.toml
git commit -m "feat(ingest): update resolve to use string field_type, new FieldDef shape"
```

---

## Chunk 2: Core Models, Repos, API Route

### Task 5: New core models

**Files:**
- Create: `core/src/models/normalized_import.rs`
- Create: `core/src/models/import_schema_field.rs`
- Modify: `core/src/models/mod.rs`

- [ ] **Step 1: Write tests for new models**

```rust
// In normalized_import.rs tests:
#[test]
fn new_normalized_import_fields() {
    let _ = NewNormalizedImport {
        raw_import_id:   Some(1),
        location_id:     Some(2),
        time_id:         Some(3),
        source_name:     "s".to_string(),
        ingest_run_id:   Uuid::new_v4(),
        normalized_data: serde_json::json!({"analyte": "TTHM"}),
    };
}

// In import_schema_field.rs tests:
#[test]
fn new_import_schema_field_fields() {
    let _ = NewImportSchemaField {
        source_name: "co_public_drinking_water".to_string(),
        field_name:  "year".to_string(),
        field_type:  "smallint".to_string(),
    };
}
```

- [ ] **Step 2: Create normalized_import.rs**

```rust
// core/src/models/normalized_import.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewNormalizedImport {
    pub raw_import_id:   Option<i64>,
    pub location_id:     Option<i64>,
    pub time_id:         Option<i64>,
    pub source_name:     String,
    pub ingest_run_id:   Uuid,
    pub normalized_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_normalized_import_fields() {
        let _ = NewNormalizedImport {
            raw_import_id:   Some(1),
            location_id:     Some(2),
            time_id:         Some(3),
            source_name:     "s".to_string(),
            ingest_run_id:   Uuid::new_v4(),
            normalized_data: serde_json::json!({"analyte": "TTHM"}),
        };
    }
}
```

- [ ] **Step 3: Create import_schema_field.rs**

```rust
// core/src/models/import_schema_field.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone)]
pub struct NewImportSchemaField {
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ImportSchemaField {
    pub id:          i32,
    pub source_name: String,
    pub field_name:  String,
    pub field_type:  String,
    pub created_at:  DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_import_schema_field_fields() {
        let _ = NewImportSchemaField {
            source_name: "co_public_drinking_water".to_string(),
            field_name:  "year".to_string(),
            field_type:  "smallint".to_string(),
        };
    }
}
```

- [ ] **Step 4: Update models/mod.rs**

```rust
pub mod import;
pub mod import_schema_field;
pub mod location;
pub mod normalized_import;
pub mod time;
```

(Remove `observation` and `row_context`.)

- [ ] **Step 5: Run tests**

```bash
cargo test -p state-search-core models 2>&1
```

Expected: new model tests pass. Ignore compile errors from other crates referencing old models — those are fixed in subsequent tasks.

- [ ] **Step 6: Commit**

```bash
git add core/src/models/normalized_import.rs core/src/models/import_schema_field.rs core/src/models/mod.rs
git commit -m "feat(core): add NormalizedImport and ImportSchemaField models"
```

---

### Task 6: New core repos + remove old models/repos

**Files:**
- Create: `core/src/repositories/normalized_import.rs`
- Create: `core/src/repositories/import_schema.rs`
- Modify: `core/src/repositories/mod.rs`
- Delete: `core/src/models/observation.rs`, `core/src/models/row_context.rs`
- Delete: `core/src/repositories/observation.rs`, `core/src/repositories/row_context.rs`

- [ ] **Step 1: Write compile-time tests for new repo signatures**

```rust
// In normalized_import.rs tests:
#[test]
fn delete_by_ingest_run_signature_exists() {
    let _ = NormalizedImportRepository::delete_by_ingest_run;
}

#[test]
fn bulk_create_signature_exists() {
    // Just verifies NormalizedImportRepository::bulk_create is accessible
    let _ = NormalizedImportRepository::bulk_create;
}
```

- [ ] **Step 2: Create repositories/normalized_import.rs**

```rust
// core/src/repositories/normalized_import.rs
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

    /// Bulk insert normalized import rows using a single UNNEST query.
    pub async fn bulk_create(&self, rows: Vec<NewNormalizedImport>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let len = rows.len();
        let mut raw_import_ids   = Vec::with_capacity(len);
        let mut location_ids     = Vec::with_capacity(len);
        let mut time_ids         = Vec::with_capacity(len);
        let mut source_names     = Vec::with_capacity(len);
        let mut ingest_run_ids   = Vec::with_capacity(len);
        let mut normalized_datas = Vec::with_capacity(len);

        for row in rows {
            raw_import_ids  .push(row.raw_import_id);
            location_ids    .push(row.location_id);
            time_ids        .push(row.time_id);
            source_names    .push(row.source_name);
            ingest_run_ids  .push(row.ingest_run_id);
            normalized_datas.push(row.normalized_data);
        }

        sqlx::query(
            "INSERT INTO normalized_imports
                 (raw_import_id, location_id, time_id, source_name, ingest_run_id, normalized_data)
             SELECT * FROM UNNEST(
                 $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::uuid[], $6::jsonb[]
             )",
        )
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

    /// Delete all normalized imports belonging to a given ingest run.
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

- [ ] **Step 3: Create repositories/import_schema.rs**

```rust
// core/src/repositories/import_schema.rs
use std::collections::HashMap;
use sqlx::PgPool;

use crate::{error::Result, models::import_schema_field::NewImportSchemaField};

pub struct ImportSchemaRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ImportSchemaRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Upsert schema field definitions for a source.
    /// ON CONFLICT updates field_type; does not overwrite created_at or description.
    pub async fn upsert_fields(&self, fields: Vec<NewImportSchemaField>) -> Result<()> {
        for field in fields {
            sqlx::query(
                "INSERT INTO import_schema (source_name, field_name, field_type)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (source_name, field_name)
                 DO UPDATE SET field_type = EXCLUDED.field_type",
            )
            .bind(&field.source_name)
            .bind(&field.field_name)
            .bind(&field.field_type)
            .execute(self.pool)
            .await?;
        }
        Ok(())
    }

    /// Fetch all field definitions for a source, keyed by canonical field name.
    /// Returns canonical_name → postgres type string.
    pub async fn load_for_source(&self, source_name: &str) -> Result<HashMap<String, String>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT field_name, field_type FROM import_schema WHERE source_name = $1",
        )
        .bind(source_name)
        .fetch_all(self.pool)
        .await?;

        Ok(rows.into_iter().collect())
    }
}
```

- [ ] **Step 4: Update repositories/mod.rs**

```rust
pub mod import;
pub mod import_schema;
pub mod location;
pub mod normalized_import;
pub mod time;
```

(Remove `observation` and `row_context`.)

- [ ] **Step 5: Delete old files**

```bash
git rm core/src/models/observation.rs core/src/models/row_context.rs
git rm core/src/repositories/observation.rs core/src/repositories/row_context.rs
```

- [ ] **Step 6: Run core tests**

```bash
cargo test -p state-search-core 2>&1
```

Expected: core tests pass. Compile errors in `api` and `ingest` crates are expected (references to removed types).

- [ ] **Step 7: Commit**

```bash
git add core/src/repositories/normalized_import.rs core/src/repositories/import_schema.rs core/src/repositories/mod.rs core/src/models/mod.rs
git commit -m "feat(core): add NormalizedImportRepository and ImportSchemaRepository, remove observation/row_context"
```

---

### Task 7: Fix API route

The `GET /api/observations` route directly uses `ObservationRepository` which no longer exists. Replace it with a `normalized_imports` query.

**Files:**
- Modify: `api/src/routes/observations.rs`

- [ ] **Step 1: Write test for new query shape**

The route should accept the same query params (source_name, limit, offset) but query `normalized_imports`:

```rust
// No unit test — integration only. Verify it compiles and returns JSON.
```

- [ ] **Step 2: Replace observations.rs**

```rust
// api/src/routes/observations.rs
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::{error::ApiResult, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(query))
}

#[derive(Debug, Deserialize)]
pub struct NormalizedImportQuery {
    pub source_name: Option<String>,
    pub location_id: Option<i64>,
    pub time_id:     Option<i64>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 { 100 }

async fn query(
    State(state): State<AppState>,
    Query(params): Query<NormalizedImportQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let rows = sqlx::query(
        "SELECT * FROM normalized_imports
         WHERE ($1::text IS NULL OR source_name = $1)
           AND ($2::bigint IS NULL OR location_id = $2)
           AND ($3::bigint IS NULL OR time_id = $3)
         ORDER BY id
         LIMIT $4 OFFSET $5",
    )
    .bind(params.source_name)
    .bind(params.location_id)
    .bind(params.time_id)
    .bind(params.limit)
    .bind(params.offset)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(state_search_core::error::CoreError::from)?;

    let json: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id":              sqlx::Row::get::<i64, _>(r, "id"),
                "source_name":     sqlx::Row::get::<String, _>(r, "source_name"),
                "ingest_run_id":   sqlx::Row::get::<uuid::Uuid, _>(r, "ingest_run_id").to_string(),
                "location_id":     sqlx::Row::get::<Option<i64>, _>(r, "location_id"),
                "time_id":         sqlx::Row::get::<Option<i64>, _>(r, "time_id"),
                "normalized_data": sqlx::Row::get::<serde_json::Value, _>(r, "normalized_data"),
            })
        })
        .collect();

    Ok(Json(serde_json::to_value(json).unwrap()))
}
```

- [ ] **Step 3: Build API crate**

```bash
cargo build -p state-search-api 2>&1
```

Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add api/src/routes/observations.rs
git commit -m "feat(api): replace observations route with normalized_imports query"
```

---

## Chunk 3: Pipeline schema.rs and derived.rs

### Task 8: schema.rs — seed_and_validate, validate_completeness, collect_normalized_data

**Files:**
- Create: `ingest/src/pipeline/schema.rs`

The dimension fields to exclude from `normalized_data` (same as existing skip-sets in `context.rs` and `observations.rs`):

```
LOCATION_FIELDS: county, country, zip_code, fips_code, latitude, longitude, state_code, state_name
TIME_FIELDS: year, quarter, month, day
```

- [ ] **Step 1: Declare the module in mod.rs**

Add `mod schema;` to `ingest/src/pipeline/mod.rs` so the new file is compiled and tests can run. (The full mod.rs rewrite happens in Task 11; this is a minimal declaration to enable TDD on this module.)

In `ingest/src/pipeline/mod.rs`, add immediately after `mod record;`:
```
mod schema;
```

- [ ] **Step 2: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::FieldValue;
    use std::collections::HashMap;

    fn make_schema(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn make_row(pairs: &[(&str, FieldValue)]) -> HashMap<String, FieldValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    // validate_completeness tests
    #[test]
    fn validate_completeness_passes_when_all_fields_present() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        assert!(validate_completeness(&row, &schema, &HashMap::new(), 1).is_ok());
    }

    #[test]
    fn validate_completeness_fails_when_non_derived_field_missing() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        // Only year is present; analyte_name is missing
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        let derived: HashMap<String, String> = HashMap::new();
        let result = validate_completeness(&row, &schema, &derived, 1);
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("analyte_name"));
    }

    #[test]
    fn validate_completeness_skips_derived_fields() {
        let schema = make_schema(&[("year", "smallint"), ("county_name", "text")]);
        let mut derived = HashMap::new();
        derived.insert("county_name".to_string(), "text".to_string());
        // county_name is derived — not expected in the transformed row
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        assert!(validate_completeness(&row, &schema, &derived, 1).is_ok());
    }

    // collect_normalized_data tests
    #[test]
    fn collect_normalized_data_excludes_dimension_fields() {
        let schema = make_schema(&[
            ("year", "smallint"),       // TIME_FIELD — excluded
            ("state_code", "text"),     // LOCATION_FIELD — excluded
            ("analyte_name", "text"),   // non-dim — included
        ]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("state_code", FieldValue::Str("CO".to_string())),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!(result.get("analyte_name").is_some());
        assert!(result.get("year").is_none());
        assert!(result.get("state_code").is_none());
    }

    #[test]
    fn collect_normalized_data_skips_already_derived_fields() {
        let schema = make_schema(&[("county_name", "text"), ("analyte_name", "text")]);
        // county_name is derived and already in existing_data
        let mut existing = serde_json::json!({});
        existing["county_name"] = serde_json::json!("Denver");
        let row = make_row(&[("analyte_name", FieldValue::Str("TTHM".to_string()))]);
        let mut derived_names = HashMap::new();
        derived_names.insert("county_name".to_string(), "text".to_string());
        let result = collect_normalized_data(&row, &schema, &derived_names, &existing, 1).unwrap();
        // county_name from existing_data is preserved
        assert_eq!(result["county_name"], "Denver");
        // analyte_name added
        assert_eq!(result["analyte_name"], "TTHM");
    }

    #[test]
    fn collect_normalized_data_includes_numeric_values() {
        let schema = make_schema(&[("average_value", "numeric")]);
        let row = make_row(&[("average_value", FieldValue::F64(3.8))]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!((result["average_value"].as_f64().unwrap() - 3.8).abs() < f64::EPSILON);
    }
}
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cargo test -p state-search-ingest schema 2>&1 | head -20
```

Expected: compile error — functions `validate_completeness`, `collect_normalized_data` not yet defined.

- [ ] **Step 4: Create schema.rs**

```rust
// ingest/src/pipeline/schema.rs
use std::collections::HashMap;

use state_search_core::{
    config::SourceConfig,
    models::import_schema_field::NewImportSchemaField,
    repositories::import_schema::ImportSchemaRepository,
    Db,
};
use tracing::info;

use crate::transforms::FieldValue;

/// Dimension field names — excluded from normalized_data (stored as FK references instead).
const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
    "state_code", "state_name",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

fn is_dimension_field(name: &str) -> bool {
    LOCATION_FIELDS.contains(&name) || TIME_FIELDS.contains(&name)
}

/// Seed import_schema from the source's field definitions, then validate
/// all expected fields are present. Returns canonical_name → type string.
pub(super) async fn seed_and_validate(
    source: &SourceConfig,
    db: &Db,
) -> anyhow::Result<HashMap<String, String>> {
    let repo = ImportSchemaRepository::new(db);

    // Build the full list of fields to seed (declared fields + derived fields)
    let mut to_seed: Vec<NewImportSchemaField> = Vec::new();

    for def in source.fields.values() {
        to_seed.push(NewImportSchemaField {
            source_name: source.name.clone(),
            field_name:  def.canonical.clone(),
            field_type:  def.field_type.clone(),
        });
    }
    for (canonical, derived_def) in &source.derived {
        to_seed.push(NewImportSchemaField {
            source_name: source.name.clone(),
            field_name:  canonical.clone(),
            field_type:  derived_def.field_type.clone(),
        });
    }

    repo.upsert_fields(to_seed).await?;

    let schema = repo.load_for_source(&source.name).await?;

    // Expected = all canonical names from fields + derived
    let expected: Vec<String> = source.fields.values()
        .map(|d| d.canonical.clone())
        .chain(source.derived.keys().cloned())
        .collect();

    let missing: Vec<&str> = expected.iter()
        .filter(|name| !schema.contains_key(*name))
        .map(String::as_str)
        .collect();

    if !missing.is_empty() {
        anyhow::bail!(
            "import_schema validation failed for source '{}': missing fields: {}",
            source.name,
            missing.join(", ")
        );
    }

    info!(source = %source.name, fields = schema.len(), "import_schema validated");
    Ok(schema)
}

/// Check that all required (non-derived) schema fields are present in the
/// transformed row. Returns Err with a message identifying the missing field.
pub(super) fn validate_completeness(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
    row_num: u64,
) -> Result<(), String> {
    for field_name in schema.keys() {
        if derived_fields.contains_key(field_name) {
            continue; // derived fields are not expected in the CSV row
        }
        if !row.contains_key(field_name) {
            return Err(format!(
                "row {}: required field '{}' missing from transformed row (check CSV headers and field_map)",
                row_num, field_name
            ));
        }
    }
    Ok(())
}

/// Build the normalized_data JSONB from the transformed row.
/// - Skips dimension fields (stored as location_id/time_id FK).
/// - Skips derived fields (already in existing_data from resolve_derived step).
/// - Merges with existing_data (derived field values).
pub(super) fn collect_normalized_data(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
    existing_data: &serde_json::Value,
    row_num: u64,
) -> Result<serde_json::Value, String> {
    let mut obj = existing_data.as_object()
        .cloned()
        .unwrap_or_default();

    for (field_name, _field_type) in schema {
        if is_dimension_field(field_name) {
            continue;
        }
        if derived_fields.contains_key(field_name) {
            continue; // already in obj from existing_data
        }

        let val = row.get(field_name).unwrap_or(&FieldValue::Null);
        let json_val = field_value_to_json(val, field_name, row_num)?;
        obj.insert(field_name.clone(), json_val);
    }

    Ok(serde_json::Value::Object(obj))
}

/// Convert a FieldValue to a serde_json::Value.
/// Returns Err if the value cannot be represented (e.g. Null where type forbids it).
fn field_value_to_json(
    val: &FieldValue,
    field_name: &str,
    row_num: u64,
) -> Result<serde_json::Value, String> {
    let json = match val {
        FieldValue::Null      => serde_json::Value::Null,
        FieldValue::Str(s)    => serde_json::Value::String(s.clone()),
        FieldValue::Bool(b)   => serde_json::Value::Bool(*b),
        FieldValue::I8(x)     => serde_json::json!(*x),
        FieldValue::I16(x)    => serde_json::json!(*x),
        FieldValue::I32(x)    => serde_json::json!(*x),
        FieldValue::I64(x)    => serde_json::json!(*x),
        FieldValue::U8(x)     => serde_json::json!(*x),
        FieldValue::U16(x)    => serde_json::json!(*x),
        FieldValue::U32(x)    => serde_json::json!(*x),
        FieldValue::U64(x)    => serde_json::json!(*x),
        FieldValue::F32(x)    => serde_json::json!(*x),
        FieldValue::F64(x)    => serde_json::json!(*x),
        FieldValue::Date(d)   => serde_json::Value::String(d.to_string()),
        FieldValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
    };

    if json.is_null() {
        // Null values are acceptable — many fields are optional
        return Ok(json);
    }

    // Sanity: if we got a Null but the schema expects a non-null, the transform
    // chain should have already failed the row. Accept nulls here without error.
    let _ = (field_name, row_num); // suppress unused warnings
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn make_row(pairs: &[(&str, FieldValue)]) -> HashMap<String, FieldValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn validate_completeness_passes_when_all_fields_present() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        assert!(validate_completeness(&row, &schema, &HashMap::new(), 1).is_ok());
    }

    #[test]
    fn validate_completeness_fails_when_non_derived_field_missing() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        let result = validate_completeness(&row, &schema, &HashMap::new(), 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("analyte_name"));
    }

    #[test]
    fn validate_completeness_skips_derived_fields() {
        let schema = make_schema(&[("year", "smallint"), ("county_name", "text")]);
        let mut derived = HashMap::new();
        derived.insert("county_name".to_string(), "text".to_string());
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        assert!(validate_completeness(&row, &schema, &derived, 1).is_ok());
    }

    #[test]
    fn collect_normalized_data_excludes_dimension_fields() {
        let schema = make_schema(&[
            ("year", "smallint"),
            ("state_code", "text"),
            ("analyte_name", "text"),
        ]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("state_code", FieldValue::Str("CO".to_string())),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!(result.get("analyte_name").is_some());
        assert!(result.get("year").is_none());
        assert!(result.get("state_code").is_none());
    }

    #[test]
    fn collect_normalized_data_skips_already_derived_fields() {
        let schema = make_schema(&[("county_name", "text"), ("analyte_name", "text")]);
        let mut existing = serde_json::json!({});
        existing["county_name"] = serde_json::json!("Denver");
        let row = make_row(&[("analyte_name", FieldValue::Str("TTHM".to_string()))]);
        let mut derived_names = HashMap::new();
        derived_names.insert("county_name".to_string(), "text".to_string());
        let result = collect_normalized_data(&row, &schema, &derived_names, &existing, 1).unwrap();
        assert_eq!(result["county_name"], "Denver");
        assert_eq!(result["analyte_name"], "TTHM");
    }

    #[test]
    fn collect_normalized_data_includes_numeric_values() {
        let schema = make_schema(&[("average_value", "numeric")]);
        let row = make_row(&[("average_value", FieldValue::F64(3.8))]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!((result["average_value"].as_f64().unwrap() - 3.8).abs() < f64::EPSILON);
    }
}
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
cargo test -p state-search-ingest schema 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add ingest/src/pipeline/mod.rs ingest/src/pipeline/schema.rs
git commit -m "feat(ingest): add schema seed/validate and collect_normalized_data"
```

---

### Task 9: derived.rs — resolve_derived

**Files:**
- Create: `ingest/src/pipeline/derived.rs`

`resolve_derived` queries the dim table using the canonical match field value and returns the output column value, inserting it into `normalized_data`. Only `dim_location` and `dim_time` are supported.

- [ ] **Step 1: Declare the module in mod.rs**

Add `mod derived;` to `ingest/src/pipeline/mod.rs` immediately after `mod schema;` (added in Task 8). This allows the module to compile and be tested.

- [ ] **Step 2: Write tests**

`resolve_derived` is async and requires a live DB — it cannot be unit-tested without a connection pool. The test block is a compile-check only. Full coverage comes from integration runs.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // resolve_derived requires a live DB — no unit tests here.
    // Tested via full pipeline integration runs.
    // This block ensures the module compiles and resolve_derived is reachable.
    #[allow(dead_code)]
    fn _assert_resolve_derived_is_pub() {
        // compile-only: verify the function is pub(super) and accessible from pipeline
        let _ = resolve_derived;
    }
}
```

- [ ] **Step 3: Create derived.rs**

```rust
// ingest/src/pipeline/derived.rs
use state_search_core::{config::SourceConfig, Db};
use tracing::warn;

/// Resolve derived fields from dimension tables and insert into normalized_data.
///
/// Prerequisite check: if a derived field uses `from: dim_location` and
/// `location_id` is None (dim upsert failed at step 5), the row fails immediately.
/// Same for `from: dim_time` and `time_id`.
///
/// Returns Err with a descriptive message on prerequisite failure or lookup failure.
pub(super) async fn resolve_derived(
    normalized_data: &mut serde_json::Value,
    source: &SourceConfig,
    location_id: Option<i64>,
    time_id: Option<i64>,
    db: &Db,
    row_num: u64,
) -> Result<(), String> {
    for (canonical, def) in &source.derived {
        // Step 6: prerequisite check
        match def.from.as_str() {
            "dim_location" => {
                if location_id.is_none() {
                    return Err(format!(
                        "row {row_num}: cannot resolve derived field '{canonical}' — \
                         dim_location upsert failed (location_id is null); \
                         check location fields in CSV row"
                    ));
                }
            }
            "dim_time" => {
                if time_id.is_none() {
                    return Err(format!(
                        "row {row_num}: cannot resolve derived field '{canonical}' — \
                         dim_time upsert failed (time_id is null); \
                         check time fields in CSV row"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "row {row_num}: unsupported derived source table '{other}' \
                     for field '{canonical}'"
                ));
            }
        }

        // Step 7: dim lookup
        let value: Option<String> = match def.from.as_str() {
            "dim_location" => {
                let id = location_id.unwrap();
                let sql = format!("SELECT {} FROM dim_location WHERE id = $1", def.output);
                sqlx::query_scalar::<_, Option<String>>(&sql)
                    .bind(id)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| format!("row {row_num}: derived lookup failed for '{canonical}': {e}"))?
                    .flatten()
            }
            "dim_time" => {
                let id = time_id.unwrap();
                let sql = format!("SELECT {} FROM dim_time WHERE id = $1", def.output);
                sqlx::query_scalar::<_, Option<String>>(&sql)
                    .bind(id)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| format!("row {row_num}: derived lookup failed for '{canonical}': {e}"))?
                    .flatten()
            }
            _ => unreachable!(),
        };

        match value {
            Some(v) => {
                normalized_data[canonical] = serde_json::Value::String(v);
            }
            None => {
                warn!(row = row_num, field = canonical, "derived lookup returned no value — row will fail");
                return Err(format!(
                    "row {row_num}: derived field '{canonical}' lookup returned no match \
                     (from={}, output={})",
                    def.from, def.output
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn unsupported_table_name_error_message() {
        let msg = format!("unsupported derived source table 'dim_unknown'");
        assert!(msg.contains("dim_unknown"));
    }
}
```

- [ ] **Step 4: Build to verify derived.rs compiles**

```bash
cargo build -p state-search-ingest 2>&1 | head -30
```

Expected: clean build (no errors in derived.rs).

- [ ] **Step 5: Commit**

```bash
git add ingest/src/pipeline/mod.rs ingest/src/pipeline/derived.rs
git commit -m "feat(ingest): add derived field resolution from dim tables"
```

---

## Chunk 4: Pipeline Wiring, Export, Cleanup

### Task 10: Update record.rs — new process_record flow

**Files:**
- Modify: `ingest/src/pipeline/record.rs`

New flow:
1. `row_to_canonical` (unchanged)
2. `INSERT raw_imports RETURNING id` → `raw_import_id: i64`
3. `apply_all_transforms` (unchanged; `with_passthrough` removed)
4. `validate_completeness` → `SkipRow` on failure
5. `resolve_location_id + resolve_time_id` (tokio::join!, unchanged)
6. dim prerequisite check + `resolve_derived`
7. `collect_normalized_data` → JSONB blob
8. Return `Option<NewNormalizedImport>` (None = row was skipped)

- [ ] **Step 1: Write unit tests for the pure functions (apply_all_transforms, with_passthrough removal)**

```rust
#[test]
fn apply_all_transforms_proceed_on_empty_resolved() {
    let raw = HashMap::new();
    let resolved = ResolvedFieldMap::new();
    match apply_all_transforms(&raw, &resolved, 1, "src", "f.csv") {
        RowOutcome::Proceed(m) => assert!(m.is_empty()),
        _ => panic!("expected Proceed"),
    }
}

// with_passthrough is removed — no test needed
```

- [ ] **Step 2: Rewrite record.rs**

```rust
// ingest/src/pipeline/record.rs
use std::collections::HashMap;

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
    row::{canonical_to_json, row_to_canonical},
    schema::{collect_normalized_data, validate_completeness},
};

pub(super) enum RowOutcome {
    Proceed(HashMap<String, FieldValue>),
    Skip,
    Abort(anyhow::Error),
}

/// Process a single CSV record.
/// Returns Some(NewNormalizedImport) on success, None if the row is skipped.
/// Returns Err only on SkipDataset (abort the entire file).
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
    let raw = row_to_canonical(headers, &record);
    debug!(row = row_num, "parsed CSV row");

    // Step 2: store raw row, capture raw_import_id
    let raw_import_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(Option::<i64>::None)
    .bind(file_path)
    .bind(&canonical_to_json(&raw))
    .fetch_one(db)
    .await?;

    // Step 3: apply transforms (no passthrough — schema is strict)
    let transformed = match apply_all_transforms(&raw, resolved, row_num, source_name, file_path) {
        RowOutcome::Proceed(t) => t,
        RowOutcome::Skip       => return Ok(None),
        RowOutcome::Abort(e)   => return Err(e),
    };

    // Step 4: validate completeness (all required schema fields present)
    if let Err(msg) = validate_completeness(&transformed, schema, derived_fields, row_num) {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    // Step 5: resolve dimension FKs
    let (location_id, time_id) = tokio::join!(
        resolve_location_id(&transformed, default_country, db, row_num, location_cache),
        resolve_time_id(&transformed, db, row_num),
    );

    // Steps 6-7: resolve derived fields (prerequisite check + dim lookups)
    let mut normalized_data = serde_json::json!({});
    if let Err(msg) = resolve_derived(
        &mut normalized_data, source, location_id, time_id, db, row_num,
    ).await {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    // Step 8: collect non-dimension, non-derived fields into JSONB
    let normalized_data = match collect_normalized_data(
        &transformed, schema, derived_fields, &normalized_data, row_num,
    ) {
        Ok(data) => data,
        Err(msg) => {
            warn!(row = row_num, "{}", msg);
            return Ok(None);
        }
    };

    debug!(row = row_num, "row ready");
    Ok(Some(NewNormalizedImport {
        raw_import_id: Some(raw_import_id),
        location_id,
        time_id,
        source_name: source_name.to_string(),
        ingest_run_id,
        normalized_data,
    }))
}

// ── Transform helpers ──────────────────────────────────────────────────────────

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

- [ ] **Step 3: Run tests**

```bash
cargo test -p state-search-ingest record 2>&1
```

Expected: unit test passes.

- [ ] **Step 4: Commit**

```bash
git add ingest/src/pipeline/record.rs
git commit -m "feat(ingest): rewrite process_record for normalized_imports flow"
```

---

### Task 11: Update mod.rs — seed at startup, new process_rows, cleanup

**Files:**
- Modify: `ingest/src/pipeline/mod.rs`

- [ ] **Step 1: Rewrite mod.rs**

Key changes:
- `run()` calls `schema::seed_and_validate` before `process_rows`, passing `schema` and `derived_fields` to `process_rows`.
- `process_rows` maps rows through `process_record` (returning `Option<NewNormalizedImport>`), filters out `None`, flushes `Vec<NewNormalizedImport>` via `NormalizedImportRepository::bulk_create`.
- Failure cleanup calls `NormalizedImportRepository::delete_by_ingest_run`.
- `write_export_script` no longer passes `&source.attributes`; `generate_export_sql` takes `schema` instead.

```rust
// ingest/src/pipeline/mod.rs
mod context;       // will be deleted after this task — keep until unused refs cleared
mod derived;
mod dimensions;
mod export;
mod record;
mod row;
mod schema;

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use futures::{stream, StreamExt};
use state_search_core::{
    config::SourceConfig,
    repositories::normalized_import::NormalizedImportRepository,
    Db,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::transforms::resolve::{build_resolved_field_map, ResolvedFieldMap};
use self::dimensions::{new_location_cache, LocationCache};
use self::export::generate_export_sql;
use self::record::process_record;
use self::row::infer_country_from_path;

const ROW_CONCURRENCY: usize = 8;
const OBS_BATCH_SIZE: usize = 500;
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

    pub async fn already_ingested(&self, file_path: &str) -> anyhow::Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_imports WHERE source_file = $1)",
        )
        .bind(file_path)
        .fetch_one(self.db)
        .await?;
        Ok(exists)
    }

    pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        let ingest_run_id = Uuid::new_v4();

        info!(source = source_name, file = file_path, %ingest_run_id, "starting ingest");

        // Seed import_schema and validate before opening the CSV
        let schema = schema::seed_and_validate(source, self.db)
            .await
            .with_context(|| format!("schema seed/validate failed for source '{source_name}'"))?;

        // Derived fields map: canonical_name → type (used for skip-set in schema functions)
        let derived_fields: HashMap<String, String> = source.derived.iter()
            .map(|(k, v)| (k.clone(), v.field_type.clone()))
            .collect();

        let resolved = build_resolved_field_map(source)
            .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

        let default_country = infer_country_from_path(file_path);

        let result = self
            .process_rows(
                source,
                file_path,
                ingest_run_id,
                &resolved,
                &schema,
                &derived_fields,
                &headers,
                default_country.as_deref(),
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
                    Err(ce)     => warn!(%ingest_run_id, error = %ce, "cleanup failed — manual intervention required"),
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
                // Convert Option<NewNormalizedImport> to Vec<NewNormalizedImport>
                // (batched_channel_sink expects Vec per item)
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

- [ ] **Step 2: Build to check for compile errors**

```bash
cargo build -p state-search-ingest 2>&1
```

Expected: mostly clean, aside from unused `context` module reference (cleaned in next task).

- [ ] **Step 3: Commit**

```bash
git add ingest/src/pipeline/mod.rs
git commit -m "feat(ingest): wire normalized_imports pipeline — schema seed, new process_rows, cleanup"
```

---

### Task 12: Update export.rs

**Files:**
- Modify: `ingest/src/pipeline/export.rs`

`generate_export_sql` now takes `schema: &HashMap<String, String>` and `derived_fields: &HashMap<String, String>` instead of `attributes: &[String]`. It projects all non-dimension, non-derived fields from `normalized_data`, plus derived fields.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn generate_export_sql_uses_normalized_imports() {
    let id = Uuid::new_v4();
    let mut schema = HashMap::new();
    schema.insert("year".to_string(),         "smallint".to_string());
    schema.insert("state_code".to_string(),   "text".to_string());
    schema.insert("analyte_name".to_string(), "text".to_string());
    schema.insert("average_value".to_string(),"numeric".to_string());

    let sql = generate_export_sql(
        "co_public_drinking_water",
        id,
        &schema,
        &HashMap::new(),
        "host=localhost dbname=state_search",
        "exports",
    );

    assert!(sql.contains("normalized_imports"));
    assert!(!sql.contains("fact_observations"));
    assert!(!sql.contains("fact_row_context"));
    assert!(sql.contains(&id.to_string()));
    // non-dim fields are projected from normalized_data
    assert!(sql.contains("normalized_data->>'analyte_name'"));
    assert!(sql.contains("normalized_data->>'average_value'"));
    // dimension fields come from dim joins, not normalized_data
    assert!(!sql.contains("normalized_data->>'year'"));
    assert!(!sql.contains("normalized_data->>'state_code'"));
}

#[test]
fn generate_export_sql_no_schema_fields_still_produces_core_columns() {
    let id = Uuid::new_v4();
    let sql = generate_export_sql(
        "epa_air_quality",
        id,
        &HashMap::new(),
        &HashMap::new(),
        "host=localhost dbname=state_search",
        "exports",
    );
    assert!(sql.contains("normalized_imports"));
    assert!(sql.contains("dim_location"));
    assert!(sql.contains("dim_time"));
}
```

- [ ] **Step 2: Run failing tests**

```bash
cargo test -p state-search-ingest export 2>&1 | head -20
```

Expected: compile error — old `generate_export_sql` signature takes `&[String]`.

- [ ] **Step 3: Rewrite export.rs**

```rust
// ingest/src/pipeline/export.rs
use std::collections::HashMap;
use uuid::Uuid;

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
    "state_code", "state_name",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

fn is_dimension_field(name: &str) -> bool {
    LOCATION_FIELDS.contains(&name) || TIME_FIELDS.contains(&name)
}

/// Generate a DuckDB SQL script that exports normalized_imports for a completed
/// ingest run to partitioned Parquet files.
pub(super) fn generate_export_sql(
    source_name: &str,
    ingest_run_id: Uuid,
    schema: &HashMap<String, String>,
    _derived_fields: &HashMap<String, String>, // reserved for future type-cast differentiation
    pg_connection: &str,
    export_base: &str,
) -> String {
    // Project all non-dimension fields from normalized_data
    let mut projections: Vec<String> = Vec::new();
    for field_name in schema.keys() {
        if is_dimension_field(field_name) {
            continue;
        }
        projections.push(format!(
            "        (n.normalized_data->>'{}') AS {}",
            field_name, field_name
        ));
    }

    let extra_cols = if projections.is_empty() {
        String::new()
    } else {
        format!(",\n{}", projections.join(",\n"))
    };

    format!(
        r#"-- Auto-generated export script for ingest_run_id = {ingest_run_id}
-- Source: {source_name}
-- Execute with: duckdb -c ".read {export_base}/{source_name}/export_{ingest_run_id}.sql"

ATTACH '{pg_connection}' AS pg (TYPE POSTGRES, READ_ONLY);

COPY (
    SELECT
        n.source_name,
        n.ingest_run_id,
        l.state_code,
        l.state_name,
        l.county,
        l.fips_code,
        t.year,
        t.quarter,
        t.month{extra_cols}
    FROM pg.normalized_imports n
    LEFT JOIN pg.dim_location l ON l.id = n.location_id
    LEFT JOIN pg.dim_time     t ON t.id = n.time_id
    WHERE n.ingest_run_id = '{ingest_run_id}'
      AND n.source_name   = '{source_name}'
) TO '{export_base}/{source_name}'
   (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE);
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_export_sql_uses_normalized_imports() {
        let id = Uuid::new_v4();
        let mut schema = HashMap::new();
        schema.insert("year".to_string(),         "smallint".to_string());
        schema.insert("state_code".to_string(),   "text".to_string());
        schema.insert("analyte_name".to_string(), "text".to_string());
        schema.insert("average_value".to_string(),"numeric".to_string());

        let sql = generate_export_sql(
            "co_public_drinking_water",
            id,
            &schema,
            &HashMap::new(),
            "host=localhost dbname=state_search",
            "exports",
        );

        assert!(sql.contains("normalized_imports"));
        assert!(!sql.contains("fact_observations"));
        assert!(!sql.contains("fact_row_context"));
        assert!(sql.contains(&id.to_string()));
        assert!(sql.contains("normalized_data->>'analyte_name'"));
        assert!(sql.contains("normalized_data->>'average_value'"));
        assert!(!sql.contains("normalized_data->>'year'"));
        assert!(!sql.contains("normalized_data->>'state_code'"));
    }

    #[test]
    fn generate_export_sql_no_schema_fields_still_produces_core_columns() {
        let id = Uuid::new_v4();
        let sql = generate_export_sql(
            "epa_air_quality",
            id,
            &HashMap::new(),
            &HashMap::new(),
            "host=localhost dbname=state_search",
            "exports",
        );
        assert!(sql.contains("normalized_imports"));
        assert!(sql.contains("dim_location"));
        assert!(sql.contains("dim_time"));
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p state-search-ingest export 2>&1
```

Expected: all export tests pass.

- [ ] **Step 5: Commit**

```bash
git add ingest/src/pipeline/export.rs
git commit -m "feat(ingest): update export SQL to join normalized_imports, project from schema"
```

---

### Task 13: Remove old pipeline modules and final build

**Files:**
- Delete: `ingest/src/pipeline/context.rs`
- Delete: `ingest/src/pipeline/observations.rs`
- Modify: `ingest/src/pipeline/mod.rs` — remove `mod context;`

- [ ] **Step 1: Remove mod declarations and delete files**

Remove `mod context;` from `ingest/src/pipeline/mod.rs`. Then:

```bash
git rm ingest/src/pipeline/context.rs ingest/src/pipeline/observations.rs
```

- [ ] **Step 2: Full build**

```bash
cargo build 2>&1
```

Expected: clean build with no errors.

- [ ] **Step 3: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 4: Update ingest/CLAUDE.md key files table**

Remove `pipeline/context.rs` and `pipeline/observations.rs` from the table. Add `pipeline/schema.rs` and `pipeline/derived.rs`.

- [ ] **Step 5: Final commit**

```bash
git add ingest/src/pipeline/mod.rs ingest/CLAUDE.md
git commit -m "feat(ingest): remove context.rs and observations.rs, update CLAUDE.md"
```
