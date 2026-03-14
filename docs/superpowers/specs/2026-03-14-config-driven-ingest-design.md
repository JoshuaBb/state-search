# Config-Driven Ingest Design

**Date:** 2026-03-14
**Status:** Approved

## Overview

Replace the database-backed source registry with a config-file-driven approach. Sources (name, field_map, file paths) are declared in `config/sources.toml`. A new `reload` command ingests all configured sources, skipping files already processed. The Docker setup runs `reload` automatically on container start before the API comes up.

## Goals

- Sources fully defined in `config/sources.toml` — no manual `register-source` step required
- `reload` command ingests all sources from config, skipping already-ingested files
- `run` command still works for individual source or file ingestion
- Ingest container runs `reload` automatically on `docker-compose up`

## Non-Goals

- Changes to the API crate
- Removing the `import_sources` table (left in schema; `POST /api/sources` becomes vestigial but the API route and `ImportRepository` in `core` are unchanged — only the ingest binary stops using them)

---

## Config Structure

A new optional file `config/sources.toml` holds all source definitions. The `config` crate loads it as a second layer after `config/default.toml`. The file is also copied into the Docker image at `/app/config/sources.toml`.

```toml
[[ingest.sources]]
name        = "co_public_drinking_water"
description = "Colorado public drinking water quality"
files       = ["data/co/public_drinking_water/public_drinking_water_2026-03-14.csv"]

[ingest.sources.field_map]
# Format: canonical_name = "source_csv_column"
# Keys are canonical dimension names; values are the CSV column headers.
state_name = "state"
fips_code  = "county_fips"
year       = "year"
quarter    = "quarter"
latitude   = "pws_latitude"
longitude  = "pws_longitude"
```

`files` is an array to support accumulating multiple data files per source over time.

### field_map direction

`field_map` uses **`canonical_name = "source_csv_column"`** format — the same direction as the existing DB `field_map`. The `build_field_map` function is rewritten to accept `&HashMap<String, String>` (canonical → source_col) and inverts it to produce `HashMap<source_col → canonical>` for use by `apply_field_map`. Signature:

```rust
fn build_field_map(field_map: &HashMap<String, String>) -> HashMap<String, String> {
    field_map.iter().map(|(canonical, src_col)| (src_col.clone(), canonical.clone())).collect()
}
```

### New config structs (core)

```rust
#[derive(Debug, Deserialize, Default)]
pub struct IngestConfig {
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub field_map: HashMap<String, String>,
}
```

`AppConfig` gains:
```rust
#[serde(default)]
pub ingest: IngestConfig,
```

The `#[serde(default)]` on `ingest` ensures deserialization succeeds when `sources.toml` is absent and no `[ingest]` section exists in any config file.

---

## CLI Commands

`register-source` is removed. The binary exposes:

| Command | Behaviour |
|---|---|
| `ingest reload` | Ingest all sources from config; skip already-processed files; exits 0 even if sources list is empty |
| `ingest run --source <name>` | Ingest all files for one named source from config; skip already-processed; error if source not found |
| `ingest run --source <name> --file <path>` | Ingest a specific file, **bypassing the skip check**; source must exist in config for its field_map |

`--file` is `Option<String>` (i.e. `#[arg(long)]` with no `required = true`).

### Error and edge cases

- `run --source <name>` where `<name>` is not in config: `error: source '<name>' not found in config — add it to config/sources.toml`, non-zero exit
- `run --source <name>` with no `--file` and an empty `files` list: log `"source '<name>' has no files configured, nothing to do"`, exit 0
- `reload` with no sources configured: log `"no sources configured, nothing to do"`, exit 0
- `reload` / `run` where all files for a source are already ingested: log each skip, exit 0

---

## Skip Logic

Before ingesting a file (when not using the `--file` override), query `raw_imports` by `source_file`:

```sql
SELECT 1 FROM raw_imports WHERE source_file = $1 LIMIT 1
```

If a row exists, log `"skipping <file> (already ingested)"` and continue to the next file. No new columns or tables required. The `--file` override always skips this check.

---

## DB Changes

- `import_sources` table and all migrations are **unchanged**.
- `ImportRepository` in `core` is **unchanged** (the API still uses it via `POST /api/sources`).
- In the ingest crate, `ImportRepository` is no longer used. `create_raw` and `set_normalized` are accessed via a thin `RawImportRepository` (extracted from the existing `ImportRepository`) or directly via `sqlx::query!` calls — implementor's choice, whichever is simpler.
- `raw_imports.source_id` will be `NULL` for all new rows (already nullable). `NewRawImport { source_id: None, ... }`.
- All other tables (`dim_location`, `dim_time`, `fact_observations`) are used exactly as before.

### latitude / longitude fix

`LOCATION_FIELDS` in `pipeline.rs` currently omits `"latitude"` and `"longitude"`. Both will be added to prevent them from falling through to `extract_metrics`. `resolve_location` will also be updated to read these values from the canonical map using a `f64_field` helper (similar to the existing `str_field` / `i16_field`):

```rust
fn f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key)
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}
```

`NewLocation.latitude` and `NewLocation.longitude` are populated from `f64_field(canonical, "latitude")` and `f64_field(canonical, "longitude")`.

---

## Config Loading (core)

`AppConfig::load()` adds `config/sources.toml` as a `required(false)` layer. The runtime `WORKDIR` is `/app` (confirmed from Dockerfile), so the relative path `"config/sources"` resolves correctly to `/app/config/sources.toml`:

```rust
Config::builder()
    .add_source(File::with_name("config/default"))
    .add_source(File::with_name("config/sources").required(false))
    .add_source(Environment::with_prefix("APP").separator("__"))
    .build()
```

---

## Pipeline Changes (ingest)

`IngestPipeline::run` signature changes from `(source_name: &str, file_path: &str)` to `(source: &SourceConfig, file_path: &str)`. The field_map is taken directly from `source.field_map` and passed to the rewritten `build_field_map`. No DB lookup for the source definition.

---

## Docker Changes

### Dockerfile

No change to `CMD` — the Dockerfile's runtime stage keeps `CMD ["/app/api"]`. The ingest service overrides the entrypoint in docker-compose.

### docker-compose.yml

The `ingest` service drops `profiles: ["ingest"]` and becomes a startup init service. The existing `entrypoint: ["/app/ingest"]` is kept; `command` is set to `reload`. A `./config` volume mount is added so that `sources.toml` changes are picked up without rebuilding the image. The `api` service gains a `depends_on` on `ingest`:

```yaml
ingest:
  build: .
  entrypoint: ["/app/ingest"]
  command: reload
  depends_on:
    db:
      condition: service_healthy
  environment:
    APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
  volumes:
    - ./data:/data
    - ./config:/app/config   # picks up sources.toml without rebuild

api:
  build: .
  restart: unless-stopped
  ports:
    - "3000:3000"
  environment:
    APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
    APP__SERVER__HOST:  0.0.0.0
    APP__SERVER__PORT:  "3000"
  depends_on:
    db:
      condition: service_healthy
    ingest:
      condition: service_completed_successfully
```

`ingest reload` always exits 0 (even with no sources or no new files), so the API always starts. On subsequent `docker-compose up` runs, all files are already ingested and `reload` exits quickly.

---

## File Inventory

| File | Change |
|---|---|
| `config/sources.toml` | **New** — source definitions with field_map and file paths |
| `core/src/config.rs` | Add `IngestConfig`, `SourceConfig`; add `#[serde(default)]` on `AppConfig.ingest`; update `load()` |
| `ingest/src/main.rs` | Remove `RegisterSource`; add `Reload`; make `--file` optional on `Run`; remove `ImportRepository` usage |
| `ingest/src/pipeline.rs` | Accept `&SourceConfig`; rewrite `build_field_map`; add skip check; add `latitude`/`longitude` to `LOCATION_FIELDS`; add `f64_field` helper; wire lat/lon into `resolve_location` |
| `docker-compose.yml` | Drop `profiles` from ingest; add `command: reload`; add config volume; add `depends_on: ingest` to api |
| `ingest/README.md` | Update commands and config reference |
