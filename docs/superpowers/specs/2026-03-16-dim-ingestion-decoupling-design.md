# Dim Ingestion Decoupling Design

**Date:** 2026-03-16
**Status:** Approved

## Summary

Decouple dimension table ingestion from fact ingestion by introducing a dedicated `config/dims.yml` config file, two new CLI commands (`reload-dims`, `run-dim`), separate Docker build targets for API and ingest images, and an `ingest-dims` Docker service that runs before fact ingestion.

---

## 1. Config (`core/src/config.rs`)

### New struct

```rust
pub struct DimIngestConfig {
    pub sources: Vec<SourceConfig>,
}
```

### Updated `AppConfig`

```rust
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub server:   ServerConfig,
    pub ingest:   IngestConfig,    // fact sources — config/sources.yml
    pub dims:     DimIngestConfig, // dim sources  — config/dims.yml
}
```

### Load behaviour

`AppConfig::load()` reads `config/dims.yml` using the same parsing path as `config/sources.yml`. After deserializing, it validates that every entry in `dims.sources` has `target` set, returning a descriptive error if not:

```
dims.yml entry 'foo' is missing required 'target' field
```

`DimIngestConfig` derives `Default` so the field is optional at the struct level; the file is optional on disk (missing file = empty sources list).

---

## 2. `config/dims.yml`

Uses the **exact same YAML contract** as `sources.yml` (`SourceConfig` fields). The `target` field is required for every entry (enforced at load time).

Existing dim entries are **migrated** out of `sources.yml` into `dims.yml`. After migration, `sources.yml` contains only fact sources (no entry has `target` set).

Example:

```yaml
# config/dims.yml
dims:
  - name: us_locations
    target: dim_location
    unique_key:
      - fips_code
    files:
      - data/locations.csv
    fields:
      county_fips:
        canonical: fips_code
        type: text
    derived: {}
```

---

## 3. CLI (`ingest/src/main.rs`)

### New subcommands

| Command | Description |
|---------|-------------|
| `reload-dims` | Process all sources from `dims.yml`, skipping already-ingested files (via `dim_file_log`) |
| `run-dim --source <name> [--file <path>]` | Run one named dim source; `--file` bypasses the skip check |

### Existing commands become fact-only

`reload` and `run` read exclusively from `cfg.ingest.sources` and always call `pipeline.run()`. The `if source.target.is_some()` branching is **removed entirely** from both handlers.

`reload-dims` and `run-dim` read exclusively from `cfg.dims.sources` and always call `pipeline.run_dim()` — no branching needed.

### Result

`main.rs` drops from 4 conditional paths to 2 clean paths per command pair.

---

## 4. Dockerfile

Split the single `runtime` stage into two named final stages, both sourcing from the shared `rust-builder`:

```dockerfile
# Stage 4a: API runtime
FROM debian:bookworm-slim AS api
COPY --from=rust-builder /app/target/release/api /app/api
COPY --from=web-builder  /app/web/dist           /app/web/dist
COPY config/                                      /app/config/
ENV APP_SERVER__HOST=0.0.0.0
ENV APP_SERVER__PORT=3000
EXPOSE 3000
CMD ["/app/api"]

# Stage 4b: Ingest runtime
FROM debian:bookworm-slim AS ingest
COPY --from=rust-builder /app/target/release/ingest /app/ingest
COPY config/                                         /app/config/
ENTRYPOINT ["/app/ingest"]
```

Each image is lean and single-purpose:
- API image: `api` binary + web dist + config (no ingest binary)
- Ingest image: `ingest` binary + config (no web assets)

---

## 5. `docker-compose.yml`

Services use build targets to select the appropriate image:

```yaml
ingest-dims:
  build: { context: ., target: ingest }
  command: ["reload-dims"]
  environment:
    APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
  depends_on:
    db:
      condition: service_healthy
  volumes:
    - ./data:/app/data
    - ./config:/app/config

ingest:
  build: { context: ., target: ingest }
  command: ["reload"]
  environment:
    APP__DATABASE__URL: postgres://postgres:postgres@db:5432/state_search
  depends_on:
    db:
      condition: service_healthy
    ingest-dims:
      condition: service_completed_successfully
  volumes:
    - ./data:/app/data
    - ./config:/app/config
    - ./exports:/app/exports

api:
  build: { context: ., target: api }
  depends_on:
    db:
      condition: service_healthy
    ingest:
      condition: service_completed_successfully
```

**Execution order:** `db` → `ingest-dims` → `ingest` → `api`

---

## 6. Error Handling

- Missing `config/dims.yml`: treated as empty dims list (no error, no-op)
- Entry in `dims.yml` without `target`: hard error at startup with source name in message
- Dim source name not found in `dims.yml` for `run-dim`: same error pattern as `run` for `sources.yml`

---

## 7. What Does Not Change

- `pipeline/dim.rs` — no changes needed; `run_dim` is already decoupled from `raw_imports`
- `SourceConfig` struct — unchanged; reused for both files
- `dim_file_log` tracking — unchanged
- `pipeline/mod.rs` — `IngestPipeline::run_dim` and `run` unchanged
- Fact ingestion path — no behavioral changes
