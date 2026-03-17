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
    pub ingest:   IngestConfig,     // fact sources — config/sources.yml
    #[serde(default)]
    pub dims:     DimIngestConfig,  // dim sources  — config/dims.yml
}
```

### Load behaviour

`AppConfig` and `AppConfig::load()` in `core/src/config.rs` must both be modified as part of this implementation. Neither `DimIngestConfig` nor the `dims.yml` loading path exist yet.

`AppConfig::load()` reads `config/dims.yml` using the same parsing path as `config/sources.yml`, via an intermediate struct:

```rust
#[derive(Deserialize)]
struct DimsFile {
    #[serde(default)]
    sources: Vec<SourceConfig>,
}
```

After deserializing, it validates that every entry in `dims.sources` has `target` set. On failure it returns `config::ConfigError::Message` (consistent with how `sources.yml` parse errors are surfaced):

```
dims.yml entry 'foo' is missing required 'target' field
```

`DimIngestConfig` derives `Default` so the `dims` field is optional at the struct level; a missing `config/dims.yml` produces an empty sources list with no error.

---

## 2. `config/dims.yml`

Uses the **exact same YAML contract** as `sources.yml` (`SourceConfig` fields). The `target` field is required for every entry (enforced at load time).

Existing dim entries are **migrated** out of `sources.yml` into `dims.yml`. After migration, `sources.yml` contains only fact sources (no entry has `target` set).

The top-level YAML key is `sources:` (matching the `DimsFile` intermediate struct), mirroring `sources.yml`'s own `sources:` key.

Example:

```yaml
# config/dims.yml
sources:
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
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=rust-builder /app/target/release/api /app/api
COPY --from=web-builder  /app/web/dist           /app/web/dist
COPY config/                                      /app/config/
ENV APP__SERVER__HOST=0.0.0.0
ENV APP__SERVER__PORT=3000
EXPOSE 3000
CMD ["/app/api"]

# Stage 4b: Ingest runtime
FROM debian:bookworm-slim AS ingest
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
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

The compose-level `entrypoint:` override on the old `ingest` service is removed — the entrypoint is now baked into the `ingest` image stage.

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

**Note on profiles:** No `profiles:` guards are added to `ingest-dims` or `ingest`. Both are prerequisite one-shot jobs; the `service_completed_successfully` dependency chain means they run automatically when `api` starts. Running `docker-compose up` will always seed dims and facts before the API. This is intentional.

---

## 6. Error Handling

- Missing `config/dims.yml`: treated as empty dims list (no error, no-op)
- Entry in `dims.yml` without `target`: hard error at startup with source name in message
- Dim source name not found in `dims.yml` for `run-dim`: same error pattern as `run` for `sources.yml`

---

## 7. Tests

New unit tests in `core/src/config.rs`:
- Missing `dims.yml` → `dims.sources` is empty, no error
- Valid `dims.yml` with `target` set → deserializes correctly
- Entry in `dims.yml` missing `target` → `load()` returns `Err` with source name in message

Existing tests to update/remove in `ingest/src/main.rs`:
- `dim_source_has_target` and `fact_source_has_no_target` test the old branching behavior in `reload`/`run`. Once branching is removed, these tests are dead weight and should be deleted.

No changes to existing `pipeline/dim.rs` tests.

---

## 8. What Does Not Change

- `pipeline/dim.rs` — no changes needed; `run_dim` is already decoupled from `raw_imports`
- `SourceConfig` struct — unchanged; reused for both files
- `dim_file_log` tracking — unchanged
- `pipeline/mod.rs` — `IngestPipeline::run_dim` and `run` unchanged
- Fact ingestion path — no behavioral changes
