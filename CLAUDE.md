# State-Search

Full-stack platform for ingesting state-level CSV data and exploring metrics via a web UI.

**Stack**: Rust (Tokio + Axum + SQLx) + PostgreSQL + SvelteKit (D3, Chart.js)

## Workspace Layout

```
state-search/
  core/         # state-search-core — shared library (models, repos, DB, config)
  api/          # state-search-api  — Axum HTTP server + SPA static file serving
  ingest/       # state-search-ingest — CLI tool for registering sources and ingesting CSVs
  web/          # SvelteKit frontend (builds to web/dist/, served by api)
  migrations/   # SQL migrations (001_initial.sql — run via sqlx migrate)
  config/       # default.toml — default config values
  data/         # (gitignored) CSV data files for ingestion
```

## Common Commands

```bash
# Build all Rust crates
cargo build

# Run API server (requires Postgres)
cargo run -p state-search-api

# Run ingestion CLI
cargo run -p state-search-ingest -- register-source --name <name> --field-map '<json>'
cargo run -p state-search-ingest -- run --source <name> --file data/file.csv

# Run tests
cargo test

# Build frontend
cd web && npm install && npm run build

# Docker (local dev)
docker-compose up           # start API + Postgres
docker-compose run --rm ingest run --source <name> --file /data/file.csv
```

## Configuration

Config is loaded from `config/default.toml` then overridden by env vars with prefix `APP__` and separator `__`.

```toml
# config/default.toml
[database]
url             = "postgres://postgres:postgres@localhost:5432/state_search"
max_connections = 10

[server]
host = "0.0.0.0"
port = 3000
```

Environment override examples:
```bash
APP__DATABASE__URL=postgres://user:pass@host:5432/db
APP__SERVER__PORT=8080
```

> Prefix is `APP`, separator is `__` — so vars take the form `APP__{SECTION}__{KEY}`. Single-underscore variants like `APP_DATABASE__URL` are silently ignored.

## Database Schema (Star Schema)

**Dimensions:**
- `dim_location` — state_code, state_name, country, zip_code, fips_code, lat/lon; unique on (state_code, country, zip_code) with COALESCE
- `dim_time` — year (required), quarter/month/day (optional), date_floor; unique on (year, quarter, month, day) with COALESCE

**Facts:**
- `fact_observations` — metric_name, metric_value (NUMERIC), source_name, location_id FK, time_id FK, raw_import_id FK, attributes JSONB

**Import:**
- `import_sources` — name (UNIQUE), description, field_map JSONB (maps source columns → canonical names)
- `raw_imports` — raw_data JSONB + normalized_data JSONB, GIN indexed; stores every original CSV row

Migrations are in `migrations/` and run automatically on API startup via `sqlx::migrate!()`.

## Architecture Patterns

### Repository Pattern (core)
All DB access goes through repositories in `core/src/repositories/`. They take `&PgPool` (not `&mut`). Use `query!` or `query_as!` with runtime-checked SQL (not compile-time, so no live DB needed to compile).

### Error Handling
- `core`: `CoreError` enum via `thiserror` — variants: `NotFound`, `InvalidInput`, `Config`, `Database`, `Io`
- `api`: `ApiError` wraps `CoreError` and implements `IntoResponse` → HTTP status codes

### Shared App State (api)
`AppState` holds `Arc<Db>` (where `Db` is a newtype around `PgPool`). Injected into Axum handlers via `State<AppState>` extractor.

### Ingestion Pipeline (ingest)
`IngestPipeline` in `ingest/src/pipeline.rs`:
1. Parse CSV → rows as `serde_json::Value`
2. Store raw row in `raw_imports`
3. Apply `field_map` from source registry (source column → canonical name)
4. Extract location fields → upsert `dim_location`
5. Extract time fields → upsert `dim_time`
6. Remaining fields → metric name/value pairs → bulk insert `fact_observations`

Location canonical fields: `state_code`, `state_name`, `country`, `zip_code`, `fips_code`, `latitude`, `longitude`
Time canonical fields: `year`, `quarter`, `month`, `day`

### Field Mapping
Sources register a `field_map` JSON object like `{"Year": "year", "State": "state_code", "Value": "metric_value"}`. The ingest pipeline applies this mapping before extracting dimensions.

## API Routes

All routes are under `/api`:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/sources` | List import sources |
| GET | `/api/sources/:name` | Get source by name |
| POST | `/api/sources` | Register new source |
| GET | `/api/locations?limit=&offset=` | List locations |
| GET | `/api/observations?metric_name=&source_name=&location_id=&limit=&offset=` | Query observations |

Routes not matching `/api/*` fall through to SvelteKit SPA static files from `web/dist/`.

## Key Dependencies

| Crate | Version | Use |
|-------|---------|-----|
| axum | workspace | HTTP router |
| sqlx | 0.8 | PostgreSQL (async, no compile-time macros) |
| tokio | 1 (full) | Async runtime |
| serde / serde_json | workspace | Serialization |
| anyhow / thiserror | workspace | Error handling |
| tracing / tracing-subscriber | workspace | Structured logging (JSON in prod) |
| config | workspace | Layered config (file + env) |
| clap | derive | CLI argument parsing (ingest) |
| csv | — | CSV parsing (ingest) |
| uuid | workspace | UUIDs |
| chrono | workspace | Timestamps |

## Docker

Multi-stage Dockerfile:
1. Node 22-alpine: build `web/dist/`
2. `cargo-chef`: dependency layer cache
3. Rust slim: build `api` and `ingest` binaries in release mode
4. Debian slim runtime: final image with binaries + assets + config

`docker-compose.yml` services: `db` (Postgres 17), `api` (port 3000), `ingest` (profile: "ingest", mounts `./data:/data`).
