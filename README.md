# state-search

Full-stack platform for ingesting state-level CSV data and exploring metrics via a web UI.

**Stack:** Rust (Tokio + Axum + SQLx) · PostgreSQL · SvelteKit + D3 + Chart.js

---

## What it does

1. **Ingest** — a CLI tool reads CSV files, maps columns to canonical names, resolves location/time dimensions, and stores every row in a normalized star schema.
2. **API** — an Axum HTTP server exposes a JSON REST API over the ingested data.
3. **UI** — a SvelteKit SPA served by the API shows a choropleth map, time-series charts, and a filterable observations table.

---

## Prerequisites

- Rust (stable, via [rustup](https://rustup.rs))
- Node.js 22+ and npm
- PostgreSQL 17 (or Docker)

---

## Quick start — Docker

```bash
# Copy and edit the example source config before starting
cp config/sources.example.toml config/sources.toml

# Start Postgres, run migrations, ingest configured sources, start API
docker-compose up
```

The API is available at `http://localhost:3000`. The UI is served from the same origin.

To ingest a specific file manually:

```bash
docker-compose run --rm ingest run --source <name> --file /data/file.csv
```

---

## Quick start — local

```bash
# 1. Start Postgres (or use an existing instance)
docker-compose up db

# 2. Build and start the API (runs migrations on startup)
cargo run -p state-search-api

# 3. In a second terminal: build the frontend
cd web && npm install && npm run build

# 4. Ingest data
cargo run -p state-search-ingest -- reload
```

See **[docs/getting-started.md](docs/getting-started.md)** for a step-by-step walkthrough including database setup and sample data.

---

## Configuration

Edit `config/default.toml` for database URL and server port, and `config/sources.toml` for data sources. Both can be overridden with `APP__*` environment variables.

See the [CLAUDE.md](CLAUDE.md) configuration reference or [config/sources.example.toml](config/sources.example.toml) for an annotated sources example.

---

## Repository layout

| Directory | Crate / package | Purpose |
|-----------|----------------|---------|
| `core/` | `state-search-core` | Shared library: models, repositories, DB pool, config, errors |
| `api/` | `state-search-api` | Axum HTTP server + static file serving |
| `ingest/` | `state-search-ingest` | CLI tool for ingesting CSV files |
| `web/` | — | SvelteKit frontend (builds to `web/dist/`) |
| `migrations/` | — | SQL migrations (applied on API startup) |
| `config/` | — | `default.toml` and `sources.toml` |
| `data/` | — | CSV data files (gitignored) |

---

## Further reading

- [docs/getting-started.md](docs/getting-started.md) — full local dev setup
- [docs/adding-sources.md](docs/adding-sources.md) — add a new CSV data source
- [ingest/README.md](ingest/README.md) — ingest CLI reference
- [docs/migrations.md](docs/migrations.md) — schema evolution history
- [docs/decisions/](docs/decisions/) — architecture decision records
- [api/README.md](api/README.md) — REST API reference
- [core/README.md](core/README.md) — shared library internals
