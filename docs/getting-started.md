# Getting Started

This guide takes you from a fresh checkout to a fully running local development environment.

---

## Prerequisites

- **Rust** (stable) — install via [rustup.rs](https://rustup.rs)
- **Node.js 22+** and **npm**
- **PostgreSQL 17** — or Docker (see option A below)

---

## Option A — Docker (recommended)

Docker runs Postgres, applies migrations, ingests data, and starts the API in one command.

```bash
# 1. Create your sources config (required before first run)
cp config/sources.example.toml config/sources.toml
# Edit config/sources.toml to add your data sources and file paths

# 2. Place CSV files in data/ (the ingest container mounts this directory)
mkdir -p data

# 3. Start everything
docker-compose up
```

On first run this will:
1. Start Postgres on the default port.
2. Run the `ingest reload` command against all sources in `config/sources.toml`.
3. Start the API server on `http://localhost:3000`.

The UI is available at `http://localhost:3000`.

To re-ingest after adding new CSV files, restart the ingest service:

```bash
docker-compose run --rm ingest reload
```

---

## Option B — local (no Docker)

### 1. Start Postgres

If you have Postgres installed locally:

```bash
createdb state_search
```

The default connection string is `postgres://postgres:postgres@localhost:5432/state_search`. Override it with:

```bash
export APP__DATABASE__URL=postgres://<user>:<pass>@localhost:5432/state_search
```

Or just edit `config/default.toml`.

### 2. Build the frontend

```bash
cd web
npm install
npm run build
cd ..
```

This produces `web/dist/`, which the API server serves statically.

### 3. Start the API

```bash
cargo run -p state-search-api
```

Migrations run automatically on startup. You should see log output like:

```
INFO state_search_api: migrations applied
INFO state_search_api: listening on 0.0.0.0:3000
```

Visit `http://localhost:3000` to confirm the UI loads and `http://localhost:3000/api/health` returns `{"status":"ok"}`.

### 4. Configure sources

```bash
cp config/sources.example.toml config/sources.toml
```

Edit `config/sources.toml` to add your CSV data sources. See [adding-sources.md](adding-sources.md) for a full walkthrough.

### 5. Ingest data

```bash
# Ingest all configured sources
cargo run -p state-search-ingest -- reload

# Or ingest a single source
cargo run -p state-search-ingest -- run --source <name>
```

---

## Verifying the setup

After ingestion, check that data appeared:

```bash
# List sources registered in the DB
curl http://localhost:3000/api/sources

# Query normalized imports (first 5 rows)
curl "http://localhost:3000/api/observations?limit=5"
```

The dashboard at `http://localhost:3000` should show data on the map and chart.

---

## Frontend dev server

To iterate on the UI without rebuilding:

```bash
cd web
npm run dev
```

The Vite dev server runs on `http://localhost:5173` and proxies `/api` requests to the Rust API at `http://localhost:3000`. Keep `cargo run -p state-search-api` running in a separate terminal.

---

## Common issues

**Migrations fail on startup**
Check the database URL in `config/default.toml` or `APP__DATABASE__URL`. Ensure the database exists and the user has `CREATE TABLE` privileges.

**`sources.toml` not found**
Copy the example: `cp config/sources.example.toml config/sources.toml`. The ingest binary requires this file.

**Ingest skips files**
Files already present in `raw_imports` are skipped to avoid duplicates. This is intentional. There is no `--force` flag — delete the relevant `raw_imports` rows if you need to re-ingest.

**Map shows no data**
The choropleth requires rows with resolved `location_id`. If your source doesn't include location columns (`county`, `zip_code`, `fips_code`, etc.) the location dimension won't be populated. See [adding-sources.md](adding-sources.md) for field map guidance.
