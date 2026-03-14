# state-search-ingest

CLI tool for ingesting state-level CSV data into the state-search database. Sources are defined in `config/sources.toml` — no manual registration step required.

## Configuration

Define sources in `config/sources.toml`:

```toml
[[ingest.sources]]
name        = "my_source"
description = "Optional description"
files       = ["data/my_source/file_2024.csv"]

[ingest.sources.field_map]
# canonical_name = "CSV_column_header"
state_name = "State"
year       = "Year"
```

`field_map` keys are **canonical names**; values are the **CSV column headers** in your file.

## Commands

### `reload` — ingest all sources

```bash
cargo run -p state-search-ingest -- reload
```

Iterates every source in `config/sources.toml` and every file listed under it. Files already present in `raw_imports` are skipped automatically.

### `run --source <name>` — ingest one source

```bash
cargo run -p state-search-ingest -- run --source <name>
```

Ingests all configured files for the named source, skipping already-processed files. Errors if the source is not in `config/sources.toml`.

### `run --source <name> --file <path>` — ingest a specific file

```bash
cargo run -p state-search-ingest -- run --source <name> --file data/file.csv
```

Ingests a specific file using the named source's `field_map`. The source must exist in config.

## Field Map Reference

`field_map` maps canonical dimension names to CSV column headers. Columns not listed in `field_map` that aren't already canonical names pass through unchanged and become metric observations.

### Location fields (canonical names)

| Canonical Name | Description |
|---|---|
| `state_code` | Two-letter state abbreviation (e.g. `CA`) |
| `state_name` | Full state name (e.g. `California`) |
| `country` | Country code |
| `zip_code` | ZIP / postal code |
| `fips_code` | FIPS numeric code |
| `latitude` | Decimal latitude |
| `longitude` | Decimal longitude |

### Time fields (canonical names)

| Canonical Name | Description |
|---|---|
| `year` | Year (required) |
| `quarter` | Quarter (1–4, optional) |
| `month` | Month (1–12, optional) |
| `day` | Day of month (optional) |

### Everything else → metrics

Any column not mapped to a location or time canonical name is treated as a metric and stored as a `fact_observations` row with that column name as `metric_name`.

## Docker

`docker-compose up` runs `ingest reload` automatically before starting the API. No manual step needed.

To force-ingest a specific file:

```bash
docker-compose run --rm ingest run --source <name> --file /data/file.csv
```

## Configuration

Reads `config/default.toml` and `config/sources.toml`, then applies `APP__*` environment variable overrides. See the root [CLAUDE.md](../CLAUDE.md) for the full config reference.
