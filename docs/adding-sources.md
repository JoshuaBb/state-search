# Adding a New Data Source

This guide walks through onboarding a new CSV file end-to-end — from writing the config entry to verifying data in the UI.

---

## Overview

Each data source needs a single entry in `config/sources.toml`. The entry tells the ingest pipeline:
- The source's name (used as an identifier throughout the system)
- Which CSV files belong to it
- How CSV column headers map to canonical field names

---

## Step 1 — Inspect your CSV

Open the file and note the column headers. For example:

```
State,Year,Quarter,Unemployment_Rate,Labor_Force_Size
Alabama,2023,1,3.2,2350000
Alaska,2023,1,4.8,390000
...
```

You need to identify:
- **Location columns** — state name, county, zip code, FIPS code, lat/lon
- **Time columns** — year, quarter, month, day
- **Metric columns** — everything else (becomes queryable data)

---

## Step 2 — Write the `sources.toml` entry

Open `config/sources.toml` and add a new `[[ingest.sources]]` block:

```toml
[[ingest.sources]]
name        = "unemployment"
description = "BLS state unemployment rates by quarter"
files       = ["data/unemployment/bls_2023.csv"]

[ingest.sources.field_map]
# canonical_name = "CSV_column_header"
state_name = "State"
year       = "Year"
quarter    = "Quarter"
```

`field_map` maps **canonical names** (keys) to **CSV column headers** (values).

Columns not listed in `field_map` — in this case `Unemployment_Rate` and `Labor_Force_Size` — are not canonical dimension names, so they pass through automatically as metric fields stored in `normalized_data`.

### Canonical field reference

**Location fields** (any subset can be provided):

| Canonical name | Description |
|---------------|-------------|
| `county` | County or region name |
| `country` | Country code (defaults to `USA`) |
| `zip_code` | ZIP / postal code |
| `fips_code` | FIPS numeric code |
| `latitude` | Decimal latitude |
| `longitude` | Decimal longitude |

**Time fields** (`year` is required if providing any time fields):

| Canonical name | Description |
|---------------|-------------|
| `year` | Year (required) |
| `quarter` | Quarter (1–4) |
| `month` | Month (1–12) |
| `day` | Day of month (1–31) |

**Everything else** is stored as-is in `normalized_data` JSONB.

---

## Step 3 — Place the CSV file

Put the file in the path you listed in `files`:

```bash
mkdir -p data/unemployment
cp /path/to/bls_2023.csv data/unemployment/
```

The `data/` directory is gitignored. In Docker, it is volume-mounted at `/app/data`.

---

## Step 4 — Run the ingest

```bash
# Local
cargo run -p state-search-ingest -- run --source unemployment

# Docker
docker-compose run --rm ingest run --source unemployment
```

You should see log output like:

```
INFO ingest: starting run  source=unemployment file=data/unemployment/bls_2023.csv
INFO ingest: pipeline done observations=204 elapsed=1.2s
```

---

## Step 5 — Verify

Check the API directly:

```bash
# Confirm the source is registered
curl http://localhost:3000/api/sources/unemployment

# Query a sample of normalized imports
curl "http://localhost:3000/api/observations?source_name=unemployment&limit=5"
```

A `normalized_imports` row should look like:

```json
{
  "id": 1,
  "source_name": "unemployment",
  "location_id": 12,
  "time_id": 3,
  "normalized_data": {
    "Unemployment_Rate": "3.2",
    "Labor_Force_Size": "2350000"
  }
}
```

Then open the UI at `http://localhost:3000` and select the `unemployment` source from the dropdown.

---

## Multiple files

List all files for the source under `files`. The ingest pipeline processes them in order and skips any file already present in `raw_imports`:

```toml
[[ingest.sources]]
name  = "unemployment"
files = [
  "data/unemployment/bls_2022.csv",
  "data/unemployment/bls_2023.csv",
]

[ingest.sources.field_map]
state_name = "State"
year       = "Year"
quarter    = "Quarter"
```

Running `cargo run -p state-search-ingest -- reload` processes all sources and all their files.

---

## Troubleshooting

**`source 'unemployment' not found`** — the source name in the CLI must match `name` in `sources.toml` exactly.

**Location dimension is null** — the CSV has no location columns, or the column headers don't match the `field_map` values. Check spelling and case sensitivity — headers are matched literally.

**All values end up in `normalized_data`** — this is correct for metric columns. Location and time fields are extracted into their dimension tables; everything else is stored in `normalized_data`.

**File is silently skipped** — files are deduplicated by `(source_name, source_file)` path. If you renamed a file, the old path was already processed. Delete the corresponding `raw_imports` row or add the new filename to `files`.
