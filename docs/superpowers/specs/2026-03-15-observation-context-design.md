# Observation Context Storage

**Date:** 2026-03-15
**Status:** Draft

## Problem

The ingest pipeline ingests heterogeneous CSV sources where each row contains:

1. **Dimension fields** — location, time (mapped to `dim_location` / `dim_time`)
2. **Numeric metric fields** — written as `fact_observations` rows
3. **String context fields** — currently discarded

String context fields (e.g. `analyte_name`, `units`, `time_period`, `pws_name`) provide
the meaning behind metric values. Without them a row like
`metric_name=average_concentration, metric_value=3.8` is uninterpretable. These fields
must be preserved.

The challenge: different sources have entirely different context fields. Schema cannot be
predicted in advance. Future sources will add new context shapes without notice.

Additionally, large-scale analytics will not be performed in Postgres. Postgres is the
operational store. DuckDB is the intended analytics query layer. The storage design must
account for both layers and leave a clear path to higher-throughput export architectures
as data volume grows.

## Constraints

- Context fields vary by source and cannot be enumerated at schema design time.
- Postgres is write-optimised operational storage, not an analytics engine.
- DuckDB expects flat, wide Parquet — not JSONB blobs or multi-table joins at query time.
- Multiple metric observations can come from a single CSV row and share identical context.
- Write throughput must not regress (the UNNEST bulk insert must be preserved).
- The design must support iteration toward an Arrow-native pipeline without a rewrite.

## Solution

Two-layer design:

**Layer 1 — Postgres (operational):** A shared `fact_row_context` table stores context
once per CSV row as a JSONB blob. `fact_observations` references it via FK. Duplication
is eliminated — 4 metrics from one CSV row share one context row rather than carrying
4 copies of the same blob.

**Layer 2 — Analytics (Parquet):** Immediately after a successful ingest run, a
post-ingest export step queries Postgres for that run's observations, joins dimensions and
context, flattens the JSONB into columns, and writes partitioned Parquet files. DuckDB
queries Parquet directly. JSONB is never exposed to the analytics layer.

## Export Strategy

### Chosen approach: Post-ingest Parquet export (Option A)

Parquet files are written immediately after `IngestPipeline::run()` succeeds, scoped to
the completed `ingest_run_id`. This makes Parquet available within seconds of ingest
completing while keeping atomicity simple — Parquet is only written when Postgres is
known-good. If the export step fails, the ingest is still committed and the export can
be retried against the same `ingest_run_id`.

```
CSV ──► ingest pipeline ──► Postgres (committed, ingest_run_id = X)
                                    │
                              on success
                                    │
                                    ▼
                         export_run(ingest_run_id=X)
                                    │
                    ┌───────────────┴────────────────┐
                    ▼                                ▼
          exports/source/                  DuckDB queries
            year=2024/                     Parquet directly
            part-XXXX.parquet
```

The export query uses DuckDB's Postgres scanner, which means the flatten-and-join logic
lives in SQL and can be iterated without touching Rust:

```sql
-- Run inside DuckDB, attaching Postgres as a remote source
ATTACH 'host=localhost dbname=state_search' AS pg (TYPE POSTGRES, READ_ONLY);

COPY (
    SELECT
        o.metric_name,
        o.metric_value,
        o.source_name,
        l.state_code,
        l.county,
        l.fips_code,
        t.year,
        t.quarter,
        t.month,
        -- Flatten source-specific context fields
        c.attributes->>'analyte_name'            AS analyte_name,
        c.attributes->>'units'                   AS units,
        c.attributes->>'time_period'             AS time_period,
        c.attributes->>'pws_name'                AS pws_name,
        c.attributes->>'pws_primary_source_code' AS pws_primary_source_code
    FROM pg.fact_observations o
    LEFT JOIN pg.dim_location     l ON l.id  = o.location_id
    LEFT JOIN pg.dim_time         t ON t.id  = o.time_id
    LEFT JOIN pg.fact_row_context c ON c.id  = o.context_id
    WHERE o.ingest_run_id = 'XXXX-...'
) TO 'exports/co_public_drinking_water/year=2024/part-0001.parquet'
   (FORMAT PARQUET, PARTITION_BY (year));
```

Each source has its own export SQL that declares which `attributes` keys to flatten into
columns. This is declared in `sources.toml` under an `attributes` list (see below) and
used to generate the `c.attributes->>'key' AS key` projection at export time.

### Cross-source joins in DuckDB

`dim_location` and `dim_time` are the join spine across all sources. DuckDB reads the
shared dimension Parquet files alongside source-specific fact Parquet:

```sql
-- TTHM concentration vs PM2.5 by county, 2020
SELECT
    w.fips_code,
    w.county,
    w.metric_value  AS tthm_avg,
    a.metric_value  AS pm25_avg
FROM 'exports/co_public_drinking_water/**/*.parquet' w
JOIN 'exports/epa_air_quality/**/*.parquet'          a
  ON w.fips_code = a.fips_code AND w.year = a.year
WHERE w.metric_name = 'average_concentration'
  AND a.metric_name = 'pm25_daily_mean'
  AND w.year = 2020;
```

No Postgres involvement at query time.

### Partitioning strategy

```
exports/
  co_public_drinking_water/
    year=2012/part-0001.parquet
    year=2013/part-0001.parquet
  epa_air_quality/
    year=2020/part-0001.parquet
  dims/
    locations.parquet      ← exported once, refreshed when dim_location changes
    times.parquet          ← exported once, refreshed when dim_time changes
```

Dimension Parquet files are small and shared across all source queries. They are
re-exported whenever a dimension row is added (or on a schedule).

### Iteration path to Arrow-native pipeline (Option C)

The post-ingest export (Option A) is designed so the abstraction boundary can shift
without a rewrite:

- The `batched_channel_sink` flush function currently takes `Vec<NewObservation>` and
  writes to Postgres. In a future Arrow-native pipeline, this function would instead
  produce `arrow::RecordBatch` values, write row groups directly to Parquet, and
  optionally also bulk-load into Postgres via COPY.
- The `on_flush` callback already exists for side-effects. A dual-write approach
  (Option B) can be layered on by changing only the flush closure in `process_rows`
  without touching `batched_channel_sink` itself.
- The export SQL (flatten + join) is source-configured and DuckDB-executed — it does
  not need to move into Rust to support Arrow-native output. The query shape stays the
  same regardless of whether the source data comes from Postgres or Arrow IPC.

**Trigger for moving to Option C:** when post-ingest export latency becomes
unacceptable, or when Parquet is needed mid-ingest for very large files. Until then,
Option A is sufficient.

## Layer 1: Postgres Schema

### Migration: `004_row_context.sql`

```sql
CREATE TABLE fact_row_context (
    id          BIGSERIAL PRIMARY KEY,
    source_name TEXT        NOT NULL,
    attributes  JSONB       NOT NULL DEFAULT '{}',
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX fact_row_context_source_idx ON fact_row_context (source_name);
CREATE INDEX fact_row_context_attrs_idx  ON fact_row_context USING GIN (attributes);

ALTER TABLE fact_observations
    ADD COLUMN context_id BIGINT REFERENCES fact_row_context (id) ON DELETE SET NULL;
```

- `context_id` is nullable — existing rows are unaffected, no backfill required.
- GIN index on `attributes` supports ad-hoc operational queries
  (e.g. `attributes @> '{"analyte_name": "TTHM"}'`) without going through DuckDB.
- `ON DELETE SET NULL` — consistent with the existing `raw_import_id` pattern.

### What goes in `attributes`

All non-numeric, non-dimension fields from the CSV row are collected automatically.
No per-source configuration is required for ingest.

```
CSV row fields
  ├── Dimension fields      → dim_location / dim_time (unchanged)
  ├── Numeric fields        → fact_observations.metric_value (unchanged)
  └── String/other fields   → fact_row_context.attributes  ← new
```

Example for the drinking water source:
```json
{
  "analyte_name":            "TTHM (total trihalomethanes)",
  "units":                   "ug/L",
  "time_period":             "Year",
  "pws_name":                "Bennett Town Of",
  "pws_primary_source_code": "GW",
  "pws_id_number":           "CO0101020"
}
```

### `sources.toml` — `attributes` list

The `attributes` list is not used during ingest (all string fields go into JSONB
automatically) but is used by the export step to know which keys to flatten into
named Parquet columns:

```toml
[[ingest.sources]]
name       = "co_public_drinking_water"
attributes = ["analyte_name", "units", "time_period", "pws_name", "pws_primary_source_code"]

[[ingest.sources]]
name       = "epa_air_quality"
attributes = ["pollutant", "units", "measurement_method", "monitor_type"]
```

Future sources declare their own list. Sources with no `attributes` list get a
Parquet file with only the core metric + dimension columns.

### Write path (per CSV row)

```
1. INSERT INTO fact_row_context (source_name, attributes)
   VALUES ($1, $2)
   RETURNING id                       → context_id

2. UNNEST bulk insert into fact_observations
   including context_id on every row
```

Step 1 is a single-row insert with RETURNING — no batch needed since the ID must
be known before step 2. This follows the same pattern as `dim_location` and
`dim_time` upserts and adds one extra DB round-trip per CSV row.

### Cleanup on failure

`delete_by_ingest_run` cleans up `fact_observations`. Context rows become
unreferenced when their observations are deleted. Orphaned context rows are
accepted as cheap audit data — they are small and bounded.

A manual cleanup query if needed:
```sql
DELETE FROM fact_row_context
WHERE inserted_at < NOW() - INTERVAL '7 days'
  AND id NOT IN (
      SELECT DISTINCT context_id
      FROM fact_observations
      WHERE context_id IS NOT NULL
  );
```

## Models (`core/src/models/`)

### New: `row_context.rs`

```rust
pub struct NewRowContext {
    pub source_name: String,
    pub attributes:  serde_json::Value,
}

pub struct RowContext {
    pub id:          i64,
    pub source_name: String,
    pub attributes:  serde_json::Value,
    pub inserted_at: DateTime<Utc>,
}
```

### Updated: `observation.rs`

Add `context_id: Option<i64>` to both `Observation` and `NewObservation`.

## Repositories (`core/src/repositories/`)

### New: `row_context.rs`

```rust
pub async fn create(&self, ctx: NewRowContext) -> Result<i64>
// INSERT INTO fact_row_context (source_name, attributes)
// VALUES ($1, $2) RETURNING id
```

### Updated: `observation.rs`

`bulk_create` extended with `context_id` in the UNNEST insert. No other changes.

## Pipeline (`ingest/src/pipeline/`)

### New: `context.rs`

```rust
/// Collect all non-numeric, non-dimension fields from the transformed row into
/// a JSONB blob for fact_row_context.
pub(super) fn extract_context(
    map: &HashMap<String, FieldValue>,
) -> serde_json::Value { ... }
```

### Updated: `record.rs`

`process_record` gains one step before metric extraction:

```
1. row_to_canonical
2. apply_all_transforms + with_passthrough
3. resolve_location_id + resolve_time_id   (tokio::join!, unchanged)
4. extract_context → INSERT fact_row_context → context_id   ← new
5. extract_metrics
6. build_observations (receives context_id)
```

### Updated: `mod.rs` — `run()`

After `process_rows` succeeds, call the export step:

```rust
let total = self.process_rows(...).await?;
self.export_run(ingest_run_id, source).await?;   // ← new
Ok(total)
```

`export_run` invokes DuckDB (via the `duckdb` crate or a subprocess) to execute
the per-source export SQL scoped to `ingest_run_id`, writing Parquet to the
configured export path. Export failure is logged but does not fail the ingest —
the Postgres data is already committed and the export can be retried.

### Updated: `observations.rs`

`build_observations` receives `context_id: Option<i64>` and sets it on each
`NewObservation`.

## Tradeoffs

| Concern | This design | Inline JSONB | Arrow-native (future) |
|---------|-------------|--------------|----------------------|
| Postgres storage | One blob per CSV row | One blob per observation | N/A |
| Write complexity | +1 insert per row | One UNNEST | Dual-write to Parquet + Postgres |
| Parquet availability | Seconds after ingest | Same | During ingest (mid-file) |
| Atomicity | Postgres-first, Parquet on success | Same | Complex — no ingest_run_id equivalent |
| Analytics query | DuckDB reads flat Parquet | Same | Same |
| Schema flexibility | Unlimited JSONB keys | Same | Requires Arrow schema per source |
| Export logic location | DuckDB SQL (easy to iterate) | Same | Rust (Arrow RecordBatch) |
| Option C migration cost | Change flush closure only | Same | — |

## Out of Scope

- Enforcing which keys are present in `attributes` per source.
- Promoting frequently-queried `attributes` keys to dedicated Postgres columns.
- Backfilling `context_id` on existing `fact_observations` rows.
- Dimension Parquet refresh scheduling.
- Retry mechanism for failed export runs.
- Arrow-native dual-write pipeline (Option C) — see iteration path above.
