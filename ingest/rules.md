# Ingest Pipeline — Design Rules

## 1. Producer/Consumer Pattern

All streaming batch work uses `batched_channel_sink` from `crate::stream`. Do not inline the channel + `tokio::join!` pattern again — that's what the abstraction is for.

```
stream of futures
    → buffer_unordered (concurrency limit)
    → bounded mpsc channel  ← backpressure lives here
    → batching consumer
    → flush function (e.g. bulk_create)
    → on_flush callback (logging, metrics)
```

## 2. No DB Writes Inside the Producer

The producer side (`buffer_unordered`) must not await DB writes. Doing so blocks the internal `FuturesUnordered` from polling other in-flight futures, which kills concurrency. DB writes belong exclusively in `flush`.

**OK:** resolve dimensions (location, time) — these are needed to build the output
**Not OK:** insert observations, write to fact tables

## 3. Memory Is Bounded by the Channel

Peak memory is determined by:

```
(channel_cap × avg_outputs_per_item) + batch_size
```

`channel_cap = ROW_CONCURRENCY * 4` and `batch_size = OBS_BATCH_SIZE` are the two knobs. Do not collect an entire file's outputs into a `Vec` before writing — that's unbounded.

## 4. Concurrency Is Bounded by the DB Pool

`ROW_CONCURRENCY = 8` is set below the DB pool max (10). Each row in the producer may make 2–3 concurrent DB calls (raw import insert + location upsert + time upsert via `tokio::join!`). Raising concurrency past the pool size will cause connection starvation.

## 5. Error Recovery via `ingest_run_id`

Every observation written during a run is tagged with its `ingest_run_id`. If the run fails, `ObservationRepository::delete_by_ingest_run` cleans up any partial data. This replaces a file-level transaction, which would hold a long-lived lock on large files.

**Rule:** every `NewObservation` must carry the run's `ingest_run_id`. Never insert observations without one.

## 6. Location Cache

`LocationCache` is an `Arc<RwLock<HashMap<String, i64>>>` scoped to a single ingest run. It avoids repeated upsert round-trips for rows that share the same location (common in state-level data).

- **Read lock first**: check cache before hitting the DB.
- **Write lock after upsert**: store the resolved ID.
- Cache key is built from all six location fields via `location_cache_key()`. Float fields use `.to_bits()` to avoid the `Hash` limitation on `f64`.

## 7. Module Boundaries

```
stream.rs           — generic async utility; no domain types
pipeline/
  mod.rs            — orchestration only; wires submodules together
  record.rs         — per-row transform + dimension resolution call
  dimensions.rs     — location/time DB upserts + location cache
  observations.rs   — metric extraction + NewObservation construction
  row.rs            — CSV parsing, typed field extractors, path utilities
```

Cross-module visibility uses `pub(super)`. Nothing in `pipeline/` is public except `IngestPipeline` and `IngestError` in `mod.rs`.

## 8. Field Map Is Applied Before Dimension Extraction

The `field_map` from `sources.toml` renames source columns to canonical names. Dimension extraction (location, time) and metric extraction all operate on the *post-transform* canonical map, never the raw CSV headers.

Canonical location fields: `county`, `country`, `zip_code`, `fips_code`, `latitude`, `longitude`
Canonical time fields: `year`, `quarter`, `month`, `day`
Everything else is treated as a metric.
