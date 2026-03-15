# Observation Ingest Run Tracking

**Date:** 2026-03-14
**Status:** Approved

## Problem

The ingest pipeline wraps each CSV file in a single Postgres transaction. As file sizes grow, this causes excessive lock duration and risks OOM conditions on the database side. The transaction must be removed, but partial-ingest cleanup is still required on failure.

## Solution

Replace the file-level transaction with per-row (immediate) commits. Track which observations belong to a given ingest run via a `ingest_run_id` UUID column on `fact_observations`. On failure, delete all observations sharing that UUID.

## Design

### Migration: `003_observation_ingest_run.sql`

Add two columns to `fact_observations`:

```sql
ALTER TABLE fact_observations
  ADD COLUMN ingest_run_id UUID,
  ADD COLUMN inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX fact_ingest_run_idx ON fact_observations (ingest_run_id);
```

- `ingest_run_id` is nullable so existing rows are unaffected.
- `inserted_at` defaults to `NOW()` so no backfill is required.
- Index on `ingest_run_id` makes cleanup deletes fast.

### Models (`core/src/models/observation.rs`)

- `Observation`: add `ingest_run_id: Option<Uuid>` and `inserted_at: DateTime<Utc>`
- `NewObservation`: add `ingest_run_id: Uuid` (always set; new inserts always belong to a run)

### Repository (`core/src/repositories/observation.rs`)

- **Remove** `bulk_create_with_tx` (no longer needed).
- **Update** `bulk_create` to include `ingest_run_id` and `inserted_at` in the INSERT columns. The existing internal transaction inside `bulk_create` (one mini-transaction per call) is **retained** — this groups all observations for a single CSV row into one commit, which is the correct granularity.
- **Add** `delete_by_ingest_run(pool: &PgPool, ingest_run_id: Uuid) -> Result<u64>`:
  ```sql
  DELETE FROM fact_observations WHERE ingest_run_id = $1
  ```
  Returns the number of deleted rows for logging.

### Pipeline (`ingest/src/pipeline.rs`)

- **Remove** the file-level `tx = self.db.begin()` / `tx.commit()` / `tx.rollback()` scope.
- At the start of `run()`, generate `let ingest_run_id = Uuid::new_v4()` and **immediately log it** (before any DB writes) so that operators can identify and manually clean up a run even if the process dies before reaching the failure cleanup branch.
- Pass `ingest_run_id` into every `NewObservation` constructed during the file.
- On any mid-file error:
  1. Log the error and the `ingest_run_id`.
  2. Call `ObservationRepository::delete_by_ingest_run(&self.db, ingest_run_id).await`.
  3. Log how many rows were cleaned up.
  4. Return the original error.
- Dimension upserts (`dim_location`, `dim_time`) are already pool-level and are unchanged.
- `raw_imports` inserts are currently inside the file-level transaction. Once that transaction is removed, raw import rows will be committed immediately per row. On failure, the cleanup delete only covers `fact_observations`; orphaned `raw_imports` rows (no corresponding observations) will remain. This is an accepted trade-off — `raw_imports` is audit data, the FK from `fact_observations` to `raw_imports` is `ON DELETE SET NULL`, and no data integrity violation occurs.

### Already-Ingested Guard

The `already_ingested` check in `main.rs` queries `raw_imports` to skip files that have already been processed. Under the old transaction model, a failed run rolled back all `raw_imports` rows along with observations, leaving the file eligible for retry. Under per-row commits, a partial failure leaves `raw_imports` rows behind, and a subsequent `reload` or `run` (without `--file`) will silently skip the file.

**Handling:** After a partial failure, operators must use `--file <path>` to force re-ingest. The pipeline logs the `ingest_run_id` on failure to assist with manual recovery. This limitation is documented in the Out of Scope section below.

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| CSV parse error mid-file | Cleanup delete runs, error returned to caller |
| Transform `OnFailure::SkipRow` | Row skipped, ingest continues (no cleanup needed) |
| Transform `OnFailure::SkipDataset` | Cleanup delete runs, error returned |
| DB error on observation insert | Cleanup delete runs, error returned |
| Cleanup delete itself fails | Log the failure + `ingest_run_id` for manual recovery, return original error |

## Out of Scope

- Resuming a failed ingest from the last successful row.
- Batching commits (N rows per commit) — can be layered on later if needed.
- Backfilling `ingest_run_id` on existing rows.
- Cleaning up orphaned `raw_imports` rows left by partial failures.
- Updating the `already_ingested` guard to detect partial failures automatically — callers must use `run --source X --file Y` to retry after a partial failure. The `reload` subcommand is equally affected: files that partially failed will be skipped on the next `reload` run.
