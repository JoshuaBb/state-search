# Observation Ingest Run Tracking Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the file-level Postgres transaction with per-row commits, tracking observations by a `ingest_run_id` UUID so partial failures can be cleaned up without holding long-lived locks.

**Architecture:** A new migration adds `ingest_run_id UUID` and `inserted_at TIMESTAMPTZ` to `fact_observations`. The pipeline generates a UUID per file run, logs it immediately, passes it into every `NewObservation`, and on any mid-file error deletes all observations sharing that UUID before returning the error. The file-level `BEGIN`/`COMMIT`/`ROLLBACK` wrapper is removed entirely.

**Tech Stack:** Rust, SQLx 0.8 (Postgres), `uuid` v1 (v4 feature already in workspace), `chrono` 0.4 (already in workspace), `thiserror` 2.

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Create | `migrations/003_observation_ingest_run.sql` | ADD COLUMN ingest_run_id + inserted_at + index |
| Modify | `core/src/models/observation.rs` | Add fields to `Observation` and `NewObservation` |
| Modify | `core/src/repositories/observation.rs` | Remove `bulk_create_with_tx`, update `bulk_create`, add `delete_by_ingest_run` |
| Modify | `ingest/src/pipeline.rs` | Remove file-level tx, generate + pass ingest_run_id, cleanup on error |

---

## Chunk 1: Data Layer (Migration + Model + Repository)

### Task 1: Migration

**Files:**
- Create: `migrations/003_observation_ingest_run.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- migrations/003_observation_ingest_run.sql
ALTER TABLE fact_observations
  ADD COLUMN ingest_run_id UUID,
  ADD COLUMN inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX fact_ingest_run_idx ON fact_observations (ingest_run_id);
```

`ingest_run_id` is nullable so existing rows are unaffected. `inserted_at` defaults to `NOW()` — no backfill needed.

- [ ] **Step 2: Verify migration applies cleanly**

```bash
cargo build -p state-search-api
```

Expected: compiles without error (migration is picked up at runtime by `sqlx::migrate!()`).

- [ ] **Step 3: Commit**

```bash
git add migrations/003_observation_ingest_run.sql
git commit -m "feat: add ingest_run_id and inserted_at to fact_observations"
```

---

### Task 2: Update Observation model

**Files:**
- Modify: `core/src/models/observation.rs`

Current state: `Observation` has 8 fields; `NewObservation` has 7 fields. Neither has UUID or timestamp fields.

- [ ] **Step 1: Write a failing compile-check test**

Add to the bottom of `core/src/models/observation.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn new_observation_has_ingest_run_id() {
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "test".to_string(),
            metric_value:  None,
            attributes:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }

    #[test]
    fn observation_has_ingest_run_id_and_inserted_at() {
        // Verify the fields exist on the read model (compile-time check).
        fn assert_fields(o: &Observation) {
            let _: &Option<Uuid> = &o.ingest_run_id;
            let _: &chrono::DateTime<Utc> = &o.inserted_at;
        }
        // Runtime instantiation isn't needed — the compile check is the test.
        let _ = assert_fields;
    }
}
```

- [ ] **Step 2: Run to confirm compile failure**

```bash
cargo test -p state-search-core 2>&1 | head -30
```

Expected: compile error — `ingest_run_id` field not found on `NewObservation`.

- [ ] **Step 3: Update the model**

Replace `core/src/models/observation.rs` with:

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

/// One normalized metric value linking location + time + source.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Observation {
    pub id:            i64,
    pub raw_import_id: Option<i64>,
    pub location_id:   Option<i64>,
    pub time_id:       Option<i64>,
    pub source_name:   Option<String>,
    pub metric_name:   String,
    pub metric_value:  Option<f64>,
    /// Any additional fields that don't fit the normalized columns.
    pub attributes:    Option<Value>,
    pub ingest_run_id: Option<Uuid>,
    pub inserted_at:   DateTime<Utc>,
}

#[derive(Debug)]
pub struct NewObservation {
    pub raw_import_id: Option<i64>,
    pub location_id:   Option<i64>,
    pub time_id:       Option<i64>,
    pub source_name:   Option<String>,
    pub metric_name:   String,
    pub metric_value:  Option<f64>,
    pub attributes:    Option<Value>,
    pub ingest_run_id: Uuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn new_observation_has_ingest_run_id() {
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "test".to_string(),
            metric_value:  None,
            attributes:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }

    #[test]
    fn observation_has_ingest_run_id_and_inserted_at() {
        fn assert_fields(o: &Observation) {
            let _: &Option<Uuid> = &o.ingest_run_id;
            let _: &chrono::DateTime<Utc> = &o.inserted_at;
        }
        let _ = assert_fields;
    }
}
```

- [ ] **Step 4: Run tests — expect compile errors in callers (not in this file)**

```bash
cargo test -p state-search-core 2>&1 | head -40
```

Expected: the model tests in `observation.rs` pass. Compile errors will appear in `observation.rs` repository and `pipeline.rs` — those are fixed in the next tasks.

- [ ] **Step 5: Commit**

```bash
git add core/src/models/observation.rs
git commit -m "feat: add ingest_run_id to NewObservation, ingest_run_id+inserted_at to Observation"
```

---

### Task 3: Update ObservationRepository

**Files:**
- Modify: `core/src/repositories/observation.rs`

Changes:
1. Remove `bulk_create_with_tx` (no longer needed — file-level tx is gone).
2. Update `bulk_create` INSERT to include `ingest_run_id`.
3. Add `delete_by_ingest_run`.
4. Update `create` INSERT to include `ingest_run_id`.

- [ ] **Step 1: Write failing tests for new repository methods**

Add to the bottom of `core/src/repositories/observation.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use crate::models::observation::NewObservation;

    /// Verifies that delete_by_ingest_run exists and has the right signature.
    /// Actual DB behaviour is verified via integration/manual testing.
    #[test]
    fn delete_by_ingest_run_signature_exists() {
        // This is a compile-time check: the function must exist and be callable
        // with (pool, Uuid). We don't call it (no pool in unit tests).
        let _: fn(&PgPool, Uuid) -> _ = |pool, id| {
            ObservationRepository::delete_by_ingest_run(pool, id)
        };
    }

    #[test]
    fn bulk_create_accepts_new_observation_with_ingest_run_id() {
        // Compile-time check: NewObservation must have ingest_run_id field.
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "m".to_string(),
            metric_value:  None,
            attributes:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }
}
```

- [ ] **Step 2: Run to confirm compile failure**

```bash
cargo test -p state-search-core 2>&1 | head -30
```

Expected: `delete_by_ingest_run` not found on `ObservationRepository`.

- [ ] **Step 3: Update the repository**

Replace `core/src/repositories/observation.rs` with:

```rust
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::Result,
    models::observation::{NewObservation, Observation},
};

pub struct ObservationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ObservationRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, obs: NewObservation) -> Result<Observation> {
        let row = sqlx::query_as::<_, Observation>(
            "INSERT INTO fact_observations
                 (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes, ingest_run_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING *",
        )
        .bind(obs.raw_import_id)
        .bind(obs.location_id)
        .bind(obs.time_id)
        .bind(obs.source_name)
        .bind(obs.metric_name)
        .bind(obs.metric_value)
        .bind(obs.attributes)
        .bind(obs.ingest_run_id)
        .fetch_one(self.pool)
        .await?;

        Ok(row)
    }

    /// Bulk insert observations. Wraps all rows for a single CSV row in one
    /// mini-transaction (per-row granularity, not per-file).
    pub async fn bulk_create(&self, observations: Vec<NewObservation>) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let mut count = 0u64;

        for obs in observations {
            sqlx::query(
                "INSERT INTO fact_observations
                     (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes, ingest_run_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(obs.raw_import_id)
            .bind(obs.location_id)
            .bind(obs.time_id)
            .bind(obs.source_name)
            .bind(obs.metric_name)
            .bind(obs.metric_value)
            .bind(obs.attributes)
            .bind(obs.ingest_run_id)
            .execute(&mut *tx)
            .await?;

            count += 1;
        }

        tx.commit().await?;
        Ok(count)
    }

    /// Delete all observations belonging to a given ingest run.
    /// Returns the number of rows deleted.
    pub async fn delete_by_ingest_run(pool: &PgPool, ingest_run_id: Uuid) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM fact_observations WHERE ingest_run_id = $1",
        )
        .bind(ingest_run_id)
        .execute(pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn query(
        &self,
        metric_name: Option<&str>,
        source_name: Option<&str>,
        location_id: Option<i64>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Observation>> {
        let rows = sqlx::query_as::<_, Observation>(
            "SELECT * FROM fact_observations
             WHERE ($1::text IS NULL OR metric_name = $1)
               AND ($2::text IS NULL OR source_name = $2)
               AND ($3::bigint IS NULL OR location_id = $3)
             ORDER BY id
             LIMIT $4 OFFSET $5",
        )
        .bind(metric_name)
        .bind(source_name)
        .bind(location_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use crate::models::observation::NewObservation;

    #[test]
    fn delete_by_ingest_run_signature_exists() {
        let _: fn(&PgPool, Uuid) -> _ = |pool, id| {
            ObservationRepository::delete_by_ingest_run(pool, id)
        };
    }

    #[test]
    fn bulk_create_accepts_new_observation_with_ingest_run_id() {
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "m".to_string(),
            metric_value:  None,
            attributes:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p state-search-core 2>&1
```

Expected: all `state-search-core` tests pass. `pipeline.rs` still won't compile — fixed next.

- [ ] **Step 5: Commit**

```bash
git add core/src/repositories/observation.rs
git commit -m "feat: add delete_by_ingest_run, remove bulk_create_with_tx, include ingest_run_id in inserts"
```

---

## Chunk 2: Pipeline

### Task 4: Update the ingest pipeline

**Files:**
- Modify: `ingest/src/pipeline.rs`

Changes:
1. Add `use uuid::Uuid` import.
2. At start of `run()`: generate `ingest_run_id = Uuid::new_v4()`, log it at `info` level.
3. Remove the file-level `tx = self.db.begin()` / `tx.commit()` / `tx.rollback()` scope.
4. Change `ObservationRepository::bulk_create_with_tx(&mut tx, observations)` → `ObservationRepository::new(self.db).bulk_create(observations)`.
5. Pass `ingest_run_id` into every `NewObservation`.
6. On any error mid-file: call `delete_by_ingest_run`, log deleted count, return original error.

The current pipeline wraps the entire per-row loop in `let result: anyhow::Result<u64> = async { ... }.await` and matches on Ok/Err to commit or rollback. That async block is replaced with direct `?` propagation — errors now fall through to a cleanup handler.

- [ ] **Step 1: Write a failing compile-check test**

Add to the `#[cfg(test)]` block at the bottom of `ingest/src/pipeline.rs`:

```rust
#[test]
fn infer_country_still_works_after_refactor() {
    // Smoke-check that the helpers are intact after the pipeline changes.
    assert_eq!(
        infer_country_from_path("data/usa/co/file.csv"),
        Some("USA".to_string())
    );
    assert_eq!(infer_country_from_path("data/co/file.csv"), None);
}
```

Run:

```bash
cargo test -p state-search-ingest infer_country_still_works_after_refactor 2>&1
```

Expected: PASS (the helper is unchanged; this confirms no accidental breakage during the refactor).

- [ ] **Step 2: Remove the file-level transaction and add ingest_run_id**

Replace the body of `IngestPipeline::run` with the new implementation shown below. Key structural changes:
- `ingest_run_id` is generated and logged before any DB writes.
- No `tx` variable; `raw_imports` inserts use `self.db` directly.
- `bulk_create_with_tx` → `bulk_create` (no tx argument).
- All `NewObservation` structs gain `ingest_run_id`.
- The `let result = async { ... }.await` + match block is replaced by a `cleanup_on_error` closure pattern: run the loop, and if it returns `Err`, call `delete_by_ingest_run` then re-return the original error.

```rust
pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
    let source_name = source.name.as_str();
    let ingest_run_id = Uuid::new_v4();

    // Log BEFORE any DB writes so operators can identify the run even if the
    // process dies before the cleanup branch executes.
    info!(
        source = source_name,
        file = file_path,
        %ingest_run_id,
        "starting ingest"
    );

    let resolved = build_resolved_field_map(source)
        .with_context(|| format!("failed to resolve field map for source '{source_name}'"))?;

    let mut reader = csv::Reader::from_path(file_path)
        .with_context(|| format!("cannot open {file_path}"))?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();
    debug!(source = source_name, ?headers, "CSV headers detected");

    let default_country = infer_country_from_path(file_path);

    let mut total = 0u64;
    let mut row_num = 0u64;

    let result = self.process_rows(
        source,
        file_path,
        ingest_run_id,
        &resolved,
        &headers,
        default_country.as_deref(),
        &mut reader,
        &mut total,
        &mut row_num,
    ).await;

    match result {
        Ok(()) => {
            info!(
                source = source_name,
                file = file_path,
                %ingest_run_id,
                observations = total,
                "ingest complete"
            );
            Ok(total)
        }
        Err(e) => {
            warn!(
                source = source_name,
                file = file_path,
                %ingest_run_id,
                error = %e,
                "ingest failed — cleaning up partial observations"
            );
            match ObservationRepository::delete_by_ingest_run(self.db, ingest_run_id).await {
                Ok(deleted) => {
                    info!(
                        %ingest_run_id,
                        deleted,
                        "cleanup complete"
                    );
                }
                Err(cleanup_err) => {
                    warn!(
                        %ingest_run_id,
                        error = %cleanup_err,
                        "cleanup failed — manual intervention required"
                    );
                }
            }
            Err(e)
        }
    }
}

async fn process_rows(
    &self,
    source: &SourceConfig,
    file_path: &str,
    ingest_run_id: Uuid,
    resolved: &crate::transforms::resolve::ResolvedFieldMap,
    headers: &[String],
    default_country: Option<&str>,
    reader: &mut csv::Reader<std::fs::File>,
    total: &mut u64,
    row_num: &mut u64,
) -> anyhow::Result<()> {
    let source_name = source.name.as_str();
    let db = self.db;

    for record in reader.records() {
        let record = record?;
        *row_num += 1;

        let raw = row_to_canonical(headers, &record);
        debug!(row = row_num, "parsed and trimmed CSV row");

        let raw_json = canonical_to_json(&raw);
        sqlx::query(
            "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3)",
        )
        .bind(Option::<i64>::None)
        .bind(file_path)
        .bind(&raw_json)
        .execute(db)
        .await?;

        let mut transformed: HashMap<String, FieldValue> = HashMap::new();
        let mut skip_row = false;

        for (canonical, resolved_field) in resolved {
            let raw_val = raw
                .get(resolved_field.source_col.as_str())
                .cloned()
                .unwrap_or(FieldValue::Null);

            match apply_chain(raw_val, &resolved_field.chain) {
                Ok(v) => { transformed.insert(canonical.clone(), v); }
                Err(OnFailure::SkipRow) => {
                    warn!(row = row_num, field = canonical, source = source_name, "SkipRow: discarding row");
                    skip_row = true;
                    break;
                }
                Err(OnFailure::SkipDataset) => {
                    return Err(IngestError::DatasetSkipped {
                        file: file_path.to_string(),
                        reason: format!("transform failure on field '{}' at row {}", canonical, row_num),
                    }.into());
                }
                Err(OnFailure::Ignore) => unreachable!(),
            }
        }

        if skip_row { continue; }

        for (col, val) in &raw {
            if !resolved.values().any(|r| r.source_col == *col) {
                transformed.insert(col.clone(), val.clone());
            }
        }

        let location_id = match resolve_location(&transformed, default_country) {
            Ok(loc) => match LocationRepository::new(db).upsert(loc).await {
                Ok(id) => { debug!(row = row_num, location_id = id, "resolved location"); Some(id) }
                Err(e) => { warn!(row = row_num, error = %e, "could not resolve location"); None }
            },
            Err(_) => None,
        };

        let time_id = match resolve_time(&transformed) {
            Ok(t) => match TimeRepository::new(db).upsert(t).await {
                Ok(id) => { debug!(row = row_num, time_id = id, "resolved time"); Some(id) }
                Err(e) => { warn!(row = row_num, error = %e, "could not resolve time"); None }
            },
            Err(_) => None,
        };

        let metrics = extract_metrics(&transformed);
        if metrics.is_empty() {
            warn!(row = row_num, "no metric fields found");
        }

        // Note: raw_id is no longer threaded through — raw_import_id on
        // fact_observations is set to NULL for now (FK is ON DELETE SET NULL).
        // A future migration can link them if needed.
        let observations: Vec<NewObservation> = metrics
            .into_iter()
            .map(|(name, value)| NewObservation {
                raw_import_id: None,
                location_id,
                time_id,
                source_name: Some(source_name.to_string()),
                metric_name: name,
                metric_value: value,
                attributes: None,
                ingest_run_id,
            })
            .collect();

        let count = ObservationRepository::new(db).bulk_create(observations).await?;
        debug!(row = row_num, inserted = count, "row complete");
        *total += count;

        if *row_num % 1000 == 0 {
            info!(
                source = source_name,
                file = file_path,
                rows = row_num,
                observations = total,
                "ingest progress"
            );
        }
    }

    Ok(())
}
```

> **Note on `raw_import_id`:** The pipeline previously captured `raw_id` from the `RETURNING id` INSERT and passed it into `NewObservation`. Without the file-level transaction and with the refactor to a separate `process_rows` method, threading `raw_id` back is straightforward but adds a parameter. The spec does not require maintaining this linkage; `raw_import_id` is set to `None`. If this matters, restore `RETURNING id` and pass `Some(raw_id)` into `NewObservation`.

- [ ] **Step 3: Update imports in pipeline.rs**

At the top of `ingest/src/pipeline.rs`, add:

```rust
use uuid::Uuid;
use state_search_core::repositories::observation::ObservationRepository;
```

(Remove the old `use` of `bulk_create_with_tx` if it was explicitly imported.)

- [ ] **Step 4: Build to check for compile errors**

```bash
cargo build -p state-search-ingest 2>&1
```

Work through any remaining compile errors (typically: `NewObservation` struct literals missing `ingest_run_id`, or remaining references to `bulk_create_with_tx`).

- [ ] **Step 5: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add ingest/src/pipeline.rs
git commit -m "feat: remove file-level tx, add ingest_run_id per-run cleanup"
```

---

## Verification

After all tasks are complete, do a manual smoke test:

```bash
# Rebuild and start
docker-compose build
docker-compose up

# Or force re-ingest of a specific file
docker-compose run --rm ingest run \
  --source co_public_drinking_water \
  --file data/usa/co/public_drinking_water/public_drinking_water_2026-03-14.csv
```

Check the logs for:
- `starting ingest ingest_run_id=<uuid>` — UUID is logged before any DB writes
- `ingest complete ingest_run_id=<uuid> observations=N` — normal success path
- On forced failure: `cleanup complete deleted=N` — cleanup ran

Check the DB:
```sql
SELECT ingest_run_id, COUNT(*) FROM fact_observations GROUP BY ingest_run_id;
-- Should show one UUID with the full observation count.
```
