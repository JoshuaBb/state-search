# ADR 002 — ingest_run_id for partial-failure recovery

**Status:** Accepted

---

## Context

Ingesting a large CSV file takes time. If the process crashes mid-file, some rows will have been written to the database and some won't. We need a way to clean up the partial write without corrupting data from previous successful runs.

The obvious solution is a database transaction wrapping the entire file. However, large files produce tens of thousands of rows, and a long-lived transaction holds locks that can block other operations and consume significant resources.

---

## Decision

Every `normalized_imports` row written during a single ingest run is tagged with the same `ingest_run_id` UUID, generated at the start of the run.

```sql
ALTER TABLE normalized_imports ADD COLUMN ingest_run_id UUID NOT NULL;
CREATE INDEX norm_imports_run_idx ON normalized_imports (ingest_run_id);
```

If the run fails, a cleanup query deletes all partial rows:

```sql
DELETE FROM normalized_imports WHERE ingest_run_id = $1;
```

The `NormalizedImportRepository::delete_by_ingest_run` method performs this cleanup.

**Rule:** every `NewNormalizedImport` must carry the run's `ingest_run_id`. The ingest pipeline enforces this at compile time — there is no optional field.

---

## Consequences

**Good:**
- No long-lived transaction; rows are committed in small batches as the pipeline runs.
- Cleanup is a single indexed DELETE, not a full-table scan.
- The run ID is useful for auditing and debugging (which rows came from which run).

**Bad:**
- Between a crash and cleanup, the DB may contain partial data for the failed run.
- The cleanup must be triggered explicitly (the pipeline does this on startup if it detects an incomplete run).
- Requires that callers always provide `ingest_run_id` — there is no default.
