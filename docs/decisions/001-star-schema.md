# ADR 001 — Normalized imports over per-metric rows

**Status:** Accepted (supersedes earlier per-metric design from migration 005)

---

## Context

The original schema stored one row in `fact_observations` per metric value per CSV row. A CSV row with 10 metric columns would produce 10 observation rows. This is a classic star-schema approach that makes per-metric queries simple (`WHERE metric_name = 'unemployment_rate'`) but has several costs:

- Row counts blow up for wide CSVs (100 columns × 1M rows = 100M fact rows).
- Querying a full row requires aggregating across metric names.
- Adding a new metric column to a CSV requires no schema change, but query patterns change.

---

## Decision

Store one `normalized_imports` row per CSV row, with dimension FKs (`location_id`, `time_id`) resolved and all non-dimension fields kept as a JSONB blob in `normalized_data`.

```
normalized_imports
  id
  location_id  → dim_location
  time_id      → dim_time
  source_name
  ingest_run_id
  normalized_data  JSONB  { "unemployment_rate": "4.2", "labor_force": "19200000" }
```

The `import_schema` table records the field names and types for each source so the UI and API know what keys to expect in `normalized_data`.

---

## Consequences

**Good:**
- Row count matches CSV row count — no explosion.
- Adding metric columns to a CSV requires no schema change.
- Full row is available in one query (no aggregation).
- GIN index on `normalized_data` supports key-based filtering.

**Bad:**
- Per-metric range queries (`WHERE metric_value > 5`) require JSONB operators (`normalized_data->>'unemployment_rate'`), which are less ergonomic than a simple numeric column.
- Type safety is weaker — all values are stored as JSON strings; casting is the caller's responsibility.
- `import_schema` must be kept in sync with the actual CSV columns.
