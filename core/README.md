# state-search-core

Shared library used by both the API server and the ingest CLI. Contains models, repositories, DB pool management, configuration loading, and the common error type.

---

## Module overview

| Module | Purpose |
|--------|---------|
| `config` | Load and merge `config/default.toml` + `APP__*` env vars into `AppConfig` |
| `db` | `Db` newtype wrapping `PgPool`; created from `AppConfig` |
| `error` | `CoreError` enum and `Result<T>` alias |
| `models/` | Plain Rust structs that mirror DB rows (`FromRow`, `Serialize`, `Deserialize`) |
| `repositories/` | All database access — one module per table or logical group |

---

## Error handling

`CoreError` is the single error type for the whole library:

```rust
pub enum CoreError {
    Database(sqlx::Error),       // → HTTP 500
    Migration(MigrateError),     // → HTTP 500
    NotFound(String),            // → HTTP 404
    InvalidInput(String),        // → HTTP 422
    Config(config::ConfigError), // → HTTP 500
    Unexpected(anyhow::Error),   // → HTTP 500
}
```

In the API, `ApiError` wraps `CoreError` and maps variants to HTTP status codes. Handlers return `ApiResult<T>` (a type alias for `Result<T, ApiError>`). The `?` operator automatically converts `CoreError → ApiError` via the `From` impl.

```
Repository → CoreError
                ↓ From<CoreError>
           ApiError → IntoResponse → HTTP status + { "error": "..." }
```

---

## Repository pattern

All database access lives in `repositories/`. Repositories are plain structs that hold a reference to the pool:

```rust
let repo = ImportRepository::new(&state.db);
let source = repo.find_source_by_name("my_source").await?;
```

- They take `&PgPool` (not `&mut`), so they can be created on the fly in handlers.
- They use `sqlx::query!` / `sqlx::query_as!` with runtime-checked SQL (not compile-time macros — see [docs/decisions/003-no-compiletime-sqlx.md](../docs/decisions/003-no-compiletime-sqlx.md)).
- Each repository owns its SQL. There is no shared query builder.

### Available repositories

| Repository | Table(s) | Key operations |
|-----------|---------|----------------|
| `ImportRepository` | `import_sources`, `raw_imports` | `create_source`, `list_sources`, `find_source_by_name`, `find_or_create_source`, `create_raw_import` |
| `LocationRepository` | `dim_location` | `upsert` |
| `TimeRepository` | `dim_time` | `upsert` |
| `NormalizedImportRepository` | `normalized_imports` | `bulk_create`, `delete_by_ingest_run` |
| `ImportSchemaRepository` | `import_schema` | schema field upserts |

---

## Models

Models in `models/` are plain structs that derive `FromRow` (for DB reads) and `Serialize`/`Deserialize` (for JSON). Insert payloads are separate `New*` structs that only contain the fields the caller provides.

### `ImportSource`
Registry entry for a CSV data source.

| Field | Type | Notes |
|-------|------|-------|
| `id` | `i32` | Serial PK |
| `name` | `String` | Unique |
| `description` | `Option<String>` | |
| `created_at` | `DateTime<Utc>` | |

### `RawImport`
One verbatim CSV row.

| Field | Type | Notes |
|-------|------|-------|
| `id` | `i64` | BigSerial PK |
| `source_id` | `Option<i32>` | FK → `import_sources` |
| `source_file` | `Option<String>` | Path the file was read from |
| `imported_at` | `DateTime<Utc>` | |
| `raw_data` | `Value` | Original row as `{ column: value }` JSON |

### `NormalizedImport`
Post-processing result: dimension FKs + remaining fields as JSONB.

| Field | Type | Notes |
|-------|------|-------|
| `id` | `i64` | BigSerial PK |
| `raw_import_id` | `Option<i64>` | FK → `raw_imports` |
| `location_id` | `Option<i64>` | FK → `dim_location` |
| `time_id` | `Option<i64>` | FK → `dim_time` |
| `source_name` | `String` | Denormalized for query convenience |
| `ingest_run_id` | `Uuid` | Groups rows from the same ingest run |
| `normalized_data` | `Value` | Non-dimension fields as `{ field: value }` JSON |
| `inserted_at` | `DateTime<Utc>` | |

---

## Configuration

`AppConfig` is loaded via the `config` crate: reads `config/default.toml` first, then applies any `APP__*` environment variable overrides (prefix `APP`, separator `__`).

```rust
let cfg = AppConfig::load()?; // reads config/ directory + env
let db  = Db::connect(&cfg.database).await?;
```

See [CLAUDE.md](../CLAUDE.md#configuration) for the full config reference.
