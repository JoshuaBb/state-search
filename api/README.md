# state-search-api

Axum HTTP server. Serves the JSON REST API under `/api/*` and the SvelteKit SPA static files from `web/dist/` for all other paths.

---

## Starting the server

```bash
cargo run -p state-search-api
```

Migrations run automatically on startup via `sqlx::migrate!()`. The server listens on `0.0.0.0:3000` by default — override with `APP__SERVER__PORT`.

---

## Routes

### Health

#### `GET /api/health`

Returns `200 OK` when the server is up.

```json
{ "status": "ok" }
```

---

### Sources

#### `GET /api/sources`

List all registered import sources.

**Response:** `200 OK` — array of source objects.

```json
[
  {
    "id": 1,
    "name": "unemployment",
    "description": "BLS state unemployment rates",
    "created_at": "2024-01-15T10:00:00Z"
  }
]
```

#### `GET /api/sources/:name`

Get a single source by name.

**Response:** `200 OK` — source object (same shape as above).
**Errors:** `404` if not found.

#### `POST /api/sources`

Register a new import source.

**Request body:**

```json
{
  "name": "unemployment",
  "description": "BLS state unemployment rates",
  "field_map": {
    "State": "state_name",
    "Year":  "year",
    "Rate":  "unemployment_rate"
  }
}
```

`field_map` maps CSV column headers → canonical field names. See [ingest/README.md](../ingest/README.md#field-map-reference) for canonical names.

**Response:** `200 OK` — created source object.
**Errors:** `422` if name is missing or already taken.

---

### Locations

#### `GET /api/locations`

List dimension locations (unique county/zip/country combinations resolved during ingestion).

**Query params:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | integer | 100 | Max rows to return |
| `offset` | integer | 0 | Pagination offset |

**Response:** `200 OK` — array of location objects.

```json
[
  {
    "id": 1,
    "county": "Los Angeles",
    "country": "USA",
    "zip_code": null,
    "fips_code": "06037",
    "latitude": 34.05,
    "longitude": -118.24
  }
]
```

---

### Observations

#### `GET /api/observations`

Query normalized import rows. Results come from the `normalized_imports` table — one row per ingested CSV row with dimension FKs resolved and remaining fields in `normalized_data` JSONB.

**Query params:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `source_name` | string | — | Filter by source name |
| `location_id` | integer | — | Filter by location FK |
| `time_id` | integer | — | Filter by time FK |
| `limit` | integer | 100 | Max rows to return |
| `offset` | integer | 0 | Pagination offset |

**Response:** `200 OK` — array of normalized import objects.

```json
[
  {
    "id": 42,
    "source_name": "unemployment",
    "ingest_run_id": "550e8400-e29b-41d4-a716-446655440000",
    "location_id": 1,
    "time_id": 7,
    "normalized_data": {
      "unemployment_rate": "4.2",
      "labor_force": "19200000"
    }
  }
]
```

---

## Error responses

All errors return JSON with a single `error` field:

```json
{ "error": "source 'unknown' not found" }
```

| HTTP status | Cause |
|-------------|-------|
| `404` | Resource not found |
| `422` | Invalid or missing input |
| `500` | Database error or unexpected failure |

---

## Static file serving

Requests that don't match `/api/*` are handled by a fallback service that serves files from `web/dist/`. Unmatched paths fall back to `web/dist/index.html` (SPA mode). Build the frontend before starting the server: `cd web && npm run build`.

---

## App state

`AppState` is a cheaply-cloneable struct injected into every handler via Axum's `State<AppState>` extractor. It holds:

- `db: Arc<Db>` — shared connection pool

Handlers access it via:

```rust
async fn handler(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let repo = SomeRepository::new(&state.db);
    ...
}
```
