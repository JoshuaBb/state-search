use std::collections::HashMap;

use anyhow::Context;
use serde_json::{Map, Value};
use state_search_core::{
    config::{FieldDef, SourceConfig},
    models::{
        location::NewLocation,
        observation::NewObservation,
        time::NewTimePeriod,
    },
    repositories::{
        location::LocationRepository,
        observation::ObservationRepository,
        time::TimeRepository,
    },
    Db, Result,
};
use tracing::{debug, info, warn};

/// Known canonical field names extracted from `field_map` before treating
/// remaining columns as metric values.
const LOCATION_FIELDS: &[&str] = &["state_code", "state_name", "country", "zip_code", "fips_code", "latitude", "longitude"];
const TIME_FIELDS: &[&str]     = &["year", "quarter", "month", "day"];

pub struct IngestPipeline<'a> {
    db: &'a Db,
}

impl<'a> IngestPipeline<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Ingest one CSV file against a named source definition.
    pub async fn run(&self, source: &SourceConfig, file_path: &str) -> anyhow::Result<u64> {
        let source_name = source.name.as_str();
        info!(source = source_name, file = file_path, "starting ingest");

        let mut reader = csv::Reader::from_path(file_path)
            .with_context(|| format!("cannot open {file_path}"))?;

        let headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|h| h.to_string())
            .collect();

        debug!(source = source_name, ?headers, "CSV headers detected");

        let field_map = build_field_map(&source.field_map);
        debug!(
            source = source_name,
            runtime_mapping = ?field_map,
            "field map built (keys=source columns, values=canonical names)"
        );

        let default_country = infer_country_from_path(file_path);
        if let Some(c) = &default_country {
            debug!(source = source_name, country = c, "inferred default country from file path");
        }

        // Skip check: if any raw_import row exists for this file, skip it
        let already_ingested: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM raw_imports WHERE source_file = $1)"
        )
        .bind(file_path)
        .fetch_one(self.db)
        .await?;

        if already_ingested {
            info!(source = source_name, file = file_path, "skipping (already ingested)");
            return Ok(0);
        }

        let mut total = 0u64;
        let mut row_num = 0u64;

        for record in reader.records() {
            let record = record?;
            row_num += 1;

            // Build raw JSON from CSV row
            let raw_json = row_to_json(&headers, &record);
            debug!(row = row_num, raw = %raw_json, "parsed CSV row");

            // Store raw row (source_id is NULL — spec §DB Changes)
            let raw_id: i64 = sqlx::query_scalar(
                "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id"
            )
            .bind(Option::<i64>::None)
            .bind(file_path)
            .bind(&raw_json)
            .fetch_one(self.db)
            .await?;

            // Remap columns using field_map
            let canonical = apply_field_map(&raw_json, &field_map);
            debug!(row = row_num, canonical = %canonical, "applied field map");

            // Resolve dimension IDs
            let location_id = match self.resolve_location(&canonical, default_country.as_deref()).await {
                Ok(id) => {
                    debug!(row = row_num, location_id = id, "resolved location");
                    Some(id)
                }
                Err(e) => {
                    warn!(row = row_num, error = %e, canonical = %canonical, "could not resolve location — observation will have no location_id");
                    None
                }
            };

            let time_id = match self.resolve_time(&canonical).await {
                Ok(id) => {
                    debug!(row = row_num, time_id = id, "resolved time");
                    Some(id)
                }
                Err(e) => {
                    warn!(row = row_num, error = %e, canonical = %canonical, "could not resolve time — observation will have no time_id");
                    None
                }
            };

            // Remaining fields → metric observations
            let metrics = extract_metrics(&canonical);
            if metrics.is_empty() {
                warn!(row = row_num, canonical = %canonical, "no metric fields found after removing location/time columns — row will produce 0 observations");
            } else {
                debug!(row = row_num, metric_names = ?metrics.iter().map(|(k,_)| k).collect::<Vec<_>>(), "extracted metrics");
            }

            let observations: Vec<NewObservation> = metrics
                .into_iter()
                .map(|(name, value)| NewObservation {
                    raw_import_id: Some(raw_id),
                    location_id,
                    time_id,
                    source_name: Some(source_name.to_string()),
                    metric_name: name,
                    metric_value: value,
                    attributes: None,
                })
                .collect();

            let count = ObservationRepository::new(self.db)
                .bulk_create(observations)
                .await?;

            debug!(row = row_num, inserted = count, "row complete");
            total += count;
        }

        info!(source = source_name, file = file_path, observations = total, "ingest complete");
        Ok(total)
    }

    async fn resolve_location(&self, canonical: &Value, default_country: Option<&str>) -> Result<i64> {
        let loc = NewLocation {
            state_code: str_field(canonical, "state_code"),
            state_name: str_field(canonical, "state_name"),
            country:    str_field(canonical, "country").or_else(|| default_country.map(str::to_string)),
            zip_code:   str_field(canonical, "zip_code"),
            fips_code:  str_field(canonical, "fips_code"),
            latitude:   f64_field(canonical, "latitude"),
            longitude:  f64_field(canonical, "longitude"),
        };

        if loc.is_empty() {
            return Err(state_search_core::CoreError::InvalidInput(
                "no location fields".into(),
            ));
        }

        LocationRepository::new(self.db).upsert(loc).await
    }

    async fn resolve_time(&self, canonical: &Value) -> Result<i64> {
        let year = i16_field(canonical, "year")
            .ok_or_else(|| {
                state_search_core::CoreError::InvalidInput("missing year field".into())
            })?;

        let t = NewTimePeriod {
            year,
            quarter: i16_field(canonical, "quarter"),
            month:   i16_field(canonical, "month"),
            day:     i16_field(canonical, "day"),
        };

        TimeRepository::new(self.db).upsert(t).await
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn row_to_json(headers: &[String], record: &csv::StringRecord) -> Value {
    let mut map = Map::new();
    for (header, value) in headers.iter().zip(record.iter()) {
        map.insert(header.clone(), Value::String(value.to_string()));
    }
    Value::Object(map)
}

/// Invert canonical_name→source_col map to produce source_col→canonical_name.
fn build_field_map(field_map: &HashMap<String, FieldDef>) -> HashMap<String, String> {
    field_map
        .iter()
        .map(|(canonical, field_def)| (field_def.source.clone(), canonical.clone()))
        .collect()
}

fn apply_field_map(raw: &Value, field_map: &HashMap<String, String>) -> Value {
    let mut out = Map::new();
    if let Some(obj) = raw.as_object() {
        for (col, val) in obj {
            let key = field_map.get(col).cloned().unwrap_or_else(|| col.clone());
            out.insert(key, val.clone());
        }
    }
    Value::Object(out)
}

/// Returns non-location, non-time fields as (metric_name, numeric_value) pairs.
fn extract_metrics(canonical: &Value) -> Vec<(String, Option<f64>)> {
    let skip: std::collections::HashSet<&str> = LOCATION_FIELDS
        .iter()
        .chain(TIME_FIELDS.iter())
        .copied()
        .collect();

    canonical
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter(|(k, _)| !skip.contains(k.as_str()))
                .map(|(k, v)| {
                    let num = v
                        .as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()));
                    (k.clone(), num)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn infer_country_from_path(file_path: &str) -> Option<String> {
    let lower = file_path.to_lowercase();
    if lower.split('/').any(|seg| seg == "usa") {
        Some("USA".to_string())
    } else {
        None
    }
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)?.as_str().map(|s| s.to_string())
}

fn i16_field(v: &Value, key: &str) -> Option<i16> {
    v.get(key)
        .and_then(|x| x.as_i64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
        .and_then(|n| i16::try_from(n).ok())
}

fn f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key)
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}
