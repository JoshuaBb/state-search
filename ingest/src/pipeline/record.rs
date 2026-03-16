use std::collections::{HashMap, HashSet};

use state_search_core::{
    config::OnFailure,
    models::normalized_import::NewNormalizedImport,
    Db,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::transforms::{chain::apply_chain, resolve::ResolvedFieldMap, FieldValue};
use super::{
    IngestError,
    derived::resolve_derived,
    dimensions::{resolve_location_id, resolve_time_id, LocationCache},
    row::{canonical_to_json, row_to_canonical, strip_excluded_columns},
    schema::{collect_normalized_data, validate_completeness},
    uuid::derive_uuid,
};

pub(super) enum RowOutcome {
    Proceed(HashMap<String, FieldValue>),
    Skip,
    Abort(anyhow::Error),
}

pub(super) async fn process_record(
    record: csv::StringRecord,
    row_num: u64,
    source: &state_search_core::config::SourceConfig,
    file_path: &str,
    ingest_run_id: Uuid,
    resolved: &ResolvedFieldMap,
    headers: &[String],
    default_country: Option<&str>,
    db: &Db,
    location_cache: &LocationCache,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
) -> anyhow::Result<Option<NewNormalizedImport>> {
    let source_name = source.name.as_str();

    // Strip excluded columns before any processing
    let (effective_headers, effective_record);
    let (h_ref, r_ref): (&[String], &csv::StringRecord) = if source.exclude_columns.is_empty() {
        (headers, &record)
    } else {
        let excluded: HashSet<String> = source.exclude_columns.iter()
            .map(|c| c.to_lowercase())
            .collect();
        let (fh, fr) = strip_excluded_columns(headers, &record, &excluded);
        effective_headers = fh;
        effective_record  = fr;
        (&effective_headers, &effective_record)
    };

    let raw = row_to_canonical(h_ref, r_ref);
    debug!(row = row_num, "parsed CSV row");

    let raw_import_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(Option::<i64>::None)
    .bind(file_path)
    .bind(&canonical_to_json(&raw))
    .fetch_one(db)
    .await?;

    let transformed = match apply_all_transforms(&raw, resolved, row_num, source_name, file_path) {
        RowOutcome::Proceed(t) => t,
        RowOutcome::Skip       => return Ok(None),
        RowOutcome::Abort(e)   => return Err(e),
    };

    if let Err(msg) = validate_completeness(&transformed, schema, derived_fields, row_num) {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    let (location_id, time_id) = tokio::join!(
        resolve_location_id(&transformed, default_country, db, row_num, location_cache),
        resolve_time_id(&transformed, db, row_num),
    );

    let mut normalized_data = serde_json::json!({});
    if let Err(msg) = resolve_derived(
        &mut normalized_data, source, location_id, time_id, db, row_num,
    ).await {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    let normalized_data = match collect_normalized_data(
        &transformed, schema, derived_fields, &normalized_data, row_num,
    ) {
        Ok(data) => data,
        Err(msg) => {
            warn!(row = row_num, "{}", msg);
            return Ok(None);
        }
    };

    // Derive normalized_imports.id: deterministic if unique_key set, else random.
    // location_id / time_id may appear in unique_key — inject them as Str values so
    // derive_uuid can find them by name.
    let row_id = if source.unique_key.is_empty() {
        Uuid::new_v4()
    } else {
        let key_set: HashSet<&str> = source.unique_key.iter().map(String::as_str).collect();
        let mut key_map: HashMap<String, FieldValue> = transformed.clone();
        if key_set.contains("location_id") {
            key_map.insert(
                "location_id".to_string(),
                FieldValue::Str(location_id.map(|id| id.to_string()).unwrap_or_default()),
            );
        }
        if key_set.contains("time_id") {
            key_map.insert(
                "time_id".to_string(),
                FieldValue::Str(time_id.map(|id| id.to_string()).unwrap_or_default()),
            );
        }
        let key_cols: Vec<&str> = source.unique_key.iter().map(String::as_str).collect();
        derive_uuid(&key_cols, &key_map)
    };

    debug!(row = row_num, "row ready");
    Ok(Some(NewNormalizedImport {
        id:              row_id,
        raw_import_id:   Some(raw_import_id),
        location_id,
        time_id,
        source_name:     source_name.to_string(),
        ingest_run_id,
        normalized_data,
    }))
}

fn apply_all_transforms(
    raw: &HashMap<String, FieldValue>,
    resolved: &ResolvedFieldMap,
    row_num: u64,
    source_name: &str,
    file_path: &str,
) -> RowOutcome {
    resolved
        .iter()
        .fold(RowOutcome::Proceed(HashMap::new()), |outcome, (canonical, resolved_field)| {
            let RowOutcome::Proceed(mut acc) = outcome else {
                return outcome;
            };

            let raw_val = raw
                .get(resolved_field.source_col.as_str())
                .cloned()
                .unwrap_or(FieldValue::Null);

            match apply_chain(raw_val, &resolved_field.chain) {
                Ok(v) => {
                    acc.insert(canonical.clone(), v);
                    RowOutcome::Proceed(acc)
                }
                Err(OnFailure::SkipRow) => {
                    warn!(row = row_num, field = canonical, source = source_name, "SkipRow: discarding row");
                    RowOutcome::Skip
                }
                Err(OnFailure::SkipDataset) => RowOutcome::Abort(
                    IngestError::DatasetSkipped {
                        file: file_path.to_string(),
                        reason: format!("transform failure on field '{}' at row {}", canonical, row_num),
                    }
                    .into(),
                ),
                Err(OnFailure::Ignore) => unreachable!(),
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_all_transforms_proceed_on_empty_resolved() {
        let raw = HashMap::new();
        let resolved = ResolvedFieldMap::new();
        match apply_all_transforms(&raw, &resolved, 1, "src", "f.csv") {
            RowOutcome::Proceed(m) => assert!(m.is_empty()),
            _ => panic!("expected Proceed"),
        }
    }
}
