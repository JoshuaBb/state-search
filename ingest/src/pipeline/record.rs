use std::collections::HashMap;

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
    row::{canonical_to_json, row_to_canonical},
    schema::{collect_normalized_data, validate_completeness},
};

pub(super) enum RowOutcome {
    Proceed(HashMap<String, FieldValue>),
    Skip,
    Abort(anyhow::Error),
}

/// Process a single CSV record.
/// Returns Some(NewNormalizedImport) on success, None if the row is skipped.
/// Returns Err only on SkipDataset (abort the entire file).
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
    let raw = row_to_canonical(headers, &record);
    debug!(row = row_num, "parsed CSV row");

    // Step 2: store raw row, capture raw_import_id
    let raw_import_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_imports (source_id, source_file, raw_data) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(Option::<i64>::None)
    .bind(file_path)
    .bind(&canonical_to_json(&raw))
    .fetch_one(db)
    .await?;

    // Step 3: apply transforms (no passthrough — schema is strict)
    let transformed = match apply_all_transforms(&raw, resolved, row_num, source_name, file_path) {
        RowOutcome::Proceed(t) => t,
        RowOutcome::Skip       => return Ok(None),
        RowOutcome::Abort(e)   => return Err(e),
    };

    // Step 4: validate completeness (all required schema fields present)
    if let Err(msg) = validate_completeness(&transformed, schema, derived_fields, row_num) {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    // Step 5: resolve dimension FKs
    let (location_id, time_id) = tokio::join!(
        resolve_location_id(&transformed, default_country, db, row_num, location_cache),
        resolve_time_id(&transformed, db, row_num),
    );

    // Steps 6-7: resolve derived fields (prerequisite check + dim lookups)
    let mut normalized_data = serde_json::json!({});
    if let Err(msg) = resolve_derived(
        &mut normalized_data, source, location_id, time_id, db, row_num,
    ).await {
        warn!(row = row_num, "{}", msg);
        return Ok(None);
    }

    // Step 8: collect non-dimension, non-derived fields into JSONB
    let normalized_data = match collect_normalized_data(
        &transformed, schema, derived_fields, &normalized_data, row_num,
    ) {
        Ok(data) => data,
        Err(msg) => {
            warn!(row = row_num, "{}", msg);
            return Ok(None);
        }
    };

    debug!(row = row_num, "row ready");
    Ok(Some(NewNormalizedImport {
        raw_import_id: Some(raw_import_id),
        location_id,
        time_id,
        source_name: source_name.to_string(),
        ingest_run_id,
        normalized_data,
    }))
}

// ── Transform helpers ──────────────────────────────────────────────────────────

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
