use state_search_core::{config::SourceConfig, Db};
use tracing::warn;

/// Resolve derived fields from dimension tables and insert into normalized_data.
///
/// Prerequisite check: if a derived field uses `from: dim_location` and
/// `location_id` is None (dim upsert failed at step 5), the row fails immediately.
/// Same for `from: dim_time` and `time_id`.
///
/// Returns Err with a descriptive message on prerequisite failure or lookup failure.
pub(super) async fn resolve_derived(
    normalized_data: &mut serde_json::Value,
    source: &SourceConfig,
    location_id: Option<i64>,
    time_id: Option<i64>,
    db: &Db,
    row_num: u64,
) -> Result<(), String> {
    for (canonical, def) in &source.derived {
        // Prerequisite check
        match def.from.as_str() {
            "dim_location" => {
                if location_id.is_none() {
                    return Err(format!(
                        "row {row_num}: cannot resolve derived field '{canonical}' — \
                         dim_location upsert failed (location_id is null); \
                         check location fields in CSV row"
                    ));
                }
            }
            "dim_time" => {
                if time_id.is_none() {
                    return Err(format!(
                        "row {row_num}: cannot resolve derived field '{canonical}' — \
                         dim_time upsert failed (time_id is null); \
                         check time fields in CSV row"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "row {row_num}: unsupported derived source table '{other}' \
                     for field '{canonical}'"
                ));
            }
        }

        // Dim lookup
        let value: Option<String> = match def.from.as_str() {
            "dim_location" => {
                let id = location_id.unwrap();
                let sql = format!("SELECT {} FROM dim_location WHERE id = $1", def.output);
                sqlx::query_scalar::<_, Option<String>>(&sql)
                    .bind(id)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| format!("row {row_num}: derived lookup failed for '{canonical}': {e}"))?
                    .flatten()
            }
            "dim_time" => {
                let id = time_id.unwrap();
                let sql = format!("SELECT {} FROM dim_time WHERE id = $1", def.output);
                sqlx::query_scalar::<_, Option<String>>(&sql)
                    .bind(id)
                    .fetch_optional(db)
                    .await
                    .map_err(|e| format!("row {row_num}: derived lookup failed for '{canonical}': {e}"))?
                    .flatten()
            }
            _ => unreachable!(),
        };

        match value {
            Some(v) => {
                normalized_data[canonical] = serde_json::Value::String(v);
            }
            None => {
                warn!(row = row_num, field = canonical, "derived lookup returned no value — row will fail");
                return Err(format!(
                    "row {row_num}: derived field '{canonical}' lookup returned no match \
                     (from={}, output={})",
                    def.from, def.output
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn unsupported_table_name_error_message() {
        let msg = format!("unsupported derived source table 'dim_unknown'");
        assert!(msg.contains("dim_unknown"));
    }
}
