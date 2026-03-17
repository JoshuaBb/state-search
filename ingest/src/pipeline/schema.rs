use std::collections::HashMap;

use state_search_core::{
    config::SourceConfig,
    models::import_schema_field::NewImportSchemaField,
    repositories::import_schema::ImportSchemaRepository,
    Db,
};
use tracing::info;

use crate::transforms::FieldValue;

/// Dimension field names — excluded from normalized_data (stored as FK references instead).
const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
    "state_code", "state_name",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

fn is_dimension_field(name: &str) -> bool {
    LOCATION_FIELDS.contains(&name) || TIME_FIELDS.contains(&name)
}

/// Seed import_schema from the source's field definitions, then validate
/// all expected fields are present. Returns canonical_name → type string.
pub(super) async fn seed_and_validate(
    source: &SourceConfig,
    db: &Db,
) -> anyhow::Result<HashMap<String, String>> {
    let repo = ImportSchemaRepository::new(db);

    // Build the full list of fields to seed (declared fields + derived fields)
    let mut to_seed: Vec<NewImportSchemaField> = Vec::new();

    for def in source.fields.values() {
        to_seed.push(NewImportSchemaField {
            source_name: source.name.clone(),
            field_name:  def.canonical.clone(),
            field_type:  def.field_type.clone(),
        });
    }
    for (canonical, derived_def) in &source.derived {
        to_seed.push(NewImportSchemaField {
            source_name: source.name.clone(),
            field_name:  canonical.clone(),
            field_type:  derived_def.field_type.clone(),
        });
    }

    repo.upsert_fields(to_seed).await?;

    let schema = repo.load_for_source(&source.name).await?;

    // Expected = all canonical names from fields + derived
    let expected: Vec<String> = source.fields.values()
        .map(|d| d.canonical.clone())
        .chain(source.derived.keys().cloned())
        .collect();

    let missing: Vec<&str> = expected.iter()
        .filter(|name| !schema.contains_key(*name))
        .map(String::as_str)
        .collect();

    if !missing.is_empty() {
        anyhow::bail!(
            "import_schema validation failed for source '{}': missing fields: {}",
            source.name,
            missing.join(", ")
        );
    }

    info!(source = %source.name, fields = schema.len(), "import_schema validated");
    Ok(schema)
}

/// Check that all required (non-derived) schema fields are present in the
/// transformed row. Returns Err with a message identifying the missing field.
pub(super) fn validate_completeness(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
    row_num: u64,
) -> Result<(), String> {
    for field_name in schema.keys() {
        if derived_fields.contains_key(field_name) {
            continue; // derived fields are not expected in the CSV row
        }
        if !row.contains_key(field_name) {
            return Err(format!(
                "row {}: required field '{}' missing from transformed row (check CSV headers and field_map)",
                row_num, field_name
            ));
        }
    }
    Ok(())
}

/// Build the normalized_data JSONB from the transformed row.
/// - Skips dimension fields (stored as location_id/time_id FK).
/// - Skips derived fields (already in existing_data from resolve_derived step).
/// - Merges with existing_data (derived field values).
pub(super) fn collect_normalized_data(
    row: &HashMap<String, FieldValue>,
    schema: &HashMap<String, String>,
    derived_fields: &HashMap<String, String>,
    existing_data: &serde_json::Value,
    row_num: u64,
) -> Result<serde_json::Value, String> {
    let mut obj = existing_data.as_object()
        .cloned()
        .unwrap_or_default();

    for (field_name, _field_type) in schema {
        if is_dimension_field(field_name) {
            continue;
        }
        if derived_fields.contains_key(field_name) {
            continue; // already in obj from existing_data
        }

        let val = row.get(field_name).unwrap_or(&FieldValue::Null);
        let json_val = field_value_to_json(val, field_name, row_num)?;
        obj.insert(field_name.clone(), json_val);
    }

    Ok(serde_json::Value::Object(obj))
}

/// Convert a FieldValue to a serde_json::Value.
fn field_value_to_json(
    val: &FieldValue,
    _field_name: &str,
    _row_num: u64,
) -> Result<serde_json::Value, String> {
    let json = match val {
        FieldValue::Null      => serde_json::Value::Null,
        FieldValue::Str(s)    => serde_json::Value::String(s.clone()),
        FieldValue::Bool(b)   => serde_json::Value::Bool(*b),
        FieldValue::I8(x)     => serde_json::json!(*x),
        FieldValue::I16(x)    => serde_json::json!(*x),
        FieldValue::I32(x)    => serde_json::json!(*x),
        FieldValue::I64(x)    => serde_json::json!(*x),
        FieldValue::U8(x)     => serde_json::json!(*x),
        FieldValue::U16(x)    => serde_json::json!(*x),
        FieldValue::U32(x)    => serde_json::json!(*x),
        FieldValue::U64(x)    => serde_json::json!(*x),
        FieldValue::F32(x)    => serde_json::json!(*x),
        FieldValue::F64(x)    => serde_json::json!(*x),
        FieldValue::Json(v)   => v.clone(),
        FieldValue::Date(d)   => serde_json::Value::String(d.to_string()),
        FieldValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
    };

    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn make_row(pairs: &[(&str, FieldValue)]) -> HashMap<String, FieldValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn validate_completeness_passes_when_all_fields_present() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        assert!(validate_completeness(&row, &schema, &HashMap::new(), 1).is_ok());
    }

    #[test]
    fn validate_completeness_fails_when_non_derived_field_missing() {
        let schema = make_schema(&[("year", "smallint"), ("analyte_name", "text")]);
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        let result = validate_completeness(&row, &schema, &HashMap::new(), 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("analyte_name"));
    }

    #[test]
    fn validate_completeness_skips_derived_fields() {
        let schema = make_schema(&[("year", "smallint"), ("county_name", "text")]);
        let mut derived = HashMap::new();
        derived.insert("county_name".to_string(), "text".to_string());
        let row = make_row(&[("year", FieldValue::I16(2024))]);
        assert!(validate_completeness(&row, &schema, &derived, 1).is_ok());
    }

    #[test]
    fn collect_normalized_data_excludes_dimension_fields() {
        let schema = make_schema(&[
            ("year", "smallint"),
            ("state_code", "text"),
            ("analyte_name", "text"),
        ]);
        let row = make_row(&[
            ("year", FieldValue::I16(2024)),
            ("state_code", FieldValue::Str("CO".to_string())),
            ("analyte_name", FieldValue::Str("TTHM".to_string())),
        ]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!(result.get("analyte_name").is_some());
        assert!(result.get("year").is_none());
        assert!(result.get("state_code").is_none());
    }

    #[test]
    fn collect_normalized_data_skips_already_derived_fields() {
        let schema = make_schema(&[("county_name", "text"), ("analyte_name", "text")]);
        let mut existing = serde_json::json!({});
        existing["county_name"] = serde_json::json!("Denver");
        let row = make_row(&[("analyte_name", FieldValue::Str("TTHM".to_string()))]);
        let mut derived_names = HashMap::new();
        derived_names.insert("county_name".to_string(), "text".to_string());
        let result = collect_normalized_data(&row, &schema, &derived_names, &existing, 1).unwrap();
        assert_eq!(result["county_name"], "Denver");
        assert_eq!(result["analyte_name"], "TTHM");
    }

    #[test]
    fn collect_normalized_data_includes_numeric_values() {
        let schema = make_schema(&[("average_value", "numeric")]);
        let row = make_row(&[("average_value", FieldValue::F64(3.8))]);
        let existing = serde_json::json!({});
        let result = collect_normalized_data(&row, &schema, &HashMap::new(), &existing, 1).unwrap();
        assert!((result["average_value"].as_f64().unwrap() - 3.8).abs() < f64::EPSILON);
    }
}
