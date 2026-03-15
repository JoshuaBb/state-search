use std::collections::HashMap;

use crate::transforms::FieldValue;

// ── CSV row parsing ───────────────────────────────────────────────────────────

/// Parse CSV row into canonical FieldValue map: trim all strings, empty → Null.
/// Header keys are lowercased so matching against `sources.toml` source values is case-insensitive.
pub(super) fn row_to_canonical(
    headers: &[String],
    record: &csv::StringRecord,
) -> HashMap<String, FieldValue> {
    headers
        .iter()
        .zip(record.iter())
        .map(|(header, raw)| {
            let trimmed = raw.trim();
            let val = if trimmed.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Str(trimmed.to_string())
            };
            (header.to_lowercase(), val)
        })
        .collect()
}

/// Serialize the canonical FieldValue map to JSON for raw_imports storage.
pub(super) fn canonical_to_json(map: &HashMap<String, FieldValue>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| {
            let json_val = match v {
                FieldValue::Null        => serde_json::Value::Null,
                FieldValue::Str(s)      => serde_json::Value::String(s.clone()),
                FieldValue::I8(x)       => serde_json::json!(*x),
                FieldValue::I16(x)      => serde_json::json!(*x),
                FieldValue::I32(x)      => serde_json::json!(*x),
                FieldValue::I64(x)      => serde_json::json!(*x),
                FieldValue::U8(x)       => serde_json::json!(*x),
                FieldValue::U16(x)      => serde_json::json!(*x),
                FieldValue::U32(x)      => serde_json::json!(*x),
                FieldValue::U64(x)      => serde_json::json!(*x),
                FieldValue::F32(x)      => serde_json::json!(*x),
                FieldValue::F64(x)      => serde_json::json!(*x),
                FieldValue::Bool(x)     => serde_json::json!(*x),
                FieldValue::Date(x)     => serde_json::Value::String(x.to_string()),
                FieldValue::DateTime(x) => serde_json::Value::String(x.to_rfc3339()),
            };
            (k.clone(), json_val)
        })
        .collect();
    serde_json::Value::Object(obj)
}

// ── Typed field extractors ────────────────────────────────────────────────────

pub(super) fn str_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<String> {
    match map.get(key)? {
        FieldValue::Str(s) => Some(s.clone()),
        _                  => None,
    }
}

pub(super) fn f64_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<f64> {
    match map.get(key)? {
        FieldValue::F64(v) => Some(*v),
        FieldValue::F32(v) => Some(*v as f64),
        FieldValue::Str(s) => s.parse().ok(),
        _                  => None,
    }
}

pub(super) fn i16_from_field(map: &HashMap<String, FieldValue>, key: &str) -> Option<i16> {
    match map.get(key)? {
        FieldValue::I8(v)  => Some(*v as i16),
        FieldValue::I16(v) => Some(*v),
        FieldValue::I32(v) => i16::try_from(*v).ok(),
        FieldValue::I64(v) => i16::try_from(*v).ok(),
        FieldValue::U8(v)  => Some(*v as i16),
        FieldValue::U16(v) => i16::try_from(*v).ok(),
        FieldValue::Str(s) => s.parse().ok(),
        _                  => None,
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

pub(super) fn infer_country_from_path(file_path: &str) -> Option<String> {
    if file_path.to_lowercase().split('/').any(|seg| seg == "usa") {
        Some("USA".to_string())
    } else {
        None
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_canonical_trims_strings() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec!["  hello  "]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Str(s) if s == "hello"));
    }

    #[test]
    fn row_to_canonical_whitespace_only_becomes_null() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec!["   "]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Null));
    }

    #[test]
    fn row_to_canonical_empty_string_becomes_null() {
        let headers = vec!["col".to_string()];
        let record = csv::StringRecord::from(vec![""]);
        let result = row_to_canonical(&headers, &record);
        assert!(matches!(result.get("col").unwrap(), FieldValue::Null));
    }

    #[test]
    fn row_to_canonical_lowercases_headers() {
        let headers = vec!["State".to_string(), "YEAR".to_string(), "My_Value".to_string()];
        let record = csv::StringRecord::from(vec!["Colorado", "2024", "42"]);
        let result = row_to_canonical(&headers, &record);
        assert!(result.contains_key("state"));
        assert!(result.contains_key("year"));
        assert!(result.contains_key("my_value"));
        assert!(!result.contains_key("State"));
        assert!(!result.contains_key("YEAR"));
    }

    #[test]
    fn infer_country_still_works_after_refactor() {
        assert_eq!(
            infer_country_from_path("data/usa/co/file.csv"),
            Some("USA".to_string())
        );
        assert_eq!(infer_country_from_path("data/co/file.csv"), None);
    }
}
