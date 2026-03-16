// ingest/src/pipeline/uuid.rs
use std::collections::HashMap;
use uuid::Uuid;
use crate::transforms::FieldValue;

/// Derive a deterministic UUIDv5 from a set of canonical key columns and their
/// post-transform values. Column names are sorted alphabetically; null values
/// render as empty string. Float variants use to_bits() for locale-stable encoding.
///
/// Namespace: Uuid::NAMESPACE_OID (fixed across all environments).
pub fn derive_uuid(key_cols: &[&str], row: &HashMap<String, FieldValue>) -> Uuid {
    let mut pairs: Vec<(&str, String)> = key_cols.iter()
        .map(|col| {
            let val = match row.get(*col) {
                Some(FieldValue::Null) | None => String::new(),
                Some(v)                       => field_value_to_key_str(v),
            };
            (*col, val)
        })
        .collect();
    pairs.sort_by_key(|(k, _)| *k);
    let key_str = pairs.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("|");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key_str.as_bytes())
}

fn field_value_to_key_str(v: &FieldValue) -> String {
    match v {
        FieldValue::Null        => String::new(),
        FieldValue::Str(s)      => s.clone(),
        FieldValue::Bool(b)     => b.to_string(),
        FieldValue::I8(x)       => x.to_string(),
        FieldValue::I16(x)      => x.to_string(),
        FieldValue::I32(x)      => x.to_string(),
        FieldValue::I64(x)      => x.to_string(),
        FieldValue::U8(x)       => x.to_string(),
        FieldValue::U16(x)      => x.to_string(),
        FieldValue::U32(x)      => x.to_string(),
        FieldValue::U64(x)      => x.to_string(),
        FieldValue::F32(f)      => f.to_bits().to_string(),
        FieldValue::F64(f)      => f.to_bits().to_string(),
        FieldValue::Date(d)     => d.to_string(),
        FieldValue::DateTime(dt) => dt.to_rfc3339(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, FieldValue)]) -> HashMap<String, FieldValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn same_input_produces_same_uuid() {
        let r = row(&[("county", FieldValue::Str("Denver".into())), ("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        let id2 = derive_uuid(&["county", "country"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn column_order_does_not_matter() {
        let r = row(&[("county", FieldValue::Str("Denver".into())), ("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        let id2 = derive_uuid(&["country", "county"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn null_value_renders_as_empty_string() {
        let r = row(&[("county", FieldValue::Null), ("country", FieldValue::Str("USA".into()))]);
        let id = derive_uuid(&["county", "country"], &r);
        // Deterministic: same null → same UUID
        let id2 = derive_uuid(&["county", "country"], &r);
        assert_eq!(id, id2);
    }

    #[test]
    fn absent_column_renders_as_empty_string() {
        let r = row(&[("country", FieldValue::Str("USA".into()))]);
        let id1 = derive_uuid(&["county", "country"], &r);
        // Same as explicit null
        let r2 = row(&[("county", FieldValue::Null), ("country", FieldValue::Str("USA".into()))]);
        let id2 = derive_uuid(&["county", "country"], &r2);
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_values_produce_different_uuids() {
        let r1 = row(&[("county", FieldValue::Str("Denver".into()))]);
        let r2 = row(&[("county", FieldValue::Str("Boulder".into()))]);
        assert_ne!(derive_uuid(&["county"], &r1), derive_uuid(&["county"], &r2));
    }

    #[test]
    fn float_encoding_is_stable() {
        let r = row(&[("lat", FieldValue::F64(39.7392))]);
        let id1 = derive_uuid(&["lat"], &r);
        let id2 = derive_uuid(&["lat"], &r);
        assert_eq!(id1, id2);
    }

    #[test]
    fn known_key_string_produces_expected_uuid() {
        // "county=Denver|country=USA" with NAMESPACE_OID
        let r = row(&[
            ("county", FieldValue::Str("Denver".into())),
            ("country", FieldValue::Str("USA".into())),
        ]);
        let id = derive_uuid(&["county", "country"], &r);
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"country=USA|county=Denver");
        assert_eq!(id, expected);
    }

    #[test]
    fn f32_encoding_uses_to_bits() {
        // Two structurally identical f32 values must produce the same UUID.
        // Encoding uses to_bits() — not Display — for locale/precision stability.
        let r = row(&[("lng", FieldValue::F32(104.9903_f32))]);
        let id1 = derive_uuid(&["lng"], &r);
        let id2 = derive_uuid(&["lng"], &r);
        assert_eq!(id1, id2);
        // Verify it matches the expected key string directly.
        let expected = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("lng={}", 104.9903_f32.to_bits()).as_bytes(),
        );
        assert_eq!(id1, expected);
    }

    #[test]
    fn bool_encoding_uses_to_string() {
        let r_true  = row(&[("active", FieldValue::Bool(true))]);
        let r_false = row(&[("active", FieldValue::Bool(false))]);
        let id_true  = derive_uuid(&["active"], &r_true);
        let id_false = derive_uuid(&["active"], &r_false);
        assert_ne!(id_true, id_false);
        let expected_true = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"active=true");
        assert_eq!(id_true, expected_true);
    }

    #[test]
    fn integer_encoding_uses_to_string() {
        let r = row(&[("year", FieldValue::I32(2024))]);
        let id = derive_uuid(&["year"], &r);
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"year=2024");
        assert_eq!(id, expected);
    }

    #[test]
    fn date_encoding_uses_iso8601() {
        use chrono::NaiveDate;
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let r = row(&[("date_floor", FieldValue::Date(d))]);
        let id = derive_uuid(&["date_floor"], &r);
        // NaiveDate::to_string() returns YYYY-MM-DD
        let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"date_floor=2024-03-15");
        assert_eq!(id, expected);
    }
}
