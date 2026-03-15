use std::collections::HashMap;
use uuid::Uuid;

const LOCATION_FIELDS: &[&str] = &[
    "county", "country", "zip_code", "fips_code", "latitude", "longitude",
    "state_code", "state_name",
];
const TIME_FIELDS: &[&str] = &["year", "quarter", "month", "day"];

fn is_dimension_field(name: &str) -> bool {
    LOCATION_FIELDS.contains(&name) || TIME_FIELDS.contains(&name)
}

/// Generate a DuckDB SQL script that exports normalized_imports for a completed
/// ingest run to partitioned Parquet files.
pub(super) fn generate_export_sql(
    source_name: &str,
    ingest_run_id: Uuid,
    schema: &HashMap<String, String>,
    _derived_fields: &HashMap<String, String>, // reserved for future type-cast differentiation
    pg_connection: &str,
    export_base: &str,
) -> String {
    // Project all non-dimension fields from normalized_data
    let mut projections: Vec<String> = Vec::new();
    for field_name in schema.keys() {
        if is_dimension_field(field_name) {
            continue;
        }
        projections.push(format!(
            "        (n.normalized_data->>'{}') AS {}",
            field_name, field_name
        ));
    }

    let extra_cols = if projections.is_empty() {
        String::new()
    } else {
        format!(",\n{}", projections.join(",\n"))
    };

    format!(
        r#"-- Auto-generated export script for ingest_run_id = {ingest_run_id}
-- Source: {source_name}
-- Execute with: duckdb -c ".read {export_base}/{source_name}/export_{ingest_run_id}.sql"

ATTACH '{pg_connection}' AS pg (TYPE POSTGRES, READ_ONLY);

COPY (
    SELECT
        n.source_name,
        n.ingest_run_id,
        l.state_code,
        l.state_name,
        l.county,
        l.fips_code,
        t.year,
        t.quarter,
        t.month{extra_cols}
    FROM pg.normalized_imports n
    LEFT JOIN pg.dim_location l ON l.id = n.location_id
    LEFT JOIN pg.dim_time     t ON t.id = n.time_id
    WHERE n.ingest_run_id = '{ingest_run_id}'
      AND n.source_name   = '{source_name}'
) TO '{export_base}/{source_name}'
   (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE);
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_export_sql_uses_normalized_imports() {
        let id = Uuid::new_v4();
        let mut schema = HashMap::new();
        schema.insert("year".to_string(),         "smallint".to_string());
        schema.insert("state_code".to_string(),   "text".to_string());
        schema.insert("analyte_name".to_string(), "text".to_string());
        schema.insert("average_value".to_string(),"numeric".to_string());

        let sql = generate_export_sql(
            "co_public_drinking_water",
            id,
            &schema,
            &HashMap::new(),
            "host=localhost dbname=state_search",
            "exports",
        );

        assert!(sql.contains("normalized_imports"));
        assert!(!sql.contains("fact_observations"));
        assert!(!sql.contains("fact_row_context"));
        assert!(sql.contains(&id.to_string()));
        // non-dim fields are projected from normalized_data
        assert!(sql.contains("normalized_data->>'analyte_name'"));
        assert!(sql.contains("normalized_data->>'average_value'"));
        // dimension fields come from dim joins, not normalized_data
        assert!(!sql.contains("normalized_data->>'year'"));
        assert!(!sql.contains("normalized_data->>'state_code'"));
    }

    #[test]
    fn generate_export_sql_no_schema_fields_still_produces_core_columns() {
        let id = Uuid::new_v4();
        let sql = generate_export_sql(
            "epa_air_quality",
            id,
            &HashMap::new(),
            &HashMap::new(),
            "host=localhost dbname=state_search",
            "exports",
        );
        assert!(sql.contains("normalized_imports"));
        assert!(sql.contains("dim_location"));
        assert!(sql.contains("dim_time"));
    }
}
