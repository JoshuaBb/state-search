use uuid::Uuid;

/// Generate a DuckDB SQL script that exports observations for a completed ingest
/// run to partitioned Parquet files.
///
/// The script attaches Postgres as a read-only source via DuckDB's Postgres
/// scanner, joins dimensions and context, flattens JSONB attributes into named
/// columns, and writes Parquet partitioned by year.
///
/// The generated SQL is written to `exports/<source_name>/export_<ingest_run_id>.sql`
/// and can be executed by DuckDB at any time:
///
/// ```sh
/// duckdb -c ".read exports/co_public_drinking_water/export_<uuid>.sql"
/// ```
pub(super) fn generate_export_sql(
    source_name: &str,
    ingest_run_id: Uuid,
    attributes: &[String],
    pg_connection: &str,
    export_base: &str,
) -> String {
    let attr_projections: String = if attributes.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = attributes
            .iter()
            .map(|k| format!("        c.attributes->>'{}' AS {}", k, k))
            .collect();
        format!(",\n{}", cols.join(",\n"))
    };

    let context_join = if attributes.is_empty() {
        String::new()
    } else {
        "\n    LEFT JOIN pg.fact_row_context c ON c.id = o.context_id".to_string()
    };

    format!(
        r#"-- Auto-generated export script for ingest_run_id = {ingest_run_id}
-- Source: {source_name}
-- Execute with: duckdb -c ".read {export_base}/{source_name}/export_{ingest_run_id}.sql"

ATTACH '{pg_connection}' AS pg (TYPE POSTGRES, READ_ONLY);

COPY (
    SELECT
        o.metric_name,
        o.metric_value,
        o.source_name,
        l.state_code,
        l.state_name,
        l.county,
        l.fips_code,
        t.year,
        t.quarter,
        t.month{attr_projections}
    FROM pg.fact_observations o
    LEFT JOIN pg.dim_location l ON l.id = o.location_id
    LEFT JOIN pg.dim_time     t ON t.id = o.time_id{context_join}
    WHERE o.ingest_run_id = '{ingest_run_id}'
) TO '{export_base}/{source_name}'
   (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE);
"#,
        ingest_run_id = ingest_run_id,
        source_name = source_name,
        pg_connection = pg_connection,
        export_base = export_base,
        attr_projections = attr_projections,
        context_join = context_join,
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_export_sql_includes_attribute_projections() {
        let id = Uuid::new_v4();
        let sql = generate_export_sql(
            "co_public_drinking_water",
            id,
            &["analyte_name".to_string(), "units".to_string()],
            "host=localhost dbname=state_search",
            "exports",
        );

        assert!(sql.contains("c.attributes->>'analyte_name' AS analyte_name"));
        assert!(sql.contains("c.attributes->>'units' AS units"));
        assert!(sql.contains("LEFT JOIN pg.fact_row_context c ON c.id = o.context_id"));
        assert!(sql.contains(&id.to_string()));
    }

    #[test]
    fn generate_export_sql_no_attributes_omits_context_join() {
        let id = Uuid::new_v4();
        let sql = generate_export_sql(
            "epa_air_quality",
            id,
            &[],
            "host=localhost dbname=state_search",
            "exports",
        );

        assert!(!sql.contains("fact_row_context"));
        assert!(sql.contains("fact_observations"));
    }
}
