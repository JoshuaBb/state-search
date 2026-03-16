/// Log entry written after a successful dim-only ingest run (append-only).
#[derive(Debug, Clone)]
pub struct NewDimFileLog {
    pub source_name: String,
    pub target:      String,  // serde snake_case of DimTarget: "dim_location" | "dim_time"
    pub file_path:   String,
    pub row_count:   i64,
}
