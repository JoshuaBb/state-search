use config::{Config, Environment, File};
use serde::Deserialize;
use std::collections::HashMap;

use crate::error::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_pool_size")]
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: default_host(), port: default_port() }
    }
}

impl ServerConfig {
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct IngestConfig {
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    Ignore      = 0,   // treat field as Null, continue
    SkipRow     = 1,   // discard this row, continue ingest
    SkipDataset = 2,   // rollback the entire file transaction
}
// Note: `Ord` is derived by declaration order, which matches the discriminant order
// (Ignore < SkipRow < SkipDataset). Do not reorder variants.

impl Default for OnFailure {
    fn default() -> Self { Self::Ignore }
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    #[default]
    String,
    I8, I16, I32, I64,
    U8, U16, U32, U64,
    F32, F64,
    Bool,
    Date,
    DateTime,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuleDef {
    pub kind: String,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub source: String,
    /// TOML key is "type"; `type` is a Rust keyword so we rename via serde.
    #[serde(rename = "type", default)]
    pub field_type: FieldType,
    /// Format string — only valid for Date/DateTime; validated at resolve time.
    pub format: Option<String>,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub field_map: HashMap<String, FieldDef>,
}

fn default_pool_size() -> u32 { 10 }
fn default_host() -> String   { "0.0.0.0".to_string() }
fn default_port() -> u16      { 3000 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_def_minimal_deserialization() {
        let src = r#"source = "state""#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.source, "state");
        assert_eq!(def.field_type, FieldType::String);
        assert!(def.on_failure.is_none());
        assert!(def.rules.is_empty());
        assert!(def.format.is_none());
    }

    #[test]
    fn field_def_full_deserialization() {
        let src = r#"
            source     = "state"
            type       = "string"
            on_failure = "skip_row"

            [[rules]]
            kind       = "state_name_to_code"
            on_failure = "skip_dataset"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.on_failure, Some(OnFailure::SkipRow));
        assert_eq!(def.rules.len(), 1);
        assert_eq!(def.rules[0].kind, "state_name_to_code");
        assert_eq!(def.rules[0].on_failure, Some(OnFailure::SkipDataset));
    }

    #[test]
    fn on_failure_ranking() {
        assert!(OnFailure::Ignore < OnFailure::SkipRow);
        assert!(OnFailure::SkipRow < OnFailure::SkipDataset);
    }

    #[test]
    fn field_type_date_with_format() {
        // Note: TOML does not support semicolons as separators — each key must be on its own line
        let src = r#"
            source = "col"
            type   = "date"
            format = "month_day_year"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.field_type, FieldType::Date);
        assert_eq!(def.format.as_deref(), Some("month_day_year"));
    }

    #[test]
    fn field_def_full_explicit_type_is_asserted() {
        let src = r#"
            source     = "state"
            type       = "string"
            on_failure = "skip_row"
        "#;
        let def: FieldDef = toml::from_str(src).unwrap();
        assert_eq!(def.field_type, FieldType::String);
        assert_eq!(def.on_failure, Some(OnFailure::SkipRow));
    }
}

impl AppConfig {
    /// Load config from `config/default.toml`, then environment variables.
    /// Environment variables are prefixed with `APP` and use `__` as separator.
    /// Example: `APP__DATABASE__URL=postgres://...`
    ///
    /// `config/sources.toml` is loaded separately via the `toml` crate because the
    /// `config` crate cannot correctly deserialize `HashMap<String, FieldDef>` — it
    /// flattens TOML into its own internal value format, losing the nested table
    /// structure that `FieldDef` requires (especially array-of-tables for rules).
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(
                Environment::with_prefix("APP")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        let mut app_config: Self = cfg.try_deserialize()?;

        let sources_path = "config/sources.toml";
        if let Ok(content) = std::fs::read_to_string(sources_path) {
            #[derive(Deserialize)]
            struct SourcesFile {
                #[serde(default)]
                ingest: IngestConfig,
            }
            let sources: SourcesFile = toml::from_str(&content).map_err(|e| {
                config::ConfigError::Message(format!("failed to parse {sources_path}: {e}"))
            })?;
            app_config.ingest = sources.ingest;
        }

        Ok(app_config)
    }
}
