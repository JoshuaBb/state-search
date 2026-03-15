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
    Ignore      = 0,
    SkipRow     = 1,
    SkipDataset = 2,
}

impl Default for OnFailure {
    fn default() -> Self { Self::Ignore }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuleDef {
    pub kind: String,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
}

/// Definition for a single source CSV column.
/// YAML key = CSV column name; `canonical` = renamed-to canonical field name.
#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub canonical: String,
    /// Postgres type string: "text", "smallint", "integer", "bigint",
    /// "real", "float8", "numeric", "boolean", "date", "timestamptz".
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
    /// Only valid for "date" / "timestamptz" types.
    pub format: Option<String>,
    #[serde(default)]
    pub on_failure: Option<OnFailure>,
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}

fn default_field_type() -> String { "text".to_string() }

/// Definition for a field derived from a dimension table lookup.
#[derive(Debug, Deserialize, Clone)]
pub struct DerivedFieldDef {
    pub from: String,
    /// YAML key is `match` (readable); renamed to avoid Rust keyword collision.
    #[serde(rename = "match")]
    pub match_field: String,
    pub output: String,
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    /// CSV column name → FieldDef. YAML key is the source column.
    #[serde(default)]
    pub fields: HashMap<String, FieldDef>,
    /// canonical name → DerivedFieldDef
    #[serde(default)]
    pub derived: HashMap<String, DerivedFieldDef>,
}

fn default_pool_size() -> u32 { 10 }
fn default_host() -> String   { "0.0.0.0".to_string() }
fn default_port() -> u16      { 3000 }

impl AppConfig {
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

        let sources_path = "config/sources.yml";
        if let Ok(content) = std::fs::read_to_string(sources_path) {
            #[derive(Deserialize)]
            struct SourcesFile {
                #[serde(default)]
                sources: Vec<SourceConfig>,
            }
            let sources: SourcesFile = serde_yaml::from_str(&content).map_err(|e| {
                config::ConfigError::Message(format!("failed to parse {sources_path}: {e}"))
            })?;
            app_config.ingest.sources = sources.sources;
        }

        Ok(app_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_def_yaml_deserialization() {
        let src = "canonical: state_code\ntype: text\n";
        let def: FieldDef = serde_yaml::from_str(src).unwrap();
        assert_eq!(def.canonical, "state_code");
        assert_eq!(def.field_type, "text");
        assert!(def.on_failure.is_none());
        assert!(def.rules.is_empty());
        assert!(def.format.is_none());
    }

    #[test]
    fn source_config_yaml_deserialization() {
        let src = r#"
name: test_source
fields:
  Year:
    canonical: year
    type: smallint
  State:
    canonical: state_code
    type: text
    on_failure: skip_row
    rules:
      - kind: state_name_to_code
        on_failure: skip_dataset
derived: {}
"#;
        let sc: SourceConfig = serde_yaml::from_str(src).unwrap();
        assert_eq!(sc.name, "test_source");
        assert_eq!(sc.fields.len(), 2);
        let year = sc.fields.get("Year").unwrap();
        assert_eq!(year.canonical, "year");
        assert_eq!(year.field_type, "smallint");
        let state = sc.fields.get("State").unwrap();
        assert_eq!(state.on_failure, Some(OnFailure::SkipRow));
        assert_eq!(state.rules.len(), 1);
    }

    #[test]
    fn derived_field_def_match_key_deserialization() {
        let src = "from: dim_location\nmatch: fips_code\noutput: county\ntype: text\n";
        let d: DerivedFieldDef = serde_yaml::from_str(src).unwrap();
        assert_eq!(d.from, "dim_location");
        assert_eq!(d.match_field, "fips_code");
        assert_eq!(d.output, "county");
    }

    #[test]
    fn on_failure_ranking() {
        assert!(OnFailure::Ignore < OnFailure::SkipRow);
        assert!(OnFailure::SkipRow < OnFailure::SkipDataset);
    }
}
