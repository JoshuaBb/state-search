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

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub field_map: HashMap<String, String>,
}

fn default_pool_size() -> u32 { 10 }
fn default_host() -> String   { "0.0.0.0".to_string() }
fn default_port() -> u16      { 3000 }

impl AppConfig {
    /// Load config from `config/default.toml`, then environment variables.
    /// Environment variables are prefixed with `APP` and use `__` as separator.
    /// Example: `APP__DATABASE__URL=postgres://...`
    pub fn load() -> Result<Self> {
        let cfg = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(File::with_name("config/sources").required(false))
            .add_source(
                Environment::with_prefix("APP")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
