use sqlx::{postgres::PgPoolOptions, PgPool};

use crate::{config::DatabaseConfig, error::Result};

pub type Db = PgPool;

pub async fn connect(cfg: &DatabaseConfig) -> Result<Db> {
    let pool = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect(&cfg.url)
        .await?;

    Ok(pool)
}

pub async fn migrate(pool: &Db) -> Result<()> {
    sqlx::migrate!("../migrations").run(pool).await?;
    Ok(())
}
