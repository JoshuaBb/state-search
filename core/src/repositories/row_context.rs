use sqlx::PgPool;

use crate::{
    error::Result,
    models::row_context::NewRowContext,
};

pub struct RowContextRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> RowContextRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Insert a context row and return its generated id.
    pub async fn create(&self, ctx: NewRowContext) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO fact_row_context (source_name, attributes)
             VALUES ($1, $2)
             RETURNING id",
        )
        .bind(ctx.source_name)
        .bind(ctx.attributes)
        .fetch_one(self.pool)
        .await?;

        Ok(id)
    }
}
