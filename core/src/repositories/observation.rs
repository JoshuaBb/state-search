use sqlx::PgPool;

use crate::{
    error::Result,
    models::observation::{NewObservation, Observation},
};

pub struct ObservationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ObservationRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, obs: NewObservation) -> Result<Observation> {
        let row = sqlx::query_as::<_, Observation>(
            "INSERT INTO fact_observations
                 (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING *",
        )
        .bind(obs.raw_import_id)
        .bind(obs.location_id)
        .bind(obs.time_id)
        .bind(obs.source_name)
        .bind(obs.metric_name)
        .bind(obs.metric_value)
        .bind(obs.attributes)
        .fetch_one(self.pool)
        .await?;

        Ok(row)
    }

    pub async fn bulk_create(&self, observations: Vec<NewObservation>) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let mut count = 0u64;

        for obs in observations {
            sqlx::query(
                "INSERT INTO fact_observations
                     (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(obs.raw_import_id)
            .bind(obs.location_id)
            .bind(obs.time_id)
            .bind(obs.source_name)
            .bind(obs.metric_name)
            .bind(obs.metric_value)
            .bind(obs.attributes)
            .execute(&mut *tx)
            .await?;

            count += 1;
        }

        tx.commit().await?;
        Ok(count)
    }

    /// Bulk insert observations using an existing transaction (no internal begin/commit).
    pub async fn bulk_create_with_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        observations: Vec<NewObservation>,
    ) -> Result<u64> {
        let mut count = 0u64;
        for obs in observations {
            sqlx::query(
                "INSERT INTO fact_observations
                     (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(obs.raw_import_id)
            .bind(obs.location_id)
            .bind(obs.time_id)
            .bind(obs.source_name)
            .bind(obs.metric_name)
            .bind(obs.metric_value)
            .bind(obs.attributes)
            .execute(&mut **tx)
            .await?;
            count += 1;
        }
        Ok(count)
    }

    pub async fn query(
        &self,
        metric_name: Option<&str>,
        source_name: Option<&str>,
        location_id: Option<i64>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Observation>> {
        let rows = sqlx::query_as::<_, Observation>(
            "SELECT * FROM fact_observations
             WHERE ($1::text IS NULL OR metric_name = $1)
               AND ($2::text IS NULL OR source_name = $2)
               AND ($3::bigint IS NULL OR location_id = $3)
             ORDER BY id
             LIMIT $4 OFFSET $5",
        )
        .bind(metric_name)
        .bind(source_name)
        .bind(location_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }
}
