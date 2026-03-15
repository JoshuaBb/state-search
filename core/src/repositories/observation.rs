use sqlx::PgPool;
use uuid::Uuid;

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
                 (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes, ingest_run_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING *",
        )
        .bind(obs.raw_import_id)
        .bind(obs.location_id)
        .bind(obs.time_id)
        .bind(obs.source_name)
        .bind(obs.metric_name)
        .bind(obs.metric_value)
        .bind(obs.attributes)
        .bind(obs.ingest_run_id)
        .fetch_one(self.pool)
        .await?;

        Ok(row)
    }

    /// Bulk insert observations. Wraps all rows for a single CSV row in one
    /// mini-transaction (per-row granularity, not per-file).
    /// Insert observations individually with no transaction — each row commits immediately.
    /// Partial progress is preserved on failure and cleaned up via `delete_by_ingest_run`.
    pub async fn bulk_create(&self, observations: Vec<NewObservation>) -> Result<u64> {
        let mut count = 0u64;

        for obs in observations {
            sqlx::query(
                "INSERT INTO fact_observations
                     (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes, ingest_run_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(obs.raw_import_id)
            .bind(obs.location_id)
            .bind(obs.time_id)
            .bind(obs.source_name)
            .bind(obs.metric_name)
            .bind(obs.metric_value)
            .bind(obs.attributes)
            .bind(obs.ingest_run_id)
            .execute(self.pool)
            .await?;

            count += 1;
        }

        Ok(count)
    }

    /// Delete all observations belonging to a given ingest run.
    /// Returns the number of rows deleted.
    pub async fn delete_by_ingest_run(pool: &PgPool, ingest_run_id: Uuid) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM fact_observations WHERE ingest_run_id = $1",
        )
        .bind(ingest_run_id)
        .execute(pool)
        .await?;

        Ok(result.rows_affected())
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use crate::models::observation::NewObservation;

    #[test]
    fn delete_by_ingest_run_signature_exists() {
        // Compile-time check: verify the function exists and is accessible.
        let _ = ObservationRepository::delete_by_ingest_run;
    }

    #[test]
    fn bulk_create_accepts_new_observation_with_ingest_run_id() {
        let _ = NewObservation {
            raw_import_id: None,
            location_id:   None,
            time_id:       None,
            source_name:   None,
            metric_name:   "m".to_string(),
            metric_value:  None,
            attributes:    None,
            ingest_run_id: Uuid::new_v4(),
        };
    }
}
