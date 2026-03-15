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

    /// Bulk insert observations using a single UNNEST query — one round-trip
    /// regardless of how many observations are in the batch.
    pub async fn bulk_create(&self, observations: Vec<NewObservation>) -> Result<u64> {
        if observations.is_empty() {
            return Ok(0);
        }

        let len = observations.len();
        let mut raw_import_ids  = Vec::with_capacity(len);
        let mut location_ids    = Vec::with_capacity(len);
        let mut time_ids        = Vec::with_capacity(len);
        let mut source_names    = Vec::with_capacity(len);
        let mut metric_names    = Vec::with_capacity(len);
        let mut metric_values   = Vec::with_capacity(len);
        let mut attributes_list = Vec::with_capacity(len);
        let mut ingest_run_ids  = Vec::with_capacity(len);

        for obs in observations {
            raw_import_ids .push(obs.raw_import_id);
            location_ids   .push(obs.location_id);
            time_ids       .push(obs.time_id);
            source_names   .push(obs.source_name);
            metric_names   .push(obs.metric_name);
            metric_values  .push(obs.metric_value);
            attributes_list.push(obs.attributes);
            ingest_run_ids .push(obs.ingest_run_id);
        }

        sqlx::query(
            "INSERT INTO fact_observations
                 (raw_import_id, location_id, time_id, source_name, metric_name, metric_value, attributes, ingest_run_id)
             SELECT * FROM UNNEST(
                 $1::bigint[], $2::bigint[], $3::bigint[], $4::text[],
                 $5::text[], $6::float8[], $7::jsonb[], $8::uuid[]
             )",
        )
        .bind(&raw_import_ids)
        .bind(&location_ids)
        .bind(&time_ids)
        .bind(&source_names)
        .bind(&metric_names)
        .bind(&metric_values)
        .bind(&attributes_list)
        .bind(&ingest_run_ids)
        .execute(self.pool)
        .await?;

        Ok(len as u64)
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
