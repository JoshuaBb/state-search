use sqlx::PgPool;

use crate::{
    error::Result,
    models::location::{Location, NewLocation},
};

pub struct LocationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> LocationRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Upsert a location and return its id.
    pub async fn upsert(&self, loc: NewLocation) -> Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO dim_location (state_code, state_name, country, zip_code, fips_code, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (state_code, country, zip_code) DO UPDATE
                 SET state_name = EXCLUDED.state_name,
                     fips_code  = COALESCE(EXCLUDED.fips_code,  dim_location.fips_code),
                     latitude   = COALESCE(EXCLUDED.latitude,   dim_location.latitude),
                     longitude  = COALESCE(EXCLUDED.longitude,  dim_location.longitude)
             RETURNING id",
        )
        .bind(loc.state_code)
        .bind(loc.state_name)
        .bind(loc.country)
        .bind(loc.zip_code)
        .bind(loc.fips_code)
        .bind(loc.latitude)
        .bind(loc.longitude)
        .fetch_one(self.pool)
        .await?;

        Ok(id)
    }

    pub async fn find_by_id(&self, id: i64) -> Result<Option<Location>> {
        let row = sqlx::query_as::<_, Location>("SELECT * FROM dim_location WHERE id = $1")
            .bind(id)
            .fetch_optional(self.pool)
            .await?;

        Ok(row)
    }

    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<Location>> {
        let rows = sqlx::query_as::<_, Location>(
            "SELECT * FROM dim_location ORDER BY state_code, zip_code LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }
}
