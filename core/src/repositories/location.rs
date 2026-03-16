use sqlx::PgPool;
use uuid::Uuid;

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

    /// Upsert a location by its deterministic UUID PK and return the id.
    pub async fn upsert(&self, loc: NewLocation) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_location (id, county, country, zip_code, fips_code, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO UPDATE
                 SET county     = EXCLUDED.county,
                     country    = EXCLUDED.country,
                     zip_code   = EXCLUDED.zip_code,
                     fips_code  = EXCLUDED.fips_code,
                     latitude   = EXCLUDED.latitude,
                     longitude  = EXCLUDED.longitude
             RETURNING id",
        )
        .bind(loc.id)
        .bind(loc.county)
        .bind(loc.country)
        .bind(loc.zip_code)
        .bind(loc.fips_code)
        .bind(loc.latitude)
        .bind(loc.longitude)
        .fetch_one(self.pool)
        .await?;

        Ok(id)
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<Location>> {
        let row = sqlx::query_as::<_, Location>("SELECT * FROM dim_location WHERE id = $1")
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
        Ok(row)
    }

    pub async fn list(&self, limit: i64, offset: i64) -> Result<Vec<Location>> {
        let rows = sqlx::query_as::<_, Location>(
            "SELECT * FROM dim_location ORDER BY county, zip_code LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    /// Upsert using an existing transaction.
    pub async fn upsert_with_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        loc: NewLocation,
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_location (id, county, country, zip_code, fips_code, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO UPDATE
                 SET county     = EXCLUDED.county,
                     country    = EXCLUDED.country,
                     zip_code   = EXCLUDED.zip_code,
                     fips_code  = EXCLUDED.fips_code,
                     latitude   = EXCLUDED.latitude,
                     longitude  = EXCLUDED.longitude
             RETURNING id",
        )
        .bind(loc.id)
        .bind(loc.county)
        .bind(loc.country)
        .bind(loc.zip_code)
        .bind(loc.fips_code)
        .bind(loc.latitude)
        .bind(loc.longitude)
        .fetch_one(&mut **tx)
        .await?;
        Ok(id)
    }
}
