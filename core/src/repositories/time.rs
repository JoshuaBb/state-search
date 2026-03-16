use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::Result,
    models::time::{NewTimePeriod, TimePeriod},
};

pub struct TimeRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> TimeRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert(&self, t: NewTimePeriod) -> Result<Uuid> {
        let date_floor = t.date_floor();
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_time (id, year, quarter, month, day, date_floor)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
                 SET year       = EXCLUDED.year,
                     quarter    = EXCLUDED.quarter,
                     month      = EXCLUDED.month,
                     day        = EXCLUDED.day,
                     date_floor = EXCLUDED.date_floor
             RETURNING id",
        )
        .bind(t.id)
        .bind(t.year)
        .bind(t.quarter)
        .bind(t.month)
        .bind(t.day)
        .bind(date_floor)
        .fetch_one(self.pool)
        .await?;
        Ok(id)
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<TimePeriod>> {
        let row = sqlx::query_as::<_, TimePeriod>("SELECT * FROM dim_time WHERE id = $1")
            .bind(id)
            .fetch_optional(self.pool)
            .await?;
        Ok(row)
    }

    pub async fn upsert_with_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        t: NewTimePeriod,
    ) -> Result<Uuid> {
        let date_floor = t.date_floor();
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO dim_time (id, year, quarter, month, day, date_floor)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
                 SET year       = EXCLUDED.year,
                     quarter    = EXCLUDED.quarter,
                     month      = EXCLUDED.month,
                     day        = EXCLUDED.day,
                     date_floor = EXCLUDED.date_floor
             RETURNING id",
        )
        .bind(t.id)
        .bind(t.year)
        .bind(t.quarter)
        .bind(t.month)
        .bind(t.day)
        .bind(date_floor)
        .fetch_one(&mut **tx)
        .await?;
        Ok(id)
    }
}
