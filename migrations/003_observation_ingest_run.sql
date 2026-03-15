-- migrations/003_observation_ingest_run.sql
ALTER TABLE fact_observations
  ADD COLUMN ingest_run_id UUID,
  ADD COLUMN inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX fact_ingest_run_idx ON fact_observations (ingest_run_id);
