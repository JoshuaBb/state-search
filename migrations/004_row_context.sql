CREATE TABLE fact_row_context (
    id          BIGSERIAL PRIMARY KEY,
    source_name TEXT        NOT NULL,
    attributes  JSONB       NOT NULL DEFAULT '{}',
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX fact_row_context_source_idx ON fact_row_context (source_name);
CREATE INDEX fact_row_context_attrs_idx  ON fact_row_context USING GIN (attributes);

ALTER TABLE fact_observations
    ADD COLUMN context_id BIGINT REFERENCES fact_row_context (id) ON DELETE SET NULL;
