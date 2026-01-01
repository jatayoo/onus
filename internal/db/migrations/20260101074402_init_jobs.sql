-- +goose Up
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    job_type TEXT NOT NULL,
    state TEXT NOT NULL,
    spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    error JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX jobs_client_id_idempotency_key_uq ON jobs (client_id, idempotency_key);

CREATE INDEX jobs_state_idx ON jobs (state);

-- +goose Down
