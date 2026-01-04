-- +goose Up
CREATE TABLE artifacts (
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, type)
);

CREATE INDEX artifacts_job_id_idx ON artifacts (job_id);

-- +goose Down
DROP TABLE IF EXISTS artifacts;