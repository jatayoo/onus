-- +goose Up
ALTER TABLE jobs ADD COLUMN claimed_by TEXT;
ALTER TABLE jobs ADD COLUMN claimed_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN claim_expires_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_claimed_by ON jobs(state, created_at) WHERE state = 'QUEUED';
