-- +goose Up
-- Add request hash column to track request payload fingerprint
ALTER TABLE jobs ADD COLUMN request_hash TEXT NOT NULL DEFAULT '';


-- Create index for performance
CREATE INDEX jobs_request_hash_idx ON jobs (request_hash);

-- Backfill existing rows (one-time)
COMMENT ON COLUMN jobs.request_hash IS 'Hash of the request payload for idempotency verification';

-- +goose Down
DROP INDEX IF EXISTS jobs_request_hash_idx;
ALTER TABLE jobs DROP COLUMN request_hash;
