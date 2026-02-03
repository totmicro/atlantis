-- Add Priority and AttemptCount fields to jobs table for queue and retry support
-- Migration: 000006_add_job_priority_and_retry.up.sql

BEGIN;

-- Add priority field (0-100, higher = more important)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 50;

-- Add attempt_count for retry tracking
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

-- Add index on priority for efficient queue queries
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC, created_at ASC) WHERE status = 'queued';

-- Add index on attempt_count for retry queries
CREATE INDEX IF NOT EXISTS idx_jobs_attempts ON jobs(attempt_count, status);

-- Add comment for priority
COMMENT ON COLUMN jobs.priority IS 'Job priority for queue ordering (0-100, higher = more important)';

-- Add comment for attempt_count
COMMENT ON COLUMN jobs.attempt_count IS 'Number of execution attempts for retry tracking';

COMMIT;
