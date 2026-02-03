-- Rollback priority and attempt_count fields from jobs table
-- Migration: 000006_add_job_priority_and_retry.down.sql

-- Drop indexes first
DROP INDEX IF EXISTS idx_jobs_attempts;
DROP INDEX IF EXISTS idx_jobs_priority;

-- Drop columns
ALTER TABLE jobs DROP COLUMN IF EXISTS attempt_count;
ALTER TABLE jobs DROP COLUMN IF EXISTS priority;
