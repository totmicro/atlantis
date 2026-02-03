-- Rollback: Remove project_context_json from jobs table

BEGIN;

DROP INDEX IF EXISTS idx_jobs_project_context;
ALTER TABLE jobs DROP COLUMN IF EXISTS project_context_json;

COMMIT;
