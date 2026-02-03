-- Migration: Add project_context_json to jobs table
-- Description: Store full ProjectContext for agent execution
-- Created: 2026-02-02

BEGIN;

-- Add column for full project context (allows agent to reconstruct execution environment)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS project_context_json JSONB;

-- Add index for faster retrieval
CREATE INDEX IF NOT EXISTS idx_jobs_project_context ON jobs USING GIN(project_context_json) WHERE project_context_json IS NOT NULL;

COMMENT ON COLUMN jobs.project_context_json IS 'Serialized command.ProjectContext - full execution context for agent';

COMMIT;
