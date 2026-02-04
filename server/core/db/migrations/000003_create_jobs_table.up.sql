-- Migration: 001_create_jobs_table
-- Description: Create jobs table for distributed job queue
-- Created: 2026-02-01

BEGIN;

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Repository and pull request info
    repo_full_name VARCHAR(255) NOT NULL,
    repo_clone_url VARCHAR(500) NOT NULL,
    pull_num INTEGER NOT NULL,
    pull_branch VARCHAR(255) NOT NULL,
    pull_base_branch VARCHAR(255) NOT NULL,
    pull_commit_sha VARCHAR(255) NOT NULL,
    
    -- Project and command info
    project_name VARCHAR(255),
    project_dir VARCHAR(500),
    workspace VARCHAR(255) NOT NULL DEFAULT 'default',
    command VARCHAR(50) NOT NULL CHECK (command IN ('plan', 'apply', 'unlock', 'version', 'import')),
    
    -- Job status
    status VARCHAR(50) NOT NULL DEFAULT 'queued' 
        CHECK (status IN ('queued', 'assigned', 'running', 'completed', 'failed', 'timeout', 'cancelled')),
    
    -- Agent assignment
    agent_controller_id UUID,
    agent_pod_name VARCHAR(255),
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    assigned_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Execution data
    plan_data BYTEA,                    -- Terraform plan binary
    output TEXT,                        -- Job execution output/logs
    error_message TEXT,                 -- Error details if failed
    exit_code INTEGER,                  -- Terraform exit code
    
    -- Configuration
    labels JSONB,                       -- For label-based routing
    env_vars JSONB,                     -- Environment variables (encrypted)
    workflow_config JSONB,              -- Custom workflow if specified
    terraform_version VARCHAR(50),      -- Required Terraform version
    timeout_seconds INTEGER DEFAULT 3600,
    
    -- VCS credentials (encrypted at application level)
    vcs_credentials_encrypted BYTEA,
    
    -- Metadata
    metadata JSONB,
    
    -- User who triggered the job
    triggered_by VARCHAR(255) NOT NULL,
    
    CONSTRAINT fk_agent_controller 
        FOREIGN KEY (agent_controller_id) 
        REFERENCES agent_controllers(id) 
        ON DELETE SET NULL
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_repo_pull ON jobs(repo_full_name, pull_num);
CREATE INDEX IF NOT EXISTS idx_jobs_agent_controller ON jobs(agent_controller_id) WHERE agent_controller_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_labels ON jobs USING GIN(labels) WHERE labels IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_completed_at ON jobs(completed_at) WHERE completed_at IS NOT NULL;

-- Partial index for active jobs
CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(status, created_at) 
    WHERE status IN ('queued', 'assigned', 'running');

-- Comments for documentation
COMMENT ON TABLE jobs IS 'Distributed job queue for Terraform operations';
COMMENT ON COLUMN jobs.plan_data IS 'Binary Terraform plan file, stored for apply operations';
COMMENT ON COLUMN jobs.labels IS 'JSON object for label-based agent routing, e.g., {"environment": "prod", "region": "us-east-1"}';
COMMENT ON COLUMN jobs.vcs_credentials_encrypted IS 'Encrypted VCS credentials for repo access';

COMMIT;
