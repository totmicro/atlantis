-- Table to track pull request project statuses
CREATE TABLE IF NOT EXISTS pull_project_status (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repo_full_name TEXT NOT NULL,
    pull_num INTEGER NOT NULL,
    workspace TEXT NOT NULL,
    repo_rel_dir TEXT NOT NULL,
    project_name TEXT NOT NULL DEFAULT '',
    status INTEGER NOT NULL, -- ProjectPlanStatus enum value
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicate project status entries
    CONSTRAINT unique_pull_project UNIQUE (repo_full_name, pull_num, workspace, repo_rel_dir, project_name)
);

-- Index for efficient pull status lookups
CREATE INDEX idx_pull_project_status_lookup ON pull_project_status(repo_full_name, pull_num);

-- Index for cleanup queries
CREATE INDEX idx_pull_project_status_updated ON pull_project_status(updated_at);

-- Function to update timestamp on status changes
CREATE OR REPLACE FUNCTION update_pull_project_status_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update timestamp
CREATE TRIGGER trigger_update_pull_project_status_timestamp
    BEFORE UPDATE ON pull_project_status
    FOR EACH ROW
    EXECUTE FUNCTION update_pull_project_status_timestamp();

-- Function to clean up old pull status entries (older than 30 days)
CREATE OR REPLACE FUNCTION cleanup_old_pull_status(
    p_days_old INTEGER DEFAULT 30
) RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM pull_project_status
    WHERE updated_at < NOW() - (p_days_old || ' days')::INTERVAL;
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;
