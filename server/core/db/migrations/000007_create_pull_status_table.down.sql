-- Drop trigger and function
DROP TRIGGER IF EXISTS trigger_update_pull_project_status_timestamp ON pull_project_status;
DROP FUNCTION IF EXISTS update_pull_project_status_timestamp();
DROP FUNCTION IF EXISTS cleanup_old_pull_status(INTEGER);

-- Drop indexes
DROP INDEX IF EXISTS idx_pull_project_status_lookup;
DROP INDEX IF EXISTS idx_pull_project_status_updated;

-- Drop table
DROP TABLE IF EXISTS pull_project_status;
