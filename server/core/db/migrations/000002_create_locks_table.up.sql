-- Migration: 002_create_locks_table
-- Description: Create locks table for distributed locking (replaces in-memory locks)
-- Created: 2026-02-01

BEGIN;

CREATE TABLE IF NOT EXISTS locks (
    id VARCHAR(500) PRIMARY KEY,        -- Format: "owner/repo/pull/workspace/project"
    
    -- Lock identifiers
    repo_full_name VARCHAR(255) NOT NULL,
    pull_num INTEGER NOT NULL,
    workspace VARCHAR(255) NOT NULL,
    project_name VARCHAR(255),
    
    -- Lock ownership
    locked_by VARCHAR(255) NOT NULL,    -- Username who acquired the lock
    locked_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Metadata
    lock_metadata JSONB,                -- Additional lock information
    
    -- For lock expiration (safety mechanism)
    expires_at TIMESTAMP                -- NULL = no expiration
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_locks_repo_pull ON locks(repo_full_name, pull_num);
CREATE INDEX IF NOT EXISTS idx_locks_locked_by ON locks(locked_by);
CREATE INDEX IF NOT EXISTS idx_locks_expires_at ON locks(expires_at) WHERE expires_at IS NOT NULL;

-- Function to auto-generate lock ID
CREATE OR REPLACE FUNCTION generate_lock_id(
    p_repo VARCHAR,
    p_pull INTEGER,
    p_workspace VARCHAR,
    p_project VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    IF p_project IS NULL OR p_project = '' THEN
        RETURN p_repo || '/' || p_pull || '/' || p_workspace;
    ELSE
        RETURN p_repo || '/' || p_pull || '/' || p_workspace || '/' || p_project;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper function to try acquiring a lock (advisory lock pattern)
CREATE OR REPLACE FUNCTION try_acquire_lock(
    p_lock_id VARCHAR,
    p_repo_full_name VARCHAR,
    p_pull_num INTEGER,
    p_workspace VARCHAR,
    p_project_name VARCHAR,
    p_locked_by VARCHAR,
    p_expires_at TIMESTAMP DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
    v_acquired BOOLEAN;
BEGIN
    -- Try to insert the lock
    INSERT INTO locks (
        id,
        repo_full_name,
        pull_num,
        workspace,
        project_name,
        locked_by,
        expires_at
    ) VALUES (
        p_lock_id,
        p_repo_full_name,
        p_pull_num,
        p_workspace,
        p_project_name,
        p_locked_by,
        p_expires_at
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING TRUE INTO v_acquired;
    
    -- If insert failed, lock is already held
    RETURN COALESCE(v_acquired, FALSE);
END;
$$ LANGUAGE plpgsql;

-- Function to release a lock
CREATE OR REPLACE FUNCTION release_lock(
    p_lock_id VARCHAR,
    p_locked_by VARCHAR DEFAULT NULL
) RETURNS BOOLEAN AS $$
DECLARE
    v_deleted BOOLEAN;
BEGIN
    -- Delete the lock (optionally verify ownership)
    DELETE FROM locks
    WHERE id = p_lock_id
        AND (p_locked_by IS NULL OR locked_by = p_locked_by)
    RETURNING TRUE INTO v_deleted;
    
    RETURN COALESCE(v_deleted, FALSE);
END;
$$ LANGUAGE plpgsql;

-- Cleanup function for expired locks
CREATE OR REPLACE FUNCTION cleanup_expired_locks() RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM locks
    WHERE expires_at IS NOT NULL
        AND expires_at < NOW()
    RETURNING COUNT(*) INTO v_deleted_count;
    
    RETURN COALESCE(v_deleted_count, 0);
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON TABLE locks IS 'Distributed locks for Terraform operations, replaces in-memory locking';
COMMENT ON COLUMN locks.id IS 'Composite key: owner/repo/pull/workspace/project';
COMMENT ON COLUMN locks.expires_at IS 'Optional lock expiration for safety (prevents orphaned locks)';
COMMENT ON FUNCTION try_acquire_lock IS 'Atomically try to acquire a lock, returns true if successful';
COMMENT ON FUNCTION release_lock IS 'Release a lock, optionally verifying ownership';
COMMENT ON FUNCTION cleanup_expired_locks IS 'Delete expired locks, returns count deleted';

COMMIT;
