-- Migration: 004_create_plan_metadata_table
-- Description: Track plan metadata for automatic cleanup
-- Created: 2026-02-01

BEGIN;

-- Enable pgcrypto extension for digest function (used for plan hashing)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS plan_metadata (
    job_id UUID PRIMARY KEY,
    
    -- Plan details
    plan_size_bytes BIGINT NOT NULL,
    plan_hash VARCHAR(64),                      -- SHA256 hash of plan
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    accessed_at TIMESTAMP,                      -- Last access (for apply)
    expires_at TIMESTAMP,                       -- NULL = keep until apply, otherwise auto-delete
    
    -- Lifecycle
    applied BOOLEAN NOT NULL DEFAULT FALSE,     -- True after successful apply
    applied_at TIMESTAMP,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    
    -- Metadata
    metadata JSONB,
    
    CONSTRAINT fk_job 
        FOREIGN KEY (job_id) 
        REFERENCES jobs(id) 
        ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_plan_metadata_expires ON plan_metadata(expires_at) 
    WHERE expires_at IS NOT NULL AND deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_plan_metadata_unapplied ON plan_metadata(created_at) 
    WHERE applied = FALSE AND deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_plan_metadata_size ON plan_metadata(plan_size_bytes);

-- Function to mark plan as accessed (e.g., during apply)
CREATE OR REPLACE FUNCTION mark_plan_accessed(
    p_job_id UUID
) RETURNS VOID AS $$
BEGIN
    UPDATE plan_metadata
    SET accessed_at = NOW()
    WHERE job_id = p_job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to mark plan as applied
CREATE OR REPLACE FUNCTION mark_plan_applied(
    p_job_id UUID
) RETURNS VOID AS $$
BEGIN
    UPDATE plan_metadata
    SET 
        applied = TRUE,
        applied_at = NOW()
    WHERE job_id = p_job_id;
    
    -- Also delete the plan data from jobs table to free space
    UPDATE jobs
    SET plan_data = NULL
    WHERE id = p_job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old plans
CREATE OR REPLACE FUNCTION cleanup_old_plans(
    p_retention_days INTEGER DEFAULT 7
) RETURNS TABLE (
    deleted_count INTEGER,
    freed_bytes BIGINT
) AS $$
DECLARE
    v_deleted_count INTEGER;
    v_freed_bytes BIGINT;
BEGIN
    -- Delete plans that are:
    -- 1. Expired (expires_at < now)
    -- 2. Applied and older than retention period
    -- 3. Failed jobs older than retention period
    -- 4. Stale unapplied plans older than 2x retention period
    
    WITH deleted_plans AS (
        DELETE FROM plan_metadata pm
        USING jobs j
        WHERE pm.job_id = j.id
            AND pm.deleted = FALSE
            AND (
                -- Expired plans
                (pm.expires_at IS NOT NULL AND pm.expires_at < NOW())
                -- Applied plans older than retention
                OR (pm.applied = TRUE AND pm.applied_at < NOW() - (p_retention_days || ' days')::INTERVAL)
                -- Failed jobs older than retention
                OR (j.status = 'failed' AND j.completed_at < NOW() - (p_retention_days || ' days')::INTERVAL)
                -- Very stale unapplied plans (2x retention)
                OR (pm.applied = FALSE AND pm.created_at < NOW() - ((p_retention_days * 2) || ' days')::INTERVAL)
            )
        RETURNING pm.job_id, pm.plan_size_bytes
    )
    SELECT 
        COUNT(*)::INTEGER,
        COALESCE(SUM(plan_size_bytes), 0)
    INTO v_deleted_count, v_freed_bytes
    FROM deleted_plans;
    
    -- Also delete the plan_data from jobs table
    UPDATE jobs j
    SET plan_data = NULL
    FROM (SELECT job_id FROM deleted_plans) dp
    WHERE j.id = dp.job_id;
    
    -- Mark metadata as deleted
    UPDATE plan_metadata pm
    SET 
        deleted = TRUE,
        deleted_at = NOW()
    WHERE pm.job_id IN (SELECT job_id FROM deleted_plans);
    
    RETURN QUERY SELECT v_deleted_count, v_freed_bytes;
END;
$$ LANGUAGE plpgsql;

-- Function to get plan storage statistics
CREATE OR REPLACE FUNCTION get_plan_storage_stats() 
RETURNS TABLE (
    total_plans BIGINT,
    total_size_bytes BIGINT,
    total_size_mb NUMERIC,
    applied_plans BIGINT,
    unapplied_plans BIGINT,
    expired_plans BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT AS total_plans,
        COALESCE(SUM(plan_size_bytes), 0)::BIGINT AS total_size_bytes,
        ROUND(COALESCE(SUM(plan_size_bytes), 0)::NUMERIC / 1024 / 1024, 2) AS total_size_mb,
        COUNT(*) FILTER (WHERE applied = TRUE)::BIGINT AS applied_plans,
        COUNT(*) FILTER (WHERE applied = FALSE)::BIGINT AS unapplied_plans,
        COUNT(*) FILTER (WHERE expires_at IS NOT NULL AND expires_at < NOW())::BIGINT AS expired_plans
    FROM plan_metadata
    WHERE deleted = FALSE;
END;
$$ LANGUAGE plpgsql;

-- Trigger to create plan_metadata when plan_data is inserted
CREATE OR REPLACE FUNCTION create_plan_metadata_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Only create metadata if plan_data is being set
    IF NEW.plan_data IS NOT NULL AND (OLD.plan_data IS NULL OR OLD.plan_data IS DISTINCT FROM NEW.plan_data) THEN
        INSERT INTO plan_metadata (
            job_id,
            plan_size_bytes,
            plan_hash,
            expires_at
        ) VALUES (
            NEW.id,
            LENGTH(NEW.plan_data),
            encode(digest(NEW.plan_data, 'sha256'), 'hex'),
            -- Set default expiration to 7 days from now
            NOW() + INTERVAL '7 days'
        )
        ON CONFLICT (job_id) DO UPDATE SET
            plan_size_bytes = LENGTH(NEW.plan_data),
            plan_hash = encode(digest(NEW.plan_data, 'sha256'), 'hex'),
            created_at = NOW(),
            expires_at = NOW() + INTERVAL '7 days';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if it exists before creating
DROP TRIGGER IF EXISTS trigger_create_plan_metadata ON jobs;

CREATE TRIGGER trigger_create_plan_metadata
    AFTER INSERT OR UPDATE OF plan_data ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION create_plan_metadata_trigger();

-- Comments
COMMENT ON TABLE plan_metadata IS 'Tracks Terraform plan metadata for lifecycle management and cleanup';
COMMENT ON COLUMN plan_metadata.expires_at IS 'Auto-delete after this time. NULL = keep until applied';
COMMENT ON FUNCTION cleanup_old_plans IS 'Deletes old plans and returns deleted count and freed bytes';
COMMENT ON FUNCTION get_plan_storage_stats IS 'Returns statistics about plan storage usage';

COMMIT;
