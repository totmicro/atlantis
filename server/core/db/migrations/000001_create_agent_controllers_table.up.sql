-- Migration: 003_create_agent_controllers_table
-- Description: Create agent_controllers table for agent registry
-- Created: 2026-02-01

BEGIN;

CREATE TABLE IF NOT EXISTS agent_controllers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Controller identification
    name VARCHAR(255) UNIQUE NOT NULL,          -- Friendly name
    token_hash VARCHAR(255) NOT NULL,           -- bcrypt hash of auth token
    
    -- Kubernetes cluster info
    cluster_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    
    -- Routing labels
    labels JSONB,                               -- e.g., {"region": "us-east-1", "env": "prod"}
    
    -- Capacity and health
    capacity INTEGER NOT NULL DEFAULT 10,       -- Max concurrent jobs
    current_jobs INTEGER NOT NULL DEFAULT 0,    -- Currently executing jobs
    status VARCHAR(50) NOT NULL DEFAULT 'active' 
        CHECK (status IN ('active', 'draining', 'offline', 'unhealthy')),
    
    -- Timestamps
    registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMP NOT NULL DEFAULT NOW(), -- Updated on any activity
    
    -- Version info
    atlantis_version VARCHAR(50),
    
    -- Metadata
    metadata JSONB,
    
    -- Constraints
    CONSTRAINT capacity_positive CHECK (capacity > 0),
    CONSTRAINT current_jobs_valid CHECK (current_jobs >= 0 AND current_jobs <= capacity)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_agent_controllers_status ON agent_controllers(status);
CREATE INDEX IF NOT EXISTS idx_agent_controllers_heartbeat ON agent_controllers(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_agent_controllers_labels ON agent_controllers USING GIN(labels) WHERE labels IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agent_controllers_cluster ON agent_controllers(cluster_name, namespace);

-- Partial index for active agents (removed NOW() which is not immutable)
CREATE INDEX IF NOT EXISTS idx_agent_controllers_active ON agent_controllers(status, last_heartbeat, capacity, current_jobs)
    WHERE status = 'active';

-- Function to update heartbeat
CREATE OR REPLACE FUNCTION update_agent_heartbeat(
    p_controller_id UUID,
    p_current_jobs INTEGER DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE agent_controllers
    SET 
        last_heartbeat = NOW(),
        last_seen = NOW(),
        current_jobs = COALESCE(p_current_jobs, current_jobs),
        -- Auto-recover from unhealthy if heartbeat received
        status = CASE 
            WHEN status = 'unhealthy' THEN 'active'
            ELSE status
        END
    WHERE id = p_controller_id;
END;
$$ LANGUAGE plpgsql;

-- Function to mark stale agents as unhealthy
CREATE OR REPLACE FUNCTION mark_stale_agents_unhealthy(
    p_threshold_seconds INTEGER DEFAULT 60
) RETURNS INTEGER AS $$
DECLARE
    v_updated_count INTEGER;
BEGIN
    UPDATE agent_controllers
    SET status = 'unhealthy'
    WHERE status IN ('active', 'draining')
        AND last_heartbeat < NOW() - (p_threshold_seconds || ' seconds')::INTERVAL
    RETURNING COUNT(*) INTO v_updated_count;
    
    RETURN COALESCE(v_updated_count, 0);
END;
$$ LANGUAGE plpgsql;

-- Function to get available agents for job assignment
CREATE OR REPLACE FUNCTION get_available_agents(
    p_required_labels JSONB DEFAULT NULL,
    p_limit INTEGER DEFAULT 10
) RETURNS TABLE (
    id UUID,
    name VARCHAR,
    cluster_name VARCHAR,
    current_jobs INTEGER,
    capacity INTEGER,
    available_slots INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ac.id,
        ac.name,
        ac.cluster_name,
        ac.current_jobs,
        ac.capacity,
        (ac.capacity - ac.current_jobs) AS available_slots
    FROM agent_controllers ac
    WHERE ac.status = 'active'
        AND ac.last_heartbeat > NOW() - INTERVAL '1 minute'
        AND ac.current_jobs < ac.capacity
        -- Label matching (if required_labels specified)
        AND (
            p_required_labels IS NULL 
            OR ac.labels @> p_required_labels
        )
    ORDER BY 
        (ac.capacity - ac.current_jobs) DESC,  -- Prefer agents with more capacity
        ac.last_heartbeat DESC                  -- Then by most recent heartbeat
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to increment job count
CREATE OR REPLACE FUNCTION increment_agent_jobs(
    p_controller_id UUID
) RETURNS VOID AS $$
BEGIN
    UPDATE agent_controllers
    SET current_jobs = current_jobs + 1,
        last_seen = NOW()
    WHERE id = p_controller_id
        AND current_jobs < capacity;  -- Safety check
END;
$$ LANGUAGE plpgsql;

-- Function to decrement job count
CREATE OR REPLACE FUNCTION decrement_agent_jobs(
    p_controller_id UUID
) RETURNS VOID AS $$
BEGIN
    UPDATE agent_controllers
    SET current_jobs = GREATEST(current_jobs - 1, 0),
        last_seen = NOW()
    WHERE id = p_controller_id;
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON TABLE agent_controllers IS 'Registry of agent controllers across Kubernetes clusters';
COMMENT ON COLUMN agent_controllers.labels IS 'JSON labels for job routing, e.g., {"region": "us-east-1", "tier": "production"}';
COMMENT ON COLUMN agent_controllers.status IS 'active: accepting jobs, draining: no new jobs, offline: deregistered, unhealthy: missed heartbeats';
COMMENT ON FUNCTION get_available_agents IS 'Returns agents with available capacity, optionally filtered by labels';
COMMENT ON FUNCTION mark_stale_agents_unhealthy IS 'Marks agents as unhealthy if heartbeat is older than threshold';

COMMIT;
