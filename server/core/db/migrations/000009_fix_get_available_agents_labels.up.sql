-- Fix get_available_agents to return labels column
-- Must drop and recreate because PostgreSQL doesn't allow changing return type
DROP FUNCTION IF EXISTS get_available_agents(JSONB, INTEGER);

CREATE FUNCTION get_available_agents(
    p_required_labels JSONB DEFAULT NULL,
    p_limit INTEGER DEFAULT 10
) RETURNS TABLE (
    id UUID,
    name VARCHAR,
    cluster_name VARCHAR,
    labels JSONB,
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
        ac.labels,
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
