-- Migration: 005_create_audit_log
-- Description: Create audit log and agent labels tables
-- Created: 2026-02-01

BEGIN;

-- Job audit log for tracking all state changes
CREATE TABLE IF NOT EXISTS job_audit_log (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL,
    
    -- State change
    old_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    
    -- Context
    changed_by VARCHAR(255),            -- Agent ID or user
    change_reason VARCHAR(500),         -- Optional reason/note
    
    -- Timestamp
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Metadata about the change
    metadata JSONB,
    
    CONSTRAINT fk_job_audit 
        FOREIGN KEY (job_id) 
        REFERENCES jobs(id) 
        ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_job_audit_log_job ON job_audit_log(job_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_audit_log_time ON job_audit_log(changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_audit_log_status ON job_audit_log(new_status, changed_at DESC);

-- Agent labels table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS agent_labels (
    agent_controller_id UUID NOT NULL,
    label_key VARCHAR(255) NOT NULL,
    label_value VARCHAR(500) NOT NULL,
    
    PRIMARY KEY (agent_controller_id, label_key),
    
    CONSTRAINT fk_agent_labels_controller 
        FOREIGN KEY (agent_controller_id) 
        REFERENCES agent_controllers(id) 
        ON DELETE CASCADE
);

-- Indexes for fast label-based agent lookup
CREATE INDEX IF NOT EXISTS idx_agent_labels_key_value ON agent_labels(label_key, label_value);
CREATE INDEX IF NOT EXISTS idx_agent_labels_agent ON agent_labels(agent_controller_id);

-- Comments
COMMENT ON TABLE job_audit_log IS 'Audit trail for all job state transitions';
COMMENT ON TABLE agent_labels IS 'Label-based routing for agent selection';

COMMIT;
