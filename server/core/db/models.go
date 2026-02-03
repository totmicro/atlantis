package db

import (
	"encoding/json"
	"time"
)

// Job represents a Terraform operation job in the distributed queue
type Job struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`

	// Repository and pull request info
	RepoFullName   string `json:"repo_full_name"`
	RepoCloneURL   string `json:"repo_clone_url"`
	PullNum        int    `json:"pull_num"`
	PullBranch     string `json:"pull_branch"`
	PullBaseBranch string `json:"pull_base_branch"`
	PullCommitSHA  string `json:"pull_commit_sha"`

	// Project and command info
	ProjectName string `json:"project_name,omitempty"`
	ProjectDir  string `json:"project_dir,omitempty"`
	Workspace   string `json:"workspace"`
	Command     string `json:"command"` // plan, apply, unlock, etc.

	// Full context for execution (serialized command.ProjectContext)
	// This contains everything needed to reconstruct the execution environment
	ProjectContextJSON json.RawMessage `json:"project_context_json,omitempty"`

	// Job status
	Status string `json:"status"` // queued, assigned, running, completed, failed, timeout, cancelled

	// Agent assignment
	AgentControllerID *string    `json:"agent_controller_id,omitempty"`
	AgentPodName      *string    `json:"agent_pod_name,omitempty"`
	AssignedAt        *time.Time `json:"assigned_at,omitempty"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`

	// Execution data
	PlanData     []byte  `json:"plan_data,omitempty"`
	Output       *string `json:"output,omitempty"`
	ErrorMessage *string `json:"error_message,omitempty"`
	ExitCode     *int    `json:"exit_code,omitempty"`

	// Configuration
	Labels            map[string]string `json:"labels,omitempty"`
	EnvVars           map[string]string `json:"env_vars,omitempty"`
	WorkflowConfig    json.RawMessage   `json:"workflow_config,omitempty"`
	TerraformVersion  *string           `json:"terraform_version,omitempty"`
	TimeoutSeconds    int               `json:"timeout_seconds"`
	VCSCredentialsEnc []byte            `json:"vcs_credentials_encrypted,omitempty"`
	Metadata          json.RawMessage   `json:"metadata,omitempty"`
	TriggeredBy       string            `json:"triggered_by"`

	// Queue and retry support
	Priority     int `json:"priority"`      // 0-100, higher = more important
	AttemptCount int `json:"attempt_count"` // Number of execution attempts
}

// Lock represents a distributed lock for Terraform operations
type Lock struct {
	ID           string          `json:"id"` // Format: owner/repo/pull/workspace/project
	RepoFullName string          `json:"repo_full_name"`
	PullNum      int             `json:"pull_num"`
	Workspace    string          `json:"workspace"`
	ProjectName  *string         `json:"project_name,omitempty"`
	LockedBy     string          `json:"locked_by"`
	LockedAt     time.Time       `json:"locked_at"`
	ExpiresAt    *time.Time      `json:"expires_at,omitempty"`
	Metadata     json.RawMessage `json:"lock_metadata,omitempty"`
}

// AgentController represents an agent controller registration
type AgentController struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	TokenHash       string            `json:"-"` // Never expose in JSON
	ClusterName     string            `json:"cluster_name"`
	Namespace       string            `json:"namespace"`
	Labels          map[string]string `json:"labels,omitempty"`
	Capacity        int               `json:"capacity"`
	CurrentJobs     int               `json:"current_jobs"`
	Status          string            `json:"status"` // active, draining, offline, unhealthy
	RegisteredAt    time.Time         `json:"registered_at"`
	LastHeartbeat   time.Time         `json:"last_heartbeat"`
	LastSeen        time.Time         `json:"last_seen"`
	AtlantisVersion string            `json:"atlantis_version,omitempty"`
	Metadata        json.RawMessage   `json:"metadata,omitempty"`
}

// PlanMetadata represents metadata about a Terraform plan
type PlanMetadata struct {
	JobID         string          `json:"job_id"`
	PlanSizeBytes int64           `json:"plan_size_bytes"`
	PlanHash      string          `json:"plan_hash,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	AccessedAt    *time.Time      `json:"accessed_at,omitempty"`
	ExpiresAt     *time.Time      `json:"expires_at,omitempty"`
	Applied       bool            `json:"applied"`
	AppliedAt     *time.Time      `json:"applied_at,omitempty"`
	Deleted       bool            `json:"deleted"`
	DeletedAt     *time.Time      `json:"deleted_at,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

// JobStatus represents valid job statuses
type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusAssigned  JobStatus = "assigned"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusTimeout   JobStatus = "timeout"
	JobStatusCancelled JobStatus = "cancelled"
)

// CommandType represents valid Terraform commands
type CommandType string

const (
	CommandPlan    CommandType = "plan"
	CommandApply   CommandType = "apply"
	CommandUnlock  CommandType = "unlock"
	CommandVersion CommandType = "version"
	CommandImport  CommandType = "import"
)

// AgentStatus represents valid agent controller statuses
type AgentStatus string

const (
	AgentStatusActive    AgentStatus = "active"
	AgentStatusDraining  AgentStatus = "draining"
	AgentStatusOffline   AgentStatus = "offline"
	AgentStatusUnhealthy AgentStatus = "unhealthy"
)
