package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

// JobStore handles job CRUD operations
type JobStore interface {
	Create(ctx context.Context, job *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	UpdateStatus(ctx context.Context, id string, status JobStatus) error
	UpdateResult(ctx context.Context, id string, status JobStatus, output string, planData []byte, exitCode int, errorMsg string) error
	GetQueued(ctx context.Context, limit int) ([]*Job, error)
	GetByAgentAndStatus(ctx context.Context, agentID string, status string) ([]*Job, error)
	AssignToAgent(ctx context.Context, jobID, agentControllerID string) error
	UpdateAgentPod(ctx context.Context, jobID, podName string) error
	SavePlan(ctx context.Context, jobID string, plan []byte) error
	GetPlan(ctx context.Context, jobID string) ([]byte, error)
	DeletePlan(ctx context.Context, jobID string) error
	SetOutput(ctx context.Context, jobID string, output string) error
	SetError(ctx context.Context, jobID string, errorMsg string, exitCode int) error
	Complete(ctx context.Context, jobID string, status JobStatus) error
	ListByRepo(ctx context.Context, repoFullName string, pullNum int) ([]*Job, error)
	IncrementAttemptCount(ctx context.Context, jobID string) error
}

// PostgresJobStore implements JobStore using PostgreSQL
type PostgresJobStore struct {
	db *sql.DB
}

// NewPostgresJobStore creates a new PostgreSQL job store
func NewPostgresJobStore(db *sql.DB) *PostgresJobStore {
	return &PostgresJobStore{db: db}
}

// Create inserts a new job
func (s *PostgresJobStore) Create(ctx context.Context, job *Job) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	if job.TimeoutSeconds == 0 {
		job.TimeoutSeconds = 3600 // Default 1 hour
	}
	if job.Workspace == "" {
		job.Workspace = "default"
	}

	// Marshal JSON fields
	labelsJSON, err := json.Marshal(job.Labels)
	if err != nil {
		return fmt.Errorf("marshaling labels: %w", err)
	}
	envVarsJSON, err := json.Marshal(job.EnvVars)
	if err != nil {
		return fmt.Errorf("marshaling env vars: %w", err)
	}

	// Handle WorkflowConfig - convert to proper type for PostgreSQL JSONB
	var workflowConfigParam interface{}
	if job.WorkflowConfig != nil && len(job.WorkflowConfig) > 0 {
		workflowConfigParam = job.WorkflowConfig
	} else {
		workflowConfigParam = nil
	}

	// Handle Metadata - convert to proper type for PostgreSQL JSONB
	var metadataParam interface{}
	if job.Metadata != nil && len(job.Metadata) > 0 {
		metadataParam = job.Metadata
	} else {
		metadataParam = nil
	}

	query := `
		INSERT INTO jobs (
			id, repo_full_name, repo_clone_url, pull_num, pull_branch, pull_base_branch, 
			pull_commit_sha, project_name, project_dir, workspace, command, status, 
			created_at, labels, env_vars, workflow_config, terraform_version, 
			timeout_seconds, vcs_credentials_encrypted, metadata, triggered_by,
			priority, attempt_count, project_context_json
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
		)
	`

	// Debug log all JSON values
	log.Printf("DEBUG job_store.Create: labelsJSON=%s envVarsJSON=%s workflowConfigParam=%v metadataParam=%v",
		string(labelsJSON), string(envVarsJSON), workflowConfigParam, metadataParam)

	_, err = s.db.ExecContext(ctx, query,
		job.ID, job.RepoFullName, job.RepoCloneURL, job.PullNum, job.PullBranch,
		job.PullBaseBranch, job.PullCommitSHA, stringOrNil(job.ProjectName),
		stringOrNil(job.ProjectDir), job.Workspace, job.Command, job.Status,
		job.CreatedAt, labelsJSON, envVarsJSON, workflowConfigParam,
		job.TerraformVersion, job.TimeoutSeconds, job.VCSCredentialsEnc,
		metadataParam, job.TriggeredBy, job.Priority, job.AttemptCount,
		job.ProjectContextJSON,
	)

	if err != nil {
		return fmt.Errorf("inserting job: %w", err)
	}

	return nil
}

// Get retrieves a job by ID
func (s *PostgresJobStore) Get(ctx context.Context, id string) (*Job, error) {
	query := `
		SELECT id, repo_full_name, repo_clone_url, pull_num, pull_branch, pull_base_branch,
			pull_commit_sha, project_name, project_dir, workspace, command, status,
			agent_controller_id, agent_pod_name, created_at, assigned_at, started_at, 
			completed_at, plan_data, output, error_message, exit_code, labels, env_vars,
			workflow_config, terraform_version, timeout_seconds, vcs_credentials_encrypted,
			metadata, triggered_by, priority, attempt_count, project_context_json
		FROM jobs
		WHERE id = $1
	`

	job := &Job{}
	var labelsJSON, envVarsJSON, workflowConfigJSON, metadataJSON, projectContextJSON []byte

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&job.ID, &job.RepoFullName, &job.RepoCloneURL, &job.PullNum, &job.PullBranch,
		&job.PullBaseBranch, &job.PullCommitSHA, &job.ProjectName, &job.ProjectDir,
		&job.Workspace, &job.Command, &job.Status, &job.AgentControllerID, &job.AgentPodName,
		&job.CreatedAt, &job.AssignedAt, &job.StartedAt, &job.CompletedAt, &job.PlanData,
		&job.Output, &job.ErrorMessage, &job.ExitCode, &labelsJSON, &envVarsJSON,
		&workflowConfigJSON, &job.TerraformVersion, &job.TimeoutSeconds, &job.VCSCredentialsEnc,
		&metadataJSON, &job.TriggeredBy, &job.Priority, &job.AttemptCount, &projectContextJSON,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("querying job: %w", err)
	}

	// Unmarshal JSON fields
	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &job.Labels); err != nil {
			return nil, fmt.Errorf("unmarshaling labels: %w", err)
		}
	}
	if len(envVarsJSON) > 0 {
		if err := json.Unmarshal(envVarsJSON, &job.EnvVars); err != nil {
			return nil, fmt.Errorf("unmarshaling env vars: %w", err)
		}
	}
	if len(workflowConfigJSON) > 0 {
		job.WorkflowConfig = workflowConfigJSON
	}
	if len(metadataJSON) > 0 {
		job.Metadata = metadataJSON
	}
	if len(projectContextJSON) > 0 {
		job.ProjectContextJSON = projectContextJSON
	}

	return job, nil
}

// UpdateStatus updates the job status
func (s *PostgresJobStore) UpdateStatus(ctx context.Context, id string, status JobStatus) error {
	now := time.Now()
	statusStr := string(status)
	query := `
		UPDATE jobs 
		SET status = $1::VARCHAR,
			started_at = CASE WHEN $1::VARCHAR = 'running' AND started_at IS NULL THEN $2 ELSE started_at END,
			completed_at = CASE WHEN $1::VARCHAR IN ('completed', 'failed', 'timeout', 'cancelled') THEN $2 ELSE completed_at END
		WHERE id = $3
	`

	result, err := s.db.ExecContext(ctx, query, statusStr, now, id)
	if err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("job not found: %s", id)
	}

	return nil
}

// UpdateResult updates the job with execution results
func (s *PostgresJobStore) UpdateResult(ctx context.Context, id string, status JobStatus, output string, planData []byte, exitCode int, errorMsg string) error {
	now := time.Now()
	statusStr := string(status)

	// Update job with results, including plan data if provided
	query := `
		UPDATE jobs 
		SET status = $1::VARCHAR,
			output = $2,
			plan_data = $3,
			exit_code = $4,
			error_message = $5,
			completed_at = $6
		WHERE id = $7
	`

	result, err := s.db.ExecContext(ctx, query, statusStr, output, planData, exitCode, errorMsg, now, id)
	if err != nil {
		return fmt.Errorf("updating job result: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("job not found: %s", id)
	}

	return nil
}

// GetQueued retrieves queued jobs
func (s *PostgresJobStore) GetQueued(ctx context.Context, limit int) ([]*Job, error) {
	query := `
		SELECT id, repo_full_name, repo_clone_url, pull_num, pull_branch, pull_base_branch,
			pull_commit_sha, project_name, project_dir, workspace, command, status,
			created_at, labels, terraform_version, timeout_seconds, triggered_by, attempt_count
		FROM jobs
		WHERE status = 'queued'
		ORDER BY created_at ASC
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("querying queued jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		var labelsJSON []byte

		err := rows.Scan(
			&job.ID, &job.RepoFullName, &job.RepoCloneURL, &job.PullNum, &job.PullBranch,
			&job.PullBaseBranch, &job.PullCommitSHA, &job.ProjectName, &job.ProjectDir,
			&job.Workspace, &job.Command, &job.Status, &job.CreatedAt, &labelsJSON,
			&job.TerraformVersion, &job.TimeoutSeconds, &job.TriggeredBy, &job.AttemptCount,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning job: %w", err)
		}

		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &job.Labels); err != nil {
				return nil, fmt.Errorf("unmarshaling labels: %w", err)
			}
		} else {
			// Initialize empty map if no labels present
			job.Labels = make(map[string]string)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// GetByAgentAndStatus retrieves jobs for a specific agent with a specific status
func (s *PostgresJobStore) GetByAgentAndStatus(ctx context.Context, agentID string, status string) ([]*Job, error) {
	query := `
		SELECT id, repo_full_name, repo_clone_url, pull_num, pull_branch, pull_base_branch,
			pull_commit_sha, project_name, project_dir, workspace, command, status,
			created_at, labels, terraform_version, timeout_seconds, triggered_by,
			project_context_json, plan_data
		FROM jobs
		WHERE agent_controller_id = $1 AND status = $2
		ORDER BY created_at ASC
	`

	rows, err := s.db.QueryContext(ctx, query, agentID, status)
	if err != nil {
		return nil, fmt.Errorf("querying jobs by agent: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		var labelsJSON []byte
		var projectContextJSON []byte
		var planData []byte

		err := rows.Scan(
			&job.ID, &job.RepoFullName, &job.RepoCloneURL, &job.PullNum, &job.PullBranch,
			&job.PullBaseBranch, &job.PullCommitSHA, &job.ProjectName, &job.ProjectDir,
			&job.Workspace, &job.Command, &job.Status, &job.CreatedAt, &labelsJSON,
			&job.TerraformVersion, &job.TimeoutSeconds, &job.TriggeredBy,
			&projectContextJSON, &planData,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning job: %w", err)
		}

		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &job.Labels); err != nil {
				return nil, fmt.Errorf("unmarshaling labels: %w", err)
			}
		} else {
			job.Labels = make(map[string]string)
		}

		if len(projectContextJSON) > 0 {
			job.ProjectContextJSON = projectContextJSON
		}

		if len(planData) > 0 {
			job.PlanData = planData
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// AssignToAgent assigns a job to an agent controller
func (s *PostgresJobStore) AssignToAgent(ctx context.Context, jobID, agentControllerID string) error {
	query := `
		UPDATE jobs
		SET status = 'assigned',
			agent_controller_id = $1,
			assigned_at = $2
		WHERE id = $3 AND status = 'queued'
	`

	result, err := s.db.ExecContext(ctx, query, agentControllerID, time.Now(), jobID)
	if err != nil {
		return fmt.Errorf("assigning job to agent: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("job not queued or not found: %s", jobID)
	}

	return nil
}

// UpdateAgentPod updates the agent pod name executing the job
func (s *PostgresJobStore) UpdateAgentPod(ctx context.Context, jobID, podName string) error {
	query := `UPDATE jobs SET agent_pod_name = $1 WHERE id = $2`
	_, err := s.db.ExecContext(ctx, query, podName, jobID)
	if err != nil {
		return fmt.Errorf("updating agent pod name: %w", err)
	}
	return nil
}

// SavePlan saves the Terraform plan data
func (s *PostgresJobStore) SavePlan(ctx context.Context, jobID string, plan []byte) error {
	query := `UPDATE jobs SET plan_data = $1 WHERE id = $2`
	_, err := s.db.ExecContext(ctx, query, plan, jobID)
	if err != nil {
		return fmt.Errorf("saving plan: %w", err)
	}
	return nil
}

// GetPlan retrieves the Terraform plan data
func (s *PostgresJobStore) GetPlan(ctx context.Context, jobID string) ([]byte, error) {
	query := `SELECT plan_data FROM jobs WHERE id = $1`
	var plan []byte
	err := s.db.QueryRowContext(ctx, query, jobID).Scan(&plan)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("getting plan: %w", err)
	}
	return plan, nil
}

// DeletePlan deletes the plan data to free space
func (s *PostgresJobStore) DeletePlan(ctx context.Context, jobID string) error {
	query := `UPDATE jobs SET plan_data = NULL WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("deleting plan: %w", err)
	}
	return nil
}

// SetOutput sets the job output
func (s *PostgresJobStore) SetOutput(ctx context.Context, jobID string, output string) error {
	query := `UPDATE jobs SET output = $1 WHERE id = $2`
	_, err := s.db.ExecContext(ctx, query, output, jobID)
	if err != nil {
		return fmt.Errorf("setting output: %w", err)
	}
	return nil
}

// SetError sets the error message and exit code
func (s *PostgresJobStore) SetError(ctx context.Context, jobID string, errorMsg string, exitCode int) error {
	query := `UPDATE jobs SET error_message = $1, exit_code = $2 WHERE id = $3`
	_, err := s.db.ExecContext(ctx, query, errorMsg, exitCode, jobID)
	if err != nil {
		return fmt.Errorf("setting error: %w", err)
	}
	return nil
}

// Complete marks a job as complete with final status
func (s *PostgresJobStore) Complete(ctx context.Context, jobID string, status JobStatus) error {
	statusStr := string(status)
	query := `
		UPDATE jobs
		SET status = $1, completed_at = $2
		WHERE id = $3
	`
	_, err := s.db.ExecContext(ctx, query, statusStr, time.Now(), jobID)
	if err != nil {
		return fmt.Errorf("completing job: %w", err)
	}
	return nil
}

// ListByRepo lists jobs for a specific repo and pull request
func (s *PostgresJobStore) ListByRepo(ctx context.Context, repoFullName string, pullNum int) ([]*Job, error) {
	query := `
		SELECT id, repo_full_name, pull_num, project_name, workspace, command, status,
			created_at, started_at, completed_at, agent_pod_name, project_dir,
			LENGTH(plan_data) as plan_data_size
		FROM jobs
		WHERE repo_full_name = $1 AND pull_num = $2
		ORDER BY created_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, repoFullName, pullNum)
	if err != nil {
		return nil, fmt.Errorf("querying jobs by repo: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		var planDataSize sql.NullInt64
		err := rows.Scan(
			&job.ID, &job.RepoFullName, &job.PullNum, &job.ProjectName, &job.Workspace,
			&job.Command, &job.Status, &job.CreatedAt, &job.StartedAt, &job.CompletedAt,
			&job.AgentPodName, &job.ProjectDir, &planDataSize,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning job: %w", err)
		}
		// Set PlanData to a non-nil slice if plan exists (for length check)
		// We don't load the actual data for performance, just indicate it exists
		if planDataSize.Valid && planDataSize.Int64 > 0 {
			job.PlanData = make([]byte, planDataSize.Int64)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// IncrementAttemptCount increments the attempt_count for a job
func (s *PostgresJobStore) IncrementAttemptCount(ctx context.Context, jobID string) error {
	query := `UPDATE jobs SET attempt_count = attempt_count + 1 WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("incrementing attempt count: %w", err)
	}
	return nil
}

// Helper function to convert empty strings to nil
func stringOrNil(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
