package jobs

import (
	"time"

	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
)

// JobType represents the type of Atlantis command
type JobType string

const (
	JobTypePlan            JobType = "plan"
	JobTypeApply           JobType = "apply"
	JobTypeUnlock          JobType = "unlock"
	JobTypeImport          JobType = "import"
	JobTypeState           JobType = "state"
	JobTypeVersion         JobType = "version"
	JobTypeApprovePolicies JobType = "approve_policies"
)

// JobStatus represents the current state of a job
type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusAssigned  JobStatus = "assigned"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCanceled  JobStatus = "canceled"
)

// JobPriority represents the priority level of a job
type JobPriority int

const (
	PriorityLow    JobPriority = 1
	PriorityNormal JobPriority = 5
	PriorityHigh   JobPriority = 10
)

// Job represents a unit of work to be executed by an agent
type Job struct {
	ID          string
	Type        JobType
	Status      JobStatus
	Priority    JobPriority
	Context     *JobContext
	Labels      map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	AssignedTo  string // Agent controller ID
	CompletedAt *time.Time
	Output      string
	Error       string
}

// JobContext contains all the information needed to execute a job
type JobContext struct {
	// Pull request information
	Pull models.PullRequest

	// Repo information
	RepoFullName string
	RepoCloneURL string
	RepoBranch   string
	RepoName     string
	RepoOwner    string
	RepoHostname string

	// VCS credentials (encrypted in database)
	VCSToken     string
	VCSTokenType string // "user", "app", "installation"

	// Command details
	Command       command.Name
	Workspace     string
	ProjectName   string
	ProjectPath   string
	RepoRelDir    string
	AutoplanFiles []string

	// Terraform configuration
	TerraformVersion string
	TerraformEnv     map[string]string

	// Policy checks
	PolicyCheck bool
	PolicySets  []string

	// Execution settings
	Verbose bool
	User    models.User

	// Additional metadata
	Metadata map[string]string
}

// ToMap converts JobContext to a map for JSONB storage
func (jc *JobContext) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"pull":              jc.Pull,
		"repo_full_name":    jc.RepoFullName,
		"repo_clone_url":    jc.RepoCloneURL,
		"repo_branch":       jc.RepoBranch,
		"repo_name":         jc.RepoName,
		"repo_owner":        jc.RepoOwner,
		"repo_hostname":     jc.RepoHostname,
		"vcs_token":         jc.VCSToken,
		"vcs_token_type":    jc.VCSTokenType,
		"command":           jc.Command,
		"workspace":         jc.Workspace,
		"project_name":      jc.ProjectName,
		"project_path":      jc.ProjectPath,
		"repo_rel_dir":      jc.RepoRelDir,
		"autoplan_files":    jc.AutoplanFiles,
		"terraform_version": jc.TerraformVersion,
		"terraform_env":     jc.TerraformEnv,
		"policy_check":      jc.PolicyCheck,
		"policy_sets":       jc.PolicySets,
		"verbose":           jc.Verbose,
		"user":              jc.User,
		"metadata":          jc.Metadata,
	}
}
