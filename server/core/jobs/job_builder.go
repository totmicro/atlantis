package jobs

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/runatlantis/atlantis/server/events/command"
)

// JobBuilder converts command contexts into jobs
type JobBuilder struct {
	// Can add dependencies here if needed (e.g., config, logger)
}

// NewJobBuilder creates a new job builder
func NewJobBuilder() *JobBuilder {
	return &JobBuilder{}
}

// BuildFromCommand creates a job from a command context
func (jb *JobBuilder) BuildFromCommand(
	ctx *command.Context,
	cmdName command.Name,
	projectName string,
	workspace string,
	repoRelDir string,
	verbose bool,
) (*Job, error) {
	if ctx == nil {
		return nil, fmt.Errorf("command context cannot be nil")
	}

	// Generate unique job ID
	jobID := uuid.New().String()

	// Determine job type from command
	jobType := commandToJobType(cmdName)

	// Extract labels from project configuration
	labels := extractLabels(ctx)

	// Build job context
	jobCtx := &JobContext{
		Pull:         ctx.Pull,
		RepoFullName: ctx.Pull.BaseRepo.FullName,
		RepoCloneURL: ctx.Pull.BaseRepo.CloneURL,
		RepoBranch:   ctx.Pull.HeadBranch,
		RepoName:     ctx.Pull.BaseRepo.Name,
		RepoOwner:    ctx.Pull.BaseRepo.Owner,
		RepoHostname: ctx.Pull.BaseRepo.VCSHost.Hostname,
		Command:      cmdName,
		Workspace:    workspace,
		ProjectName:  projectName,
		RepoRelDir:   repoRelDir,
		Verbose:      verbose,
		User:         ctx.User,
		Metadata:     make(map[string]string),
	}

	// Create the job
	job := &Job{
		ID:       jobID,
		Type:     jobType,
		Status:   JobStatusQueued,
		Priority: PriorityNormal,
		Context:  jobCtx,
		Labels:   labels,
	}

	return job, nil
}

// BuildFromProjectCommand creates a job from a project command context
func (jb *JobBuilder) BuildFromProjectCommand(
	ctx *command.Context,
	projectCtx command.ProjectContext,
) (*Job, error) {
	if ctx == nil {
		return nil, fmt.Errorf("command context cannot be nil")
	}

	jobID := uuid.New().String()
	jobType := commandToJobType(projectCtx.CommandName)

	// Extract labels from project configuration
	labels := extractProjectLabels(projectCtx)

	// Convert TerraformVersion to string
	tfVersion := ""
	if projectCtx.TerraformVersion != nil {
		tfVersion = projectCtx.TerraformVersion.String()
	}

	// Build job context with project-specific details
	jobCtx := &JobContext{
		Pull:             ctx.Pull,
		RepoFullName:     ctx.Pull.BaseRepo.FullName,
		RepoCloneURL:     ctx.Pull.BaseRepo.CloneURL,
		RepoBranch:       ctx.Pull.HeadBranch,
		RepoName:         ctx.Pull.BaseRepo.Name,
		RepoOwner:        ctx.Pull.BaseRepo.Owner,
		RepoHostname:     ctx.Pull.BaseRepo.VCSHost.Hostname,
		Command:          projectCtx.CommandName,
		Workspace:        projectCtx.Workspace,
		ProjectName:      projectCtx.ProjectName,
		RepoRelDir:       projectCtx.RepoRelDir,
		TerraformVersion: tfVersion,
		Verbose:          projectCtx.Verbose,
		User:             ctx.User,
		PolicySets:       policySetsToStrings(projectCtx.PolicySets),
		Metadata:         make(map[string]string),
	}

	// Add project configuration to metadata
	if projectCtx.ProjectName != "" {
		jobCtx.Metadata["project_name"] = projectCtx.ProjectName
	}
	if tfVersion != "" {
		jobCtx.Metadata["terraform_version"] = tfVersion
	}

	job := &Job{
		ID:       jobID,
		Type:     jobType,
		Status:   JobStatusQueued,
		Priority: determinePriority(projectCtx),
		Context:  jobCtx,
		Labels:   labels,
	}

	return job, nil
}

// commandToJobType converts an Atlantis command to a job type
func commandToJobType(cmdName command.Name) JobType {
	switch cmdName {
	case command.Plan:
		return JobTypePlan
	case command.Apply:
		return JobTypeApply
	case command.Unlock:
		return JobTypeUnlock
	case command.Import:
		return JobTypeImport
	case command.State:
		return JobTypeState
	case command.Version:
		return JobTypeVersion
	case command.ApprovePolicies:
		return JobTypeApprovePolicies
	default:
		return JobTypePlan // default to plan
	}
}

// extractLabels extracts routing labels from command context
func extractLabels(ctx *command.Context) map[string]string {
	labels := make(map[string]string)

	// Add basic labels
	labels["repo"] = ctx.Pull.BaseRepo.FullName
	labels["pr"] = fmt.Sprintf("%d", ctx.Pull.Num)
	labels["vcs"] = ctx.Pull.BaseRepo.VCSHost.Type.String()

	return labels
}

// extractProjectLabels extracts routing labels from project context
func extractProjectLabels(projectCtx command.ProjectContext) map[string]string {
	labels := make(map[string]string)

	// Add project-specific labels
	if projectCtx.ProjectName != "" {
		labels["project"] = projectCtx.ProjectName
	}
	if projectCtx.Workspace != "" {
		labels["workspace"] = projectCtx.Workspace
	}
	if projectCtx.RepoRelDir != "" {
		labels["dir"] = projectCtx.RepoRelDir
	}

	// Add repo information
	labels["repo"] = projectCtx.BaseRepo.FullName

	return labels
}

// determinePriority determines job priority based on project context
func determinePriority(projectCtx command.ProjectContext) JobPriority {
	// Apply commands get higher priority than plan
	if projectCtx.CommandName == command.Apply {
		return PriorityHigh
	}

	// Check if policy sets exist (not just a bool)
	if len(projectCtx.PolicySets.PolicySets) > 0 {
		return PriorityNormal
	}

	// Default priority
	return PriorityNormal
}

// policySetsToStrings converts PolicySets to a slice of strings
func policySetsToStrings(policySets interface{}) []string {
	// For now, return empty slice - will implement full conversion if needed
	return []string{}
}
