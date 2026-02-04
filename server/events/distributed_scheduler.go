package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
)

// DistributedScheduler adapts the core scheduler for use in the events package
type DistributedScheduler struct {
	scheduler *scheduler.DefaultScheduler
	jobStore  *db.PostgresJobStore
	logger    logging.SimpleLogging
	globalCfg valid.GlobalCfg
}

// NewDistributedScheduler creates a new distributed scheduler adapter
func NewDistributedScheduler(scheduler *scheduler.DefaultScheduler, jobStore *db.PostgresJobStore, logger logging.SimpleLogging, globalCfg valid.GlobalCfg) *DistributedScheduler {
	return &DistributedScheduler{
		scheduler: scheduler,
		jobStore:  jobStore,
		logger:    logger,
		globalCfg: globalCfg,
	}
}

// ScheduleJob creates and schedules a job from a project context
func (d *DistributedScheduler) ScheduleJob(ctx command.ProjectContext) (string, error) {
	jobID := uuid.New().String()

	// Set JobID in context before serialization so it's available on the agent
	ctx.JobID = jobID

	// Convert command name to string using String() method
	commandStr := ctx.CommandName.String()
	d.logger.Info("scheduling job: command=%q commandName=%v jobID=%s", commandStr, ctx.CommandName, jobID)

	// Debug: Log the steps being serialized
	d.logger.Info("project context has %d steps before serialization", len(ctx.Steps))
	for i, step := range ctx.Steps {
		d.logger.Info("step %d before serialization: StepName=%q RunCommand=%q ExtraArgs=%v",
			i, step.StepName, step.RunCommand, step.ExtraArgs)
	}

	// Ensure Labels is never nil (PostgreSQL jsonb field requires valid JSON)
	labels := ctx.AgentPoolSelector
	if labels == nil {
		labels = make(map[string]string)
	}

	// Ensure EnvVars is never nil
	envVars := make(map[string]string)

	// Serialize the full ProjectContext for agent execution
	// This allows the agent to reconstruct the complete execution environment
	projectContextJSON, err := json.Marshal(ctx)
	if err != nil {
		return "", fmt.Errorf("serializing project context: %w", err)
	}

	d.logger.Info("serialized project context to %d bytes", len(projectContextJSON))

	// Build clean clone URL without embedded credentials
	// Format: https://github.com/owner/repo.git
	cloneURL := fmt.Sprintf("https://%s/%s.git", ctx.Pull.BaseRepo.VCSHost.Hostname, ctx.Pull.BaseRepo.FullName)

	// Extract pre-workflow hooks for this repository
	// These hooks run before parsing atlantis.yaml, allowing dynamic config generation
	preWorkflowHooks := make([]*valid.WorkflowHook, 0)
	for _, repo := range d.globalCfg.Repos {
		if repo.IDMatches(ctx.Pull.BaseRepo.ID()) && len(repo.PreWorkflowHooks) > 0 {
			preWorkflowHooks = append(preWorkflowHooks, repo.PreWorkflowHooks...)
		}
	}

	// Store VCS host info and pre-workflow hooks in metadata
	vcsMetadata := map[string]interface{}{
		"vcs_host_type": ctx.Pull.BaseRepo.VCSHost.Type.String(),
		"vcs_hostname":  ctx.Pull.BaseRepo.VCSHost.Hostname,
		"repo_owner":    ctx.Pull.BaseRepo.Owner,
		"repo_name":     ctx.Pull.BaseRepo.Name,
	}

	// Add pre-workflow hooks to metadata if any exist
	if len(preWorkflowHooks) > 0 {
		vcsMetadata["pre_workflow_hooks"] = preWorkflowHooks
		d.logger.Info("serialized %d pre-workflow hooks for job %s", len(preWorkflowHooks), jobID)
	}

	metadataJSON, _ := json.Marshal(vcsMetadata)

	// Create database job for persistence
	dbJob := &db.Job{
		ID:                 jobID,
		RepoFullName:       ctx.Pull.BaseRepo.FullName,
		RepoCloneURL:       cloneURL,
		PullNum:            ctx.Pull.Num,
		PullBranch:         ctx.Pull.HeadBranch,
		PullBaseBranch:     ctx.Pull.BaseBranch,
		PullCommitSHA:      ctx.Pull.HeadCommit,
		Command:            commandStr,
		ProjectName:        ctx.ProjectName,
		ProjectDir:         ctx.RepoRelDir,
		Workspace:          ctx.Workspace,
		ProjectContextJSON: projectContextJSON,
		Labels:             labels,
		EnvVars:            envVars,
		Metadata:           metadataJSON,
		Status:             string(db.JobStatusQueued),
		CreatedAt:          time.Now(),
		TriggeredBy:        ctx.User.Username,
		Priority:           0, // Default priority
		AttemptCount:       0, // First attempt
	}

	// Create job in database
	if err := d.jobStore.Create(context.Background(), dbJob); err != nil {
		return "", fmt.Errorf("creating job in database: %w", err)
	}

	d.logger.Info("created job %s for %s/%d project %s", jobID, dbJob.RepoFullName, dbJob.PullNum, dbJob.ProjectName)

	// For plan jobs, clean up old plan data from previous plan jobs for the same project/workspace
	// This ensures we only keep the most recent plan and don't accumulate stale plans
	if commandStr == "plan" {
		d.logger.Info("plan job: cleaning up old plans for same project/workspace")
		oldJobs, err := d.jobStore.ListByRepo(context.Background(), dbJob.RepoFullName, dbJob.PullNum)
		if err != nil {
			d.logger.Warn("failed to list old jobs for cleanup: %s", err)
		} else {
			for _, oldJob := range oldJobs {
				// Delete plan data from old plan jobs for same project/workspace
				if oldJob.Command == "plan" &&
					oldJob.ProjectName == dbJob.ProjectName &&
					oldJob.Workspace == dbJob.Workspace &&
					oldJob.ID != jobID { // Don't delete the job we just created
					d.logger.Info("deleting old plan data from job %s", oldJob.ID)
					if err := d.jobStore.DeletePlan(context.Background(), oldJob.ID); err != nil {
						d.logger.Warn("failed to delete old plan data from job %s: %s", oldJob.ID, err)
					}
				}
			}
		}
	}

	// For apply jobs, find and copy plan data from the most recent plan job
	// This is critical for correctness - we must ensure we get the right plan
	if commandStr == "apply" {
		d.logger.Info("apply job: locating plan job for repo=%q pull=%d project=%q workspace=%q",
			dbJob.RepoFullName, dbJob.PullNum, dbJob.ProjectName, dbJob.Workspace)

		planJobs, err := d.jobStore.ListByRepo(context.Background(), dbJob.RepoFullName, dbJob.PullNum)
		if err != nil {
			d.logger.Err("failed to list jobs for plan lookup: %s", err)
			return "", fmt.Errorf("apply job requires plan data, but failed to search for plan job: %w", err)
		}

		// Find THE most recent completed plan job for EXACT same project/workspace
		// We are very strict here to ensure we never use the wrong plan
		var planJobID string
		var planJobCreatedAt time.Time
		for _, job := range planJobs {
			// Strict matching: must be exact same project, workspace, and completed successfully
			if job.Command == "plan" &&
				job.ProjectName == dbJob.ProjectName &&
				job.Workspace == dbJob.Workspace &&
				job.Status == string(db.JobStatusCompleted) {
				planJobID = job.ID
				planJobCreatedAt = job.CreatedAt
				break // List is ordered by created_at DESC, so first match is most recent
			}
		}

		if planJobID == "" {
			d.logger.Err("no completed plan job found for apply job (repo=%q pull=%d project=%q workspace=%q)",
				dbJob.RepoFullName, dbJob.PullNum, dbJob.ProjectName, dbJob.Workspace)
			return "", fmt.Errorf("cannot create apply job: no plan found for project %q workspace %q - run 'atlantis plan' first",
				dbJob.ProjectName, dbJob.Workspace)
		}

		// Fetch full plan job to get plan_data and validate
		planJob, err := d.jobStore.Get(context.Background(), planJobID)
		if err != nil {
			d.logger.Err("failed to fetch plan job %s: %s", planJobID, err)
			return "", fmt.Errorf("failed to fetch plan job: %w", err)
		}

		// Validate plan data exists
		if len(planJob.PlanData) == 0 {
			d.logger.Err("plan job %s exists but has no plan data", planJobID)
			return "", fmt.Errorf("plan job %s completed but has no plan data - plan may have failed", planJobID)
		}

		// Store plan job ID in apply job metadata for traceability
		var metadata map[string]interface{}
		if err := json.Unmarshal(dbJob.Metadata, &metadata); err != nil {
			metadata = make(map[string]interface{})
		}
		metadata["plan_job_id"] = planJobID
		metadata["plan_job_created_at"] = planJobCreatedAt.Format(time.RFC3339)
		metadata["plan_data_size"] = len(planJob.PlanData)
		dbJob.Metadata, _ = json.Marshal(metadata)

		// Update apply job metadata
		if err := d.jobStore.Create(context.Background(), &db.Job{
			ID:       jobID,
			Metadata: dbJob.Metadata,
		}); err != nil {
			d.logger.Warn("failed to update apply job metadata with plan job ID: %s", err)
		}

		// Copy plan data to apply job
		d.logger.Info("copying plan data from job %s (created: %s, size: %d bytes) to apply job %s",
			planJobID, planJobCreatedAt.Format(time.RFC3339), len(planJob.PlanData), jobID)

		if err := d.jobStore.SavePlan(context.Background(), jobID, planJob.PlanData); err != nil {
			d.logger.Err("failed to copy plan data to apply job: %s", err)
			return "", fmt.Errorf("failed to attach plan to apply job: %w", err)
		}

		d.logger.Info("successfully attached plan to apply job %s (plan from job %s)", jobID, planJobID)
	}

	d.logger.Info("job %s created in database, triggering immediate assignment", jobID)

	// Immediately try to assign the job to an available agent (push-based)
	// This avoids waiting for the 5-second polling interval
	go func() {
		if job, agent, err := d.scheduler.AssignNextJob(); err != nil {
			// Expected - job may already be assigned or no agents available
			d.logger.Debug("immediate job assignment skipped: %v", err)
		} else if job != nil && agent != nil {
			d.logger.Info("immediately assigned job %s to agent %s", job.ID, agent.ID)
		} else {
			d.logger.Debug("no available agents for immediate assignment of job %s", jobID)
		}
	}()

	return jobID, nil
}
