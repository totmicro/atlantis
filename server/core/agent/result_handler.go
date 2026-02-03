package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/jobs"
	"github.com/runatlantis/atlantis/server/logging"
)

// DistributedJobResultHandler posts job results using standard Atlantis mechanisms
type DistributedJobResultHandler struct {
	resultUpdater        *events.DistributedResultUpdater
	projectStatusUpdater jobs.ProjectStatusUpdater
	logger               logging.SimpleLogging
}

// NewDistributedJobResultHandler creates a new result handler
func NewDistributedJobResultHandler(resultUpdater *events.DistributedResultUpdater, projectStatusUpdater jobs.ProjectStatusUpdater, logger logging.SimpleLogging) *DistributedJobResultHandler {
	return &DistributedJobResultHandler{
		resultUpdater:        resultUpdater,
		projectStatusUpdater: projectStatusUpdater,
		logger:               logger,
	}
}

// HandleJobResult posts the job result using Atlantis's standard PR update mechanism
func (h *DistributedJobResultHandler) HandleJobResult(ctx context.Context, job *db.Job, result *proto.JobResult) error {
	// Deserialize project context to get Pull and other metadata
	var projectCtx command.ProjectContext
	if err := json.Unmarshal(job.ProjectContextJSON, &projectCtx); err != nil {
		return fmt.Errorf("deserializing project context: %w", err)
	}

	// Set logger on projectCtx (not serialized in JSON)
	projectCtx.Log = h.logger

	// Parse command from job
	cmdName, err := command.ParseCommandName(job.Command)
	if err != nil {
		return fmt.Errorf("parsing command name %q: %w", job.Command, err)
	}

	// Convert job result to ProjectResult
	projectResult := command.ProjectResult{
		Command:     cmdName,
		RepoRelDir:  job.ProjectDir,
		Workspace:   job.Workspace,
		ProjectName: job.ProjectName,
	}

	// Set result based on status and command type
	if result.Status == "completed" && result.ErrorMessage == "" {
		switch cmdName {
		case command.Plan:
			projectResult.ProjectCommandOutput = command.ProjectCommandOutput{
				PlanSuccess: &models.PlanSuccess{
					TerraformOutput: result.Output,
					LockURL:         "", // Will be set by lock system if needed
					RePlanCmd:       projectCtx.RePlanCmd,
					ApplyCmd:        projectCtx.ApplyCmd,
				},
			}
		case command.Apply:
			projectResult.ProjectCommandOutput = command.ProjectCommandOutput{
				ApplySuccess: result.Output,
			}
		default:
			// For other commands, just set output as plan for now
			projectResult.ProjectCommandOutput = command.ProjectCommandOutput{
				PlanSuccess: &models.PlanSuccess{
					TerraformOutput: result.Output,
				},
			}
		}
	} else {
		// Failed
		projectResult.ProjectCommandOutput = command.ProjectCommandOutput{
			Error:   fmt.Errorf("%s", result.ErrorMessage),
			Failure: result.Output,
		}
	}

	// Update per-project commit status FIRST (fast, non-blocking)
	// This ensures GitHub checks update immediately while PR comment posts async
	if result.Status == "completed" && result.ErrorMessage == "" {
		// Success case
		if cmdName == command.Plan {
			// Set plan status to success
			if err := h.projectStatusUpdater.UpdateProject(
				projectCtx,
				command.Plan,
				models.SuccessCommitStatus,
				"",
				&projectResult.ProjectCommandOutput,
			); err != nil {
				h.logger.Err("failed to update per-project plan status: %v", err)
			}

			// If plan has no changes, also set apply status to success
			if projectResult.PlanSuccess != nil && projectResult.PlanSuccess.NoChanges() {
				if err := h.projectStatusUpdater.UpdateProject(
					projectCtx,
					command.Apply,
					models.SuccessCommitStatus,
					"",
					&projectResult.ProjectCommandOutput,
				); err != nil {
					h.logger.Err("failed to update per-project apply status: %v", err)
				}
			}
		} else if cmdName == command.Apply {
			// Set apply status to success
			if err := h.projectStatusUpdater.UpdateProject(
				projectCtx,
				command.Apply,
				models.SuccessCommitStatus,
				"",
				&projectResult.ProjectCommandOutput,
			); err != nil {
				h.logger.Err("failed to update per-project apply status: %v", err)
			}
		}
	} else {
		// Failure case - set status to failed
		if err := h.projectStatusUpdater.UpdateProject(
			projectCtx,
			cmdName,
			models.FailedCommitStatus,
			"",
			&projectResult.ProjectCommandOutput,
		); err != nil {
			h.logger.Err("failed to update per-project status to failed: %v", err)
		}
	}

	// Update database and combined commit status synchronously (fast, includes global apply check)
	// This ensures the global apply status updates immediately after plan completes
	if err := h.resultUpdater.UpdateStatusOnly(h.logger, projectCtx.Pull, cmdName, []command.ProjectResult{projectResult}); err != nil {
		return fmt.Errorf("updating pull status: %w", err)
	}

	// Post PR comment asynchronously (slower, but won't block check updates or result handler)
	go h.resultUpdater.PostComment(h.logger, projectCtx.Pull, cmdName, []command.ProjectResult{projectResult})

	h.logger.Info("posted distributed job result status for PR %d in %s (PR comment posting in background)", job.PullNum, job.RepoFullName)

	return nil
}
