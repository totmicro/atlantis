// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

// DistributedResultUpdater provides public methods for updating distributed job results
// This follows the same pattern as ApplyCommandRunner to avoid code duplication
type DistributedResultUpdater struct {
	dbUpdater           *DBUpdater
	pullUpdater         *PullUpdater
	commitStatusUpdater CommitStatusUpdater
}

// NewDistributedResultUpdater creates a new result updater
func NewDistributedResultUpdater(
	dbUpdater *DBUpdater,
	pullUpdater *PullUpdater,
	commitStatusUpdater CommitStatusUpdater,
) *DistributedResultUpdater {
	return &DistributedResultUpdater{
		dbUpdater:           dbUpdater,
		pullUpdater:         pullUpdater,
		commitStatusUpdater: commitStatusUpdater,
	}
}

// UpdateWithResults updates the database and PR comment with job results
// This follows the exact same pattern as ApplyCommandRunner.Run() to ensure consistency
func (u *DistributedResultUpdater) UpdateWithResults(
	log logging.SimpleLogging,
	pull models.PullRequest,
	cmdName command.Name,
	results []command.ProjectResult,
) error {
	// Create a minimal context for the update
	ctx := &command.Context{
		Log:  log,
		Pull: pull,
	}

	// 1. Update PR comment (same as standard flow)
	u.pullUpdater.updatePull(
		ctx,
		&CommentCommand{Name: cmdName},
		command.Result{ProjectResults: results},
	)

	// 2. Update database (same as standard flow)
	pullStatus, err := u.dbUpdater.updateDB(ctx, pull, results)
	if err != nil {
		return err
	}

	// 3. Update commit status (same as standard flow)
	u.updateCommitStatus(ctx, cmdName, pullStatus)

	// Note: Automerge is not handled here as it requires additional context
	// (projectCmds, cmd flags) that we don't have from async job results
	return nil
}

// UpdateStatusOnly updates database and commit status without posting PR comment
// This is faster and can be called synchronously while PR comment posts async
func (u *DistributedResultUpdater) UpdateStatusOnly(
	log logging.SimpleLogging,
	pull models.PullRequest,
	cmdName command.Name,
	results []command.ProjectResult,
) error {
	// Create a minimal context for the update
	ctx := &command.Context{
		Log:  log,
		Pull: pull,
	}

	// 1. Update database (fast)
	pullStatus, err := u.dbUpdater.updateDB(ctx, pull, results)
	if err != nil {
		return err
	}

	// 2. Update commit status including global apply check (fast)
	u.updateCommitStatus(ctx, cmdName, pullStatus)

	return nil
}

// PostComment posts the PR comment asynchronously
func (u *DistributedResultUpdater) PostComment(
	log logging.SimpleLogging,
	pull models.PullRequest,
	cmdName command.Name,
	results []command.ProjectResult,
) {
	// Create a minimal context for the update
	ctx := &command.Context{
		Log:  log,
		Pull: pull,
	}

	// Post PR comment (slower)
	u.pullUpdater.updatePull(
		ctx,
		&CommentCommand{Name: cmdName},
		command.Result{ProjectResults: results},
	)
}

// updateCommitStatus updates the VCS commit status
// This is extracted from ApplyCommandRunner.updateCommitStatus to avoid duplication
func (u *DistributedResultUpdater) updateCommitStatus(
	ctx *command.Context,
	cmdName command.Name,
	pullStatus models.PullStatus,
) {
	var numSuccess int
	var numErrored int
	status := models.SuccessCommitStatus

	switch cmdName {
	case command.Apply:
		numSuccess = pullStatus.StatusCount(models.AppliedPlanStatus) + pullStatus.StatusCount(models.PlannedNoChangesPlanStatus)
		numErrored = pullStatus.StatusCount(models.ErroredApplyStatus)

		if numErrored > 0 {
			status = models.FailedCommitStatus
		} else if numSuccess < len(pullStatus.Projects) {
			// If there are plans that haven't been applied yet, use pending status
			status = models.PendingCommitStatus
		}

	case command.Plan:
		numErrored = pullStatus.StatusCount(models.ErroredPlanStatus)
		// We consider anything that isn't a plan error as a plan success.
		// For example, if there is an apply error, that means that at least a
		// plan was generated successfully. This includes PlannedNoChangesPlanStatus.
		numSuccess = len(pullStatus.Projects) - numErrored

		if numErrored > 0 {
			status = models.FailedCommitStatus
		}
	}

	if err := u.commitStatusUpdater.UpdateCombinedCount(
		ctx.Log,
		ctx.Pull.BaseRepo,
		ctx.Pull,
		status,
		cmdName,
		numSuccess,
		len(pullStatus.Projects),
	); err != nil {
		ctx.Log.Warn("unable to update commit status: %s", err)
	}
}

// PostJobFailureComment posts a failure comment when a job can't be assigned
func (u *DistributedResultUpdater) PostJobFailureComment(logger logging.SimpleLogging, repoFullName string, pullNum int, jobID string, errorMsg string) error {
	// Parse repo owner and name from full name (format: "owner/repo")
	parts := strings.Split(repoFullName, "/")
	if len(parts) < 2 {
		return fmt.Errorf("invalid repo full name format: %s", repoFullName)
	}

	// Handle cases like "gitlab/subgroup/repo" by joining all but last part as owner
	repoName := parts[len(parts)-1]
	repoOwner := strings.Join(parts[:len(parts)-1], "/")

	// Create a basic pull request model for comment posting
	pull := models.PullRequest{
		Num: pullNum,
		BaseRepo: models.Repo{
			FullName: repoFullName,
			Owner:    repoOwner,
			Name:     repoName,
		},
	}

	// Create a failed project result with the error message
	projectResult := command.ProjectResult{
		RepoRelDir: "job-" + jobID,
		Workspace:  "default",
		ProjectCommandOutput: command.ProjectCommandOutput{
			Error:   fmt.Errorf("%s", errorMsg),
			Failure: errorMsg,
		},
	}

	// Create a minimal context for comment posting
	ctx := &command.Context{
		Log:  logger,
		Pull: pull,
	}

	// Post PR comment using the same mechanism as other failures
	u.pullUpdater.updatePull(
		ctx,
		&CommentCommand{Name: command.Plan}, // Use plan command name
		command.Result{ProjectResults: []command.ProjectResult{projectResult}},
	)

	return nil
}

// PostBatchJobFailureComment posts a single batched comment for multiple cancelled jobs
func (u *DistributedResultUpdater) PostBatchJobFailureComment(logger logging.SimpleLogging, repoFullName string, pullNum int, jobs []*db.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	// Parse repo owner and name from full name (format: "owner/repo")
	parts := strings.Split(repoFullName, "/")
	if len(parts) < 2 {
		return fmt.Errorf("invalid repo full name format: %s", repoFullName)
	}

	// Handle cases like "gitlab/subgroup/repo" by joining all but last part as owner
	repoName := parts[len(parts)-1]
	repoOwner := strings.Join(parts[:len(parts)-1], "/")

	// Create a basic pull request model for comment posting
	pull := models.PullRequest{
		Num: pullNum,
		BaseRepo: models.Repo{
			FullName: repoFullName,
			Owner:    repoOwner,
			Name:     repoName,
		},
	}

	// Build a batched error message listing all cancelled jobs
	var failureMsg strings.Builder
	failureMsg.WriteString(fmt.Sprintf("## ❌ Auto-Cancelled Jobs (%d)\n\n", len(jobs)))
	failureMsg.WriteString("The following jobs were cancelled because no agents with matching labels were available:\n\n")

	for i, job := range jobs {
		labelsJSON, _ := json.Marshal(job.Labels)
		failureMsg.WriteString(fmt.Sprintf("%d. **Project:** `%s`", i+1, job.ProjectName))
		if job.Workspace != "default" {
			failureMsg.WriteString(fmt.Sprintf(" (workspace: `%s`)", job.Workspace))
		}
		failureMsg.WriteString(fmt.Sprintf("\n   - **Required labels:** `%s`\n", string(labelsJSON)))
		failureMsg.WriteString(fmt.Sprintf("   - **Attempts:** %d\n", job.AttemptCount))
		failureMsg.WriteString("\n")
	}

	failureMsg.WriteString("\n**Resolution:** Deploy agents with the required labels or update your `atlantis.yaml` configuration.\n")

	// Create a failed project result with the batched message
	projectResult := command.ProjectResult{
		RepoRelDir: "auto-cancel",
		Workspace:  "default",
		ProjectCommandOutput: command.ProjectCommandOutput{
			Failure: failureMsg.String(),
		},
	}

	// Create a minimal context for comment posting
	ctx := &command.Context{
		Log:  logger,
		Pull: pull,
	}

	// Post PR comment using the same mechanism as other failures
	u.pullUpdater.updatePull(
		ctx,
		&CommentCommand{Name: command.Plan}, // Use plan command name
		command.Result{ProjectResults: []command.ProjectResult{projectResult}},
	)

	return nil
}
