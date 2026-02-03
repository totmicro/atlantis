// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/runatlantis/atlantis/server/utils"
)

//go:generate pegomock generate --package mocks -o mocks/mock_pending_plan_finder.go PendingPlanFinder

type PendingPlanFinder interface {
	Find(pullDir string) ([]PendingPlan, error)
	DeletePlans(pullDir string) error
}

// DefaultPendingPlanFinder finds unapplied plans.
type DefaultPendingPlanFinder struct{}

// DistributedPendingPlanFinder finds unapplied plans in distributed mode by querying the database.
type DistributedPendingPlanFinder struct {
	JobStore db.JobStore
	Logger   logging.SimpleLogging
}

// PendingPlan is a plan that has not been applied.
type PendingPlan struct {
	// RepoDir is the absolute path to the root of the repo that holds this
	// plan.
	RepoDir string
	// RepoRelDir is the relative path from the repo to the project that
	// the plan is for.
	RepoRelDir string
	// Workspace is the workspace this plan should execute in.
	Workspace   string
	ProjectName string
}

// Find finds all pending plans in pullDir. pullDir should be the working
// directory where Atlantis will operate on this pull request. It's one level
// up from where Atlantis clones the repo for each workspace.
func (p *DefaultPendingPlanFinder) Find(pullDir string) ([]PendingPlan, error) {
	plans, _, err := p.findWithAbsPaths(pullDir)
	return plans, err
}

func (p *DefaultPendingPlanFinder) findWithAbsPaths(pullDir string) ([]PendingPlan, []string, error) {
	workspaceDirs, err := os.ReadDir(pullDir)
	if err != nil {
		return nil, nil, err
	}
	var plans []PendingPlan
	var absPaths []string
	for _, workspaceDir := range workspaceDirs {
		workspace := workspaceDir.Name()
		repoDir := filepath.Join(pullDir, workspace)

		// Any generated plans should be untracked by git since Atlantis created
		// them.
		lsCmd := exec.Command("git", "ls-files", ".", "--others") // nolint: gosec
		lsCmd.Dir = repoDir
		lsOut, err := lsCmd.CombinedOutput()
		if err != nil {
			return nil, nil, fmt.Errorf("running 'git ls-files . --others' in '%s' directory: %s: %w", repoDir, string(lsOut), err)
		}
		for file := range strings.SplitSeq(string(lsOut), "\n") {
			if filepath.Ext(file) == ".tfplan" {
				// Ignore .terragrunt-cache dirs (#487)
				if strings.Contains(file, ".terragrunt-cache/") {
					continue
				}

				projectName, err := runtime.ProjectNameFromPlanfile(workspace, filepath.Base(file))
				if err != nil {
					return nil, nil, err
				}
				plans = append(plans, PendingPlan{
					RepoDir:     repoDir,
					RepoRelDir:  filepath.Dir(file),
					Workspace:   workspace,
					ProjectName: projectName,
				})
				absPaths = append(absPaths, filepath.Join(repoDir, file))
			}
		}
	}
	return plans, absPaths, nil
}

// deletePlans deletes all plans in pullDir.
func (p *DefaultPendingPlanFinder) DeletePlans(pullDir string) error {
	_, absPaths, err := p.findWithAbsPaths(pullDir)
	if err != nil {
		return err
	}
	for _, path := range absPaths {
		if err := utils.RemoveIgnoreNonExistent(path); err != nil {
			return fmt.Errorf("delete plan at %s: %w", path, err)
		}
	}
	return nil
}

// Find finds all pending plans by querying the database for completed plan jobs.
// In distributed mode, plans are stored in the database, not on disk.
// pullDir format: /path/to/repos/<org>/<repo>/<pull-num>
func (d *DistributedPendingPlanFinder) Find(pullDir string) ([]PendingPlan, error) {
	// Extract repo and pull number from pullDir
	// Expected format: .../repos/<org>/<repo>/<pull-num>
	d.Logger.Info("DistributedPendingPlanFinder.Find called with pullDir=%q", pullDir)

	parts := strings.Split(filepath.Clean(pullDir), string(filepath.Separator))
	d.Logger.Info("pullDir split into %d parts: %v", len(parts), parts)

	if len(parts) < 3 {
		d.Logger.Warn("cannot parse pullDir for distributed mode: %q (need at least 3 parts)", pullDir)
		return nil, fmt.Errorf("invalid pullDir format: %s", pullDir)
	}

	// Get pull number (last part)
	pullNumStr := parts[len(parts)-1]
	var pullNum int
	if _, err := fmt.Sscanf(pullNumStr, "%d", &pullNum); err != nil {
		d.Logger.Warn("cannot parse pull number from pullDir: %q, pullNumStr=%q", pullDir, pullNumStr)
		return nil, fmt.Errorf("invalid pull number in pullDir: %s", pullDir)
	}

	// Get repo full name (last two before pull number)
	repoFullName := filepath.Join(parts[len(parts)-3], parts[len(parts)-2])

	d.Logger.Info("finding pending plans in database for repo=%q pull=%d (from pullDir=%q)", repoFullName, pullNum, pullDir)

	// Query database for completed plan jobs
	ctx := context.Background()
	jobs, err := d.JobStore.ListByRepo(ctx, repoFullName, pullNum)
	if err != nil {
		d.Logger.Err("failed to query database for jobs: %v", err)
		return nil, fmt.Errorf("querying database for plans: %w", err)
	}

	d.Logger.Info("database query returned %d total jobs for repo=%q pull=%d", len(jobs), repoFullName, pullNum)

	// Filter to completed plan jobs that have plan data
	var plans []PendingPlan
	seenProjects := make(map[string]bool) // Track unique project+workspace combinations

	d.Logger.Info("filtering %d jobs for pending plans", len(jobs))

	// Track skip reasons for summary
	skipReasons := map[string]int{
		"not_plan_command": 0,
		"not_completed":    0,
		"no_plan_data":     0,
		"already_applied":  0,
		"duplicate":        0,
	}

	for _, job := range jobs {
		// Only consider completed plan jobs
		if job.Command != "plan" {
			skipReasons["not_plan_command"]++
			continue
		}

		if job.Status != string(db.JobStatusCompleted) {
			skipReasons["not_completed"]++
			d.Logger.Info("skipping job %s: status=%q (not completed)", job.ID, job.Status)
			continue
		}

		// Verify plan has data - skip if plan failed or has no data
		if len(job.PlanData) == 0 {
			skipReasons["no_plan_data"]++
			d.Logger.Info("skipping job %s: no plan data (project=%q workspace=%q)", job.ID, job.ProjectName, job.Workspace)
			continue
		}

		// Check if this project+workspace has already been applied
		// (look for a completed apply job for same project after this plan)
		projectKey := fmt.Sprintf("%s/%s", job.ProjectName, job.Workspace)
		hasApply := false
		for _, otherJob := range jobs {
			if otherJob.Command == "apply" &&
				otherJob.Status == string(db.JobStatusCompleted) &&
				otherJob.ProjectName == job.ProjectName &&
				otherJob.Workspace == job.Workspace &&
				otherJob.CreatedAt.After(job.CreatedAt) {
				hasApply = true
				break
			}
		}

		if hasApply {
			skipReasons["already_applied"]++
			d.Logger.Info("skipping job %s: project %q workspace %q already applied", job.ID, job.ProjectName, job.Workspace)
			continue
		}

		if seenProjects[projectKey] {
			skipReasons["duplicate"]++
			continue // Already added this project+workspace
		}

		seenProjects[projectKey] = true

		// Add to pending plans
		// Note: RepoDir is not strictly needed in distributed mode since we don't use files
		plans = append(plans, PendingPlan{
			RepoDir:     pullDir, // Use pullDir as placeholder
			RepoRelDir:  job.ProjectDir,
			Workspace:   job.Workspace,
			ProjectName: job.ProjectName,
		})

		d.Logger.Info("found pending plan for project=%q dir=%q workspace=%q (job %s)",
			job.ProjectName, job.ProjectDir, job.Workspace, job.ID)
	}

	d.Logger.Info("skip summary: not_plan=%d not_completed=%d no_data=%d already_applied=%d duplicate=%d",
		skipReasons["not_plan_command"], skipReasons["not_completed"], skipReasons["no_plan_data"],
		skipReasons["already_applied"], skipReasons["duplicate"])
	d.Logger.Info("found %d pending plan(s) for repo=%q pull=%d", len(plans), repoFullName, pullNum)
	return plans, nil
}

// DeletePlans deletes plan data from the database.
func (d *DistributedPendingPlanFinder) DeletePlans(pullDir string) error {
	// Extract repo and pull number from pullDir
	parts := strings.Split(filepath.Clean(pullDir), string(filepath.Separator))
	if len(parts) < 3 {
		return fmt.Errorf("invalid pullDir format: %s", pullDir)
	}

	pullNumStr := parts[len(parts)-1]
	var pullNum int
	if _, err := fmt.Sscanf(pullNumStr, "%d", &pullNum); err != nil {
		return fmt.Errorf("invalid pull number in pullDir: %s", pullDir)
	}

	repoFullName := filepath.Join(parts[len(parts)-3], parts[len(parts)-2])

	d.Logger.Info("deleting plans from database for repo=%q pull=%d", repoFullName, pullNum)

	// Query database for plan jobs
	ctx := context.Background()
	jobs, err := d.JobStore.ListByRepo(ctx, repoFullName, pullNum)
	if err != nil {
		return fmt.Errorf("querying database for plans: %w", err)
	}

	// Delete plan data from each plan job
	for _, job := range jobs {
		if job.Command == "plan" {
			if err := d.JobStore.DeletePlan(ctx, job.ID); err != nil {
				d.Logger.Warn("failed to delete plan for job %s: %v", job.ID, err)
				// Continue with other plans
			} else {
				d.Logger.Debug("deleted plan for job %s", job.ID)
			}
		}
	}

	return nil
}
