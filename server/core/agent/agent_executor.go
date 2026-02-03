// Copyright 2017 HootSuite Media Inc.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Modified hereafter by contributors to runatlantis/atlantis.

package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
)

// AgentExecutor executes jobs using the same infrastructure as the Atlantis server.
// This is "Atlantis server lite" - it receives jobs via gRPC instead of webhooks,
// but uses identical VCS clients, terraform clients, and command runners.
type AgentExecutor struct {
	Logger               logging.SimpleLogging
	ProjectCommandRunner events.ProjectCommandRunner
	VCSClient            vcs.Client
	WorkingDir           events.WorkingDir
}

// NewAgentExecutor creates a new agent executor with all the standard Atlantis components.
func NewAgentExecutor(
	logger logging.SimpleLogging,
	projectCommandRunner events.ProjectCommandRunner,
	vcsClient vcs.Client,
	workingDir events.WorkingDir,
) *AgentExecutor {
	return &AgentExecutor{
		Logger:               logger,
		ProjectCommandRunner: projectCommandRunner,
		VCSClient:            vcsClient,
		WorkingDir:           workingDir,
	}
}

// ExecuteResult represents the result of job execution
type ExecuteResult struct {
	Output      string
	PlanSuccess bool
	PlanData    []byte // Terraform plan file data (for plan commands)
	Error       error
}

// ExecuteJob executes a job using the exact same infrastructure as the server.
// This deserializes the ProjectContext and calls the standard ProjectCommandRunner.
func (a *AgentExecutor) ExecuteJob(job *db.Job) ExecuteResult {
	log := a.Logger.With("job_id", job.ID)

	log.Info("executing job using standard Atlantis infrastructure")
	log.Info("job details: command=%q planDataSize=%d", job.Command, len(job.PlanData))

	// Deserialize the ProjectContext that was serialized by the master
	var projectCtx command.ProjectContext
	if err := json.Unmarshal(job.ProjectContextJSON, &projectCtx); err != nil {
		log.Err("failed to deserialize project context: %v", err)
		return ExecuteResult{
			Output:      "",
			PlanSuccess: false,
			Error:       fmt.Errorf("deserializing project context: %w", err),
		}
	}

	// Recreate the logger context for this project
	projectCtx.Log = log

	// Inject plan data from job (not in serialized JSON, stored separately in DB)
	projectCtx.PlanData = job.PlanData

	// Override ExecutionMode to "local" since we're already on the agent
	// This prevents ProjectCommandRunner from trying to schedule the job again
	projectCtx.ExecutionMode = "local"

	log.Info("deserialized project context: repo=%q project=%q workspace=%q dir=%q",
		projectCtx.Pull.BaseRepo.FullName,
		projectCtx.ProjectName,
		projectCtx.Workspace,
		projectCtx.RepoRelDir)

	// Debug: log steps configuration
	log.Info("project has %d steps configured", len(projectCtx.Steps))
	if len(projectCtx.Steps) == 0 {
		log.Warn("project context has no steps configured - this will cause execution to skip terraform commands")
	} else {
		for i, step := range projectCtx.Steps {
			log.Info("step %d: name=%q runCommand=%q extraArgs=%q", i, step.StepName, step.RunCommand, step.ExtraArgs)
		}
	}

	// Execute pre-workflow hooks if present
	// These run BEFORE plan/apply, typically for dynamic atlantis.yaml generation
	log.Info("checking for pre-workflow hooks...")
	if err := a.executePreWorkflowHooks(job, &projectCtx, log); err != nil {
		log.Err("pre-workflow hooks failed: %v", err)
		return ExecuteResult{
			Output:      fmt.Sprintf("Pre-workflow hooks failed: %s", err.Error()),
			PlanSuccess: false,
			Error:       fmt.Errorf("pre-workflow hooks failed: %w", err),
		}
	}
	log.Info("pre-workflow hooks check completed")

	// Execute command using the standard ProjectCommandRunner
	// This handles git clone, terraform execution, all hooks, policies, etc.
	log.Info("executing %s command via ProjectCommandRunner (steps: %d)", job.Command, len(projectCtx.Steps))

	var result command.ProjectCommandOutput
	cmdName, err := command.ParseCommandName(job.Command)
	if err != nil {
		log.Err("invalid command: %s", job.Command)
		return ExecuteResult{
			Output:      "",
			PlanSuccess: false,
			Error:       fmt.Errorf("invalid command %s: %w", job.Command, err),
		}
	}

	switch cmdName {
	case command.Plan:
		result = a.ProjectCommandRunner.Plan(projectCtx)
	case command.Apply:
		// Plan data is in projectCtx.PlanData - will be written to disk after clone in doApply()
		if len(job.PlanData) > 0 {
			log.Info("apply command: plan data available (size: %d bytes), will be written after clone", len(job.PlanData))
		} else {
			log.Warn("apply command but no plan data available - terraform may fail")
		}
		result = a.ProjectCommandRunner.Apply(projectCtx)
	case command.PolicyCheck:
		result = a.ProjectCommandRunner.PolicyCheck(projectCtx)
	case command.Version:
		result = a.ProjectCommandRunner.Version(projectCtx)
	case command.Import:
		result = a.ProjectCommandRunner.Import(projectCtx)
	default:
		log.Err("unsupported command: %s", job.Command)
		return ExecuteResult{
			Output:      "",
			PlanSuccess: false,
			Error:       fmt.Errorf("unsupported command: %s", job.Command),
		}
	}

	log.Info("command execution completed: failure=%q", result.Failure)

	// Extract output from result
	// The ProjectCommandOutput contains formatted markdown output
	var output string
	var planData []byte

	// Debug: Log what we got from ProjectCommandRunner
	log.Info("result details: Failure=%q PlanSuccess=%v ApplySuccess=%q VersionSuccess=%q Error=%v",
		result.Failure,
		result.PlanSuccess != nil,
		result.ApplySuccess,
		result.VersionSuccess,
		result.Error)

	if result.PlanSuccess != nil {
		log.Info("PlanSuccess details: TerraformOutput length=%d LockURL=%s RePlanCmd=%s ApplyCmd=%s",
			len(result.PlanSuccess.TerraformOutput),
			result.PlanSuccess.LockURL,
			result.PlanSuccess.RePlanCmd,
			result.PlanSuccess.ApplyCmd)
	}

	// Handle Error first - this takes precedence
	if result.Error != nil {
		output = fmt.Sprintf("**Error executing command:**\n```\n%s\n```", result.Error.Error())
		log.Err("job execution failed with error: %s", result.Error)
	} else if result.Failure != "" {
		output = result.Failure
	} else if result.PlanSuccess != nil && result.PlanSuccess.TerraformOutput != "" {
		output = result.PlanSuccess.TerraformOutput

		// Read plan file if this was a plan command
		if cmdName == command.Plan {
			repoDir, err := a.WorkingDir.GetWorkingDir(projectCtx.Pull.BaseRepo, projectCtx.Pull, projectCtx.Workspace)
			if err == nil {
				projectDir := filepath.Join(repoDir, projectCtx.RepoRelDir)
				log.Info("searching for plan file in: %s", projectDir)
				log.Info("  repoDir=%s, RepoRelDir=%s, Workspace=%s", repoDir, projectCtx.RepoRelDir, projectCtx.Workspace)
				log.Info("  ProjectName=%s", projectCtx.ProjectName)
				log.Info("  expected filename from GetPlanFilename: %s", runtime.GetPlanFilename(projectCtx.Workspace, projectCtx.ProjectName))

				// Check if directory exists and is accessible
				if stat, err := os.Stat(projectDir); err != nil {
					log.Warn("project directory does not exist or is not accessible: %s", err)
				} else if !stat.IsDir() {
					log.Warn("project path is not a directory: %s", projectDir)
				} else {
					// First, try the expected plan filename
					expectedPlanFile := filepath.Join(projectDir, runtime.GetPlanFilename(projectCtx.Workspace, projectCtx.ProjectName))
					if _, err := os.Stat(expectedPlanFile); err == nil {
						log.Info("found expected plan file: %s", expectedPlanFile)
						planData, err = os.ReadFile(expectedPlanFile)
						if err != nil {
							log.Warn("failed to read plan file: %s", err)
						} else {
							log.Info("read plan file: %d bytes", len(planData))
						}
					} else {
						// If expected file not found, scan directory
						log.Info("expected plan file not found, scanning directory...")
						entries, err := os.ReadDir(projectDir)
						if err != nil {
							log.Warn("failed to read project directory for plan files: %s", err)
						} else {
							log.Info("project directory contains %d entries", len(entries))
							// Log all files for debugging
							for _, entry := range entries {
								log.Info("  entry: %s (isDir=%v, ext=%s)", entry.Name(), entry.IsDir(), filepath.Ext(entry.Name()))
							}

							// Also check parent directory (repoDir) in case plan is written there
							log.Info("checking parent directory (repoDir): %s", repoDir)
							parentEntries, err := os.ReadDir(repoDir)
							if err == nil {
								log.Info("parent directory contains %d entries", len(parentEntries))
								for _, entry := range parentEntries {
									if !entry.IsDir() && filepath.Ext(entry.Name()) == ".tfplan" {
										log.Info("  parent entry: %s (ext=%s)", entry.Name(), filepath.Ext(entry.Name()))
									}
								}
							}

							var planPath string
							for _, entry := range entries {
								if !entry.IsDir() && filepath.Ext(entry.Name()) == ".tfplan" {
									planPath = filepath.Join(projectDir, entry.Name())
									log.Info("found plan file: %s", planPath)
									break
								}
							}

							if planPath != "" {
								planData, err = os.ReadFile(planPath)
								if err != nil {
									log.Warn("failed to read plan file: %s", err)
								} else {
									log.Info("read plan file: %d bytes", len(planData))
								}
							} else {
								log.Warn("no .tfplan file found in project directory: %s", projectDir)
							}
						}
					}
				}
			} else {
				log.Warn("failed to get working directory for plan file: %s", err)
			}
		}
	} else if result.ApplySuccess != "" {
		output = result.ApplySuccess
	} else if result.VersionSuccess != "" {
		output = result.VersionSuccess
	}

	log.Info("ExecuteJob complete: command=%s planDataSize=%d bytes success=%v error=%v",
		job.Command, len(planData), result.Error == nil && result.Failure == "", result.Error != nil)

	return ExecuteResult{
		Output:      output,
		PlanSuccess: result.Error == nil && result.Failure == "",
		PlanData:    planData,
		Error:       result.Error,
	}
}

// executePreWorkflowHooks runs pre-workflow hooks extracted from job metadata
// These hooks run before the actual terraform command and can generate dynamic config
func (a *AgentExecutor) executePreWorkflowHooks(job *db.Job, projectCtx *command.ProjectContext, log logging.SimpleLogging) error {
	log.Info("executePreWorkflowHooks: starting")

	// Parse metadata to extract pre-workflow hooks
	if len(job.Metadata) == 0 {
		log.Info("executePreWorkflowHooks: no metadata in job - skipping")
		return nil
	}

	log.Info("executePreWorkflowHooks: parsing metadata (%d bytes)", len(job.Metadata))
	var metadata map[string]interface{}
	if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
		log.Warn("executePreWorkflowHooks: failed to unmarshal job metadata: %v", err)
		return nil // Don't fail job for metadata parsing errors
	}

	// Extract pre_workflow_hooks from metadata
	hooksData, exists := metadata["pre_workflow_hooks"]
	if !exists {
		log.Info("executePreWorkflowHooks: no pre_workflow_hooks in metadata - skipping")
		return nil
	}

	log.Info("executePreWorkflowHooks: found pre_workflow_hooks in metadata")

	// Re-marshal and unmarshal to convert interface{} to valid.WorkflowHook
	hooksJSON, err := json.Marshal(hooksData)
	if err != nil {
		log.Warn("failed to marshal hooks data: %v", err)
		return nil
	}

	var hooks []valid.WorkflowHook
	if err := json.Unmarshal(hooksJSON, &hooks); err != nil {
		log.Warn("failed to unmarshal workflow hooks: %v", err)
		return nil
	}

	if len(hooks) == 0 {
		log.Debug("no pre-workflow hooks to execute")
		return nil
	}

	log.Info("executing %d pre-workflow hooks", len(hooks))

	// Get the cloned repo directory
	// The repo should already be cloned at this point by the agent initialization
	repoDir, err := a.WorkingDir.GetWorkingDir(projectCtx.Pull.BaseRepo, projectCtx.Pull, projectCtx.Workspace)
	if err != nil {
		return fmt.Errorf("getting repo directory: %w", err)
	}

	// Execute each hook
	for i, hook := range hooks {
		hookDesc := hook.StepDescription
		if hookDesc == "" {
			hookDesc = fmt.Sprintf("pre workflow hook #%d", i)
		}

		// Check if this hook should run for this command
		if hook.Commands != "" && !strings.Contains(hook.Commands, job.Command) {
			log.Debug("skipping hook '%s' - not configured for command '%s'", hookDesc, job.Command)
			continue
		}

		log.Info("running pre-workflow hook: %s", hookDesc)

		shell := hook.Shell
		if shell == "" {
			shell = "sh"
		}
		shellArgs := hook.ShellArgs
		if shellArgs == "" {
			shellArgs = "-c"
		}

		// Run the hook command
		cmd := exec.Command(shell, shellArgs, hook.RunCommand)
		cmd.Dir = repoDir

		// Set environment variables (same as runtime.DefaultPreWorkflowHookRunner)
		baseEnvVars := os.Environ()
		customEnvVars := map[string]string{
			"BASE_BRANCH_NAME": projectCtx.Pull.BaseBranch,
			"BASE_REPO_NAME":   projectCtx.Pull.BaseRepo.Name,
			"BASE_REPO_OWNER":  projectCtx.Pull.BaseRepo.Owner,
			"DIR":              repoDir,
			"HEAD_BRANCH_NAME": projectCtx.Pull.HeadBranch,
			"HEAD_COMMIT":      projectCtx.Pull.HeadCommit,
			"HEAD_REPO_NAME":   projectCtx.HeadRepo.Name,
			"HEAD_REPO_OWNER":  projectCtx.HeadRepo.Owner,
			"PULL_AUTHOR":      projectCtx.Pull.Author,
			"PULL_NUM":         fmt.Sprintf("%d", projectCtx.Pull.Num),
			"PULL_URL":         projectCtx.Pull.URL,
			"USER_NAME":        projectCtx.User.Username,
			"COMMAND_NAME":     job.Command,
		}

		finalEnvVars := baseEnvVars
		for key, val := range customEnvVars {
			finalEnvVars = append(finalEnvVars, fmt.Sprintf("%s=%s", key, val))
		}
		cmd.Env = finalEnvVars

		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Err("pre-workflow hook '%s' failed: %s", hookDesc, err)
			log.Err("hook command: %s %s %q", shell, shellArgs, hook.RunCommand)
			log.Err("hook output:\n%s", string(output))
			return fmt.Errorf("pre-workflow hook '%s' failed: %w\nOutput: %s", hookDesc, err, string(output))
		}
		if len(output) > 0 {
			log.Info("pre-workflow hook '%s' output:\n%s", hookDesc, string(output))
		}
	}

	log.Info("pre-workflow hooks completed successfully")
	return nil
}
