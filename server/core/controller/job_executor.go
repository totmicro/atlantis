package controller

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/logging"
)

// JobExecutor executes Terraform jobs
type JobExecutor struct {
	config JobExecutorConfig
	logger logging.SimpleLogging
}

// JobExecutorConfig configures job execution
type JobExecutorConfig struct {
	WorkDir          string
	TerraformBinPath string
	Logger           logging.SimpleLogging
}

// JobResult contains the result of job execution
type JobResult struct {
	Status       string // completed, failed, timeout
	Output       string
	PlanData     []byte
	ExitCode     int
	ErrorMessage string
}

// NewJobExecutor creates a new job executor
func NewJobExecutor(config JobExecutorConfig) *JobExecutor {
	if config.TerraformBinPath == "" {
		config.TerraformBinPath = "terraform"
	}
	return &JobExecutor{
		config: config,
		logger: config.Logger,
	}
}

// Execute runs a job and returns the result
func (je *JobExecutor) Execute(ctx context.Context, job *proto.JobAssignment) *JobResult {
	result := &JobResult{
		Status: "failed",
	}

	// Validate command first before doing any work
	switch job.Command {
	case "plan", "apply", "unlock":
		// Valid command, continue
	default:
		result.ErrorMessage = fmt.Sprintf("unknown command: %s", job.Command)
		je.logger.Err("job %s: %s", job.JobId, result.ErrorMessage)
		return result
	}

	// Create job working directory
	jobDir := filepath.Join(je.config.WorkDir, job.JobId)
	if err := os.MkdirAll(jobDir, 0755); err != nil {
		result.ErrorMessage = fmt.Sprintf("creating job directory: %v", err)
		je.logger.Err("job %s: %s", job.JobId, result.ErrorMessage)
		return result
	}
	defer je.cleanup(jobDir)

	je.logger.Info("job %s: executing in %s", job.JobId, jobDir)

	// Clone repository
	repoDir := filepath.Join(jobDir, "repo")
	if err := je.cloneRepo(ctx, job, repoDir); err != nil {
		result.ErrorMessage = fmt.Sprintf("cloning repository: %v", err)
		je.logger.Err("job %s: %s", job.JobId, result.ErrorMessage)
		return result
	}

	// Navigate to project directory
	projectDir := filepath.Join(repoDir, job.ProjectDir)
	if _, err := os.Stat(projectDir); err != nil {
		result.ErrorMessage = fmt.Sprintf("project directory not found: %v", err)
		je.logger.Err("job %s: %s", job.JobId, result.ErrorMessage)
		return result
	}

	// Execute command based on job type
	switch job.Command {
	case "plan":
		return je.executePlan(ctx, job, projectDir)
	case "apply":
		return je.executeApply(ctx, job, projectDir)
	case "unlock":
		return je.executeUnlock(ctx, job, projectDir)
	default:
		// Should never reach here due to validation above
		result.ErrorMessage = fmt.Sprintf("unknown command: %s", job.Command)
		je.logger.Err("job %s: %s", job.JobId, result.ErrorMessage)
		return result
	}
}

// cloneRepo clones the repository
func (je *JobExecutor) cloneRepo(ctx context.Context, job *proto.JobAssignment, targetDir string) error {
	cloneURL := job.RepoCloneUrl

	// Debug: Log VCS credentials status
	je.logger.Info("job %s: VcsCredentials nil=%v", job.JobId, job.VcsCredentials == nil)
	if job.VcsCredentials != nil {
		je.logger.Info("job %s: VcsCredentials user=%q token_len=%d", job.JobId, job.VcsCredentials.Username, len(job.VcsCredentials.Token))
	}

	// Inject credentials into clone URL if provided
	if job.VcsCredentials != nil && job.VcsCredentials.Username != "" && job.VcsCredentials.Token != "" {
		// Replace https:// with https://user:token@
		cloneURL = injectCredentialsIntoURL(cloneURL, job.VcsCredentials.Username, job.VcsCredentials.Token)
		je.logger.Info("job %s: cloning with authenticated URL (length=%d)", job.JobId, len(cloneURL))
	} else {
		je.logger.Info("job %s: cloning %s (no credentials)", job.JobId, job.RepoCloneUrl)
	}

	// Build git clone command
	args := []string{"clone", "--depth=1"}

	// Add branch if specified
	if job.PullBranch != "" {
		args = append(args, "--branch", job.PullBranch)
	}

	args = append(args, cloneURL, targetDir)

	// Debug: Show sanitized URL (hide token)
	sanitizedURL := cloneURL
	if job.VcsCredentials != nil && job.VcsCredentials.Token != "" {
		sanitizedURL = strings.Replace(cloneURL, job.VcsCredentials.Token, "***", -1)
	}
	je.logger.Info("job %s: git clone command: git clone --depth=1 %s %s", job.JobId, sanitizedURL, targetDir)

	cmd := exec.CommandContext(ctx, "git", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git clone failed: %w\nOutput: %s", err, string(output))
	}

	// Checkout specific commit if provided
	if job.PullCommitSha != "" {
		je.logger.Info("job %s: checking out commit %s", job.JobId, job.PullCommitSha)
		checkoutCmd := exec.CommandContext(ctx, "git", "-C", targetDir, "checkout", job.PullCommitSha)
		if output, err := checkoutCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git checkout failed: %w\nOutput: %s", err, string(output))
		}
	}

	je.logger.Info("job %s: repository cloned successfully", job.JobId)
	return nil
}

// injectCredentialsIntoURL injects username and token into an HTTPS URL
func injectCredentialsIntoURL(rawURL, username, token string) string {
	// URL-encode username and token (GitHub App bot names contain [bot] which needs encoding)
	encodedUsername := url.QueryEscape(username)
	encodedToken := url.QueryEscape(token)

	// Simple string replacement: https:// → https://user:token@
	if len(rawURL) > 8 && rawURL[:8] == "https://" {
		return "https://" + encodedUsername + ":" + encodedToken + "@" + rawURL[8:]
	}
	return rawURL
}

// executePlan runs terraform plan
func (je *JobExecutor) executePlan(ctx context.Context, job *proto.JobAssignment, workDir string) *JobResult {
	result := &JobResult{}
	output := &bytes.Buffer{}

	je.logger.Info("job %s: running terraform plan", job.JobId)

	// Run terraform init
	if err := je.runTerraform(ctx, workDir, output, "init", "-input=false"); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform init failed: %v", err)
		result.ExitCode = 1
		return result
	}

	// Run terraform plan
	planArgs := []string{"plan", "-input=false", "-no-color"}
	if job.Workspace != "" && job.Workspace != "default" {
		// Select workspace
		je.runTerraform(ctx, workDir, output, "workspace", "select", job.Workspace)
	}

	planArgs = append(planArgs, "-out=tfplan")

	if err := je.runTerraform(ctx, workDir, output, planArgs...); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform plan failed: %v", err)
		result.ExitCode = 1
		return result
	}

	// Read plan file
	planPath := filepath.Join(workDir, "tfplan")
	planData, err := os.ReadFile(planPath)
	if err != nil {
		je.logger.Warn("job %s: could not read plan file: %v", job.JobId, err)
	} else {
		result.PlanData = planData
	}

	result.Status = "completed"
	result.Output = output.String()
	result.ExitCode = 0

	je.logger.Info("job %s: terraform plan completed successfully", job.JobId)
	return result
}

// executeApply runs terraform apply
func (je *JobExecutor) executeApply(ctx context.Context, job *proto.JobAssignment, workDir string) *JobResult {
	result := &JobResult{}
	output := &bytes.Buffer{}

	je.logger.Info("job %s: running terraform apply", job.JobId)

	// Run terraform init
	if err := je.runTerraform(ctx, workDir, output, "init", "-input=false"); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform init failed: %v", err)
		result.ExitCode = 1
		return result
	}

	// Select workspace if needed
	if job.Workspace != "" && job.Workspace != "default" {
		je.runTerraform(ctx, workDir, output, "workspace", "select", job.Workspace)
	}

	// If plan data provided, write it to file
	var applyArgs []string
	if len(job.PlanData) > 0 {
		planPath := filepath.Join(workDir, "tfplan")
		if err := os.WriteFile(planPath, job.PlanData, 0644); err != nil {
			result.Status = "failed"
			result.ErrorMessage = fmt.Sprintf("writing plan file: %v", err)
			result.ExitCode = 1
			return result
		}
		applyArgs = []string{"apply", "-input=false", "-no-color", "tfplan"}
	} else {
		applyArgs = []string{"apply", "-input=false", "-no-color", "-auto-approve"}
	}

	if err := je.runTerraform(ctx, workDir, output, applyArgs...); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform apply failed: %v", err)
		result.ExitCode = 1
		return result
	}

	result.Status = "completed"
	result.Output = output.String()
	result.ExitCode = 0

	je.logger.Info("job %s: terraform apply completed successfully", job.JobId)
	return result
}

// executeUnlock runs terraform force-unlock
func (je *JobExecutor) executeUnlock(ctx context.Context, job *proto.JobAssignment, workDir string) *JobResult {
	result := &JobResult{}
	output := &bytes.Buffer{}

	je.logger.Info("job %s: running terraform force-unlock", job.JobId)

	// Run terraform init
	if err := je.runTerraform(ctx, workDir, output, "init", "-input=false"); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform init failed: %v", err)
		result.ExitCode = 1
		return result
	}

	// Select workspace if needed
	if job.Workspace != "" && job.Workspace != "default" {
		je.runTerraform(ctx, workDir, output, "workspace", "select", job.Workspace)
	}

	// Get lock ID from metadata (if available)
	lockID := ""
	if job.Metadata != nil {
		if id, ok := job.Metadata["lock_id"]; ok {
			lockID = id
		}
	}

	if lockID == "" {
		result.Status = "failed"
		result.ErrorMessage = "lock ID not provided"
		result.ExitCode = 1
		return result
	}

	if err := je.runTerraform(ctx, workDir, output, "force-unlock", "-force", lockID); err != nil {
		result.Status = "failed"
		result.Output = output.String()
		result.ErrorMessage = fmt.Sprintf("terraform force-unlock failed: %v", err)
		result.ExitCode = 1
		return result
	}

	result.Status = "completed"
	result.Output = output.String()
	result.ExitCode = 0

	je.logger.Info("job %s: terraform force-unlock completed successfully", job.JobId)
	return result
}

// runTerraform executes a terraform command
func (je *JobExecutor) runTerraform(ctx context.Context, workDir string, output *bytes.Buffer, args ...string) error {
	cmd := exec.CommandContext(ctx, je.config.TerraformBinPath, args...)
	cmd.Dir = workDir
	cmd.Stdout = output
	cmd.Stderr = output

	// Set environment variables
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "TF_IN_AUTOMATION=1")

	je.logger.Debug("running: %s %s", je.config.TerraformBinPath, strings.Join(args, " "))

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)

	if err != nil {
		je.logger.Warn("terraform command failed after %s: %v", duration, err)
		return err
	}

	je.logger.Debug("terraform command completed in %s", duration)
	return nil
}

// cleanup removes the job working directory
func (je *JobExecutor) cleanup(jobDir string) {
	je.logger.Debug("cleaning up %s", jobDir)
	if err := os.RemoveAll(jobDir); err != nil {
		je.logger.Warn("failed to cleanup %s: %v", jobDir, err)
	}
}
