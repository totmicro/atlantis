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
	"fmt"
	"os"

	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/core/runtime/policy"
	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/core/terraform/tfclient"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/events/vcs/azuredevops"
	"github.com/runatlantis/atlantis/server/events/vcs/common"
	"github.com/runatlantis/atlantis/server/events/vcs/github"
	"github.com/runatlantis/atlantis/server/events/vcs/gitlab"
	"github.com/runatlantis/atlantis/server/events/webhooks"
	"github.com/runatlantis/atlantis/server/jobs"
	"github.com/runatlantis/atlantis/server/logging"
)

// AtlantisComponents holds all the standard Atlantis components needed for execution
type AtlantisComponents struct {
	VCSClient            vcs.Client
	ProjectCommandRunner events.ProjectCommandRunner
	WorkingDir           events.WorkingDir
	TerraformClient      *tfclient.DefaultClient
	Logger               logging.SimpleLogging
}

// AgentInitConfig holds configuration for initializing agent components
type AgentInitConfig struct {
	// Working directory for clones and terraform execution
	DataDir string

	// Terraform configuration
	TerraformBinPath string
	TerraformVersion string

	// VCS configuration
	GithubUser          string
	GithubToken         string
	GithubAppID         int64
	GithubAppKey        string
	GithubAppKeyFile    string
	GithubHostname      string
	GithubWebhookSecret string

	GitlabUser     string
	GitlabToken    string
	GitlabHostname string

	BitbucketUser  string
	BitbucketToken string

	AzureDevopsUser  string
	AzureDevopsToken string

	// Logging
	Logger logging.SimpleLogging
}

// InitializeAtlantisComponents creates all the standard Atlantis components for an agent.
// This makes the agent functionally equivalent to the Atlantis server for command execution.
func InitializeAtlantisComponents(config AgentInitConfig) (*AtlantisComponents, error) {
	logger := config.Logger

	logger.Info("initializing Atlantis components for agent")

	// Initialize working directory handler
	workingDir := &events.FileWorkspace{
		DataDir:             config.DataDir,
		CheckoutMerge:       false,
		GpgNoSigningEnabled: true,
	}

	// Initialize terraform client
	distribution := terraform.NewDistributionTerraform()
	noopOutputHandler := &noOpProjectCmdOutputHandler{}

	terraformClient, err := tfclient.NewClient(
		logger,
		distribution,
		"",                      // binDir - will use PATH
		config.DataDir,          // cacheDir
		"",                      // tfeToken
		"",                      // tfeHostname
		config.TerraformVersion, // defaultVersion
		"default-tf-version",    // defaultVersionFlagName
		"https://releases.hashicorp.com/terraform", // tfDownloadURL
		true,              // tfDownloadAllowed
		true,              // usePluginCache
		noopOutputHandler, // projectCmdOutputHandler
	)
	if err != nil {
		return nil, fmt.Errorf("initializing terraform client: %w", err)
	}

	// Initialize VCS client (GitHub, GitLab, etc.)
	vcsClient, err := initializeVCSClient(config, logger)
	if err != nil {
		return nil, fmt.Errorf("initializing VCS client: %w", err)
	}

	// Wrap WorkingDir with GithubAppWorkingDir if using GitHub App
	// This ensures clone URLs don't have embedded credentials and git uses credential store
	var finalWorkingDir events.WorkingDir = workingDir
	if config.GithubAppID != 0 && config.GithubAppKeyFile != "" {
		logger.Info("wrapping WorkingDir with GithubAppWorkingDir for credential rotation")
		// Read the key again to create credentials
		keyBytes, err := os.ReadFile(config.GithubAppKeyFile)
		if err != nil {
			return nil, fmt.Errorf("reading GitHub App key for WorkingDir: %w", err)
		}
		hostname := config.GithubHostname
		if hostname == "" {
			hostname = "github.com"
		}
		credentials := &github.AppCredentials{
			AppID:    config.GithubAppID,
			Key:      keyBytes,
			Hostname: hostname,
		}
		finalWorkingDir = &events.GithubAppWorkingDir{
			WorkingDir:     workingDir,
			Credentials:    credentials,
			GithubHostname: hostname,
		}
	}

	// Initialize step runners for terraform commands
	defaultTfDistribution := terraformClient.DefaultDistribution()
	defaultTfVersion := terraformClient.DefaultVersion()

	initStepRunner := &runtime.InitStepRunner{
		TerraformExecutor:     terraformClient,
		DefaultTFDistribution: defaultTfDistribution,
		DefaultTFVersion:      defaultTfVersion,
	}

	planStepRunner := runtime.NewPlanStepRunner(terraformClient, defaultTfDistribution, defaultTfVersion, nil, nil)

	applyStepRunner := &runtime.ApplyStepRunner{
		TerraformExecutor:     terraformClient,
		DefaultTFDistribution: defaultTfDistribution,
		DefaultTFVersion:      defaultTfVersion,
	}

	showStepRunner, err := runtime.NewShowStepRunner(terraformClient, defaultTfDistribution, defaultTfVersion)
	if err != nil {
		return nil, fmt.Errorf("initializing show step runner: %w", err)
	}

	policyCheckStepRunner, err := runtime.NewPolicyCheckStepRunner(
		defaultTfDistribution,
		defaultTfVersion,
		policy.NewConfTestExecutorWorkflow(logger, config.DataDir, &policy.ConfTestGoGetterVersionDownloader{}),
	)
	if err != nil {
		return nil, fmt.Errorf("initializing policy check step runner: %w", err)
	}

	versionStepRunner := &runtime.VersionStepRunner{
		TerraformExecutor: terraformClient,
	}

	runStepRunner := &runtime.RunStepRunner{
		TerraformExecutor:       terraformClient,
		DefaultTFDistribution:   defaultTfDistribution,
		DefaultTFVersion:        defaultTfVersion,
		TerraformBinDir:         "",
		ProjectCmdOutputHandler: noopOutputHandler,
	}

	envStepRunner := &runtime.EnvStepRunner{
		RunStepRunner: runStepRunner,
	}

	multiEnvStepRunner := &runtime.MultiEnvStepRunner{
		RunStepRunner: runStepRunner,
	}

	// Initialize project command runner with all dependencies
	projectCommandRunner := &events.DefaultProjectCommandRunner{
		VcsClient:                 vcsClient,
		Locker:                    &noOpProjectLocker{}, // Agent doesn't need locking
		LockURLGenerator:          &noOpLockURLGenerator{},
		Logger:                    logger,
		InitStepRunner:            initStepRunner,
		PlanStepRunner:            planStepRunner,
		ShowStepRunner:            showStepRunner,
		ApplyStepRunner:           applyStepRunner,
		PolicyCheckStepRunner:     policyCheckStepRunner,
		VersionStepRunner:         versionStepRunner,
		RunStepRunner:             runStepRunner,
		EnvStepRunner:             envStepRunner,
		MultiEnvStepRunner:        multiEnvStepRunner,
		WorkingDir:                finalWorkingDir,
		Webhooks:                  &noOpWebhooksSender{},
		WorkingDirLocker:          events.NewDefaultWorkingDirLocker(),
		CommandRequirementHandler: &events.DefaultCommandRequirementHandler{},
		CancellationTracker:       &events.DefaultCancellationTracker{},
		// Agent doesn't use job scheduler or job store - it reports to master via gRPC
		JobScheduler: nil,
		JobStore:     nil,
	}

	logger.Info("Atlantis components initialized successfully")

	return &AtlantisComponents{
		VCSClient:            vcsClient,
		ProjectCommandRunner: projectCommandRunner,
		WorkingDir:           finalWorkingDir,
		TerraformClient:      terraformClient,
		Logger:               logger,
	}, nil
}

// initializeVCSClient creates the appropriate VCS client based on configuration
func initializeVCSClient(config AgentInitConfig, logger logging.SimpleLogging) (vcs.Client, error) {
	// Try GitHub first (most common)
	if config.GithubToken != "" || config.GithubAppID != 0 {
		return initializeGitHubClient(config, logger)
	}

	// Try GitLab
	if config.GitlabToken != "" {
		return initializeGitLabClient(config, logger)
	}

	// Try Bitbucket
	if config.BitbucketToken != "" {
		return initializeBitbucketClient(config, logger)
	}

	// Try Azure DevOps
	if config.AzureDevopsToken != "" {
		return initializeAzureDevOpsClient(config, logger)
	}

	return nil, fmt.Errorf("no VCS credentials configured - must provide GitHub, GitLab, Bitbucket, or Azure DevOps credentials")
}

// initializeGitHubClient creates a GitHub VCS client
func initializeGitHubClient(config AgentInitConfig, logger logging.SimpleLogging) (vcs.Client, error) {
	logger.Info("initializing GitHub VCS client")

	hostname := config.GithubHostname
	if hostname == "" {
		hostname = "github.com"
	}

	var credentials github.Credentials

	// GitHub App takes precedence over static tokens
	if config.GithubAppID != 0 {
		logger.Info("using GitHub App credentials (App ID: %d)", config.GithubAppID)

		// Read key from file or use direct key string
		key := config.GithubAppKey
		if key == "" && config.GithubAppKeyFile != "" {
			keyBytes, err := os.ReadFile(config.GithubAppKeyFile)
			if err != nil {
				return nil, fmt.Errorf("reading GitHub App key file: %w", err)
			}
			key = string(keyBytes)
		}

		if key == "" {
			return nil, fmt.Errorf("GitHub App key not provided")
		}

		credentials = &github.AppCredentials{
			AppID:    config.GithubAppID,
			Key:      []byte(key),
			Hostname: hostname,
		}
	} else if config.GithubToken != "" {
		logger.Info("using GitHub static token credentials (user: %s)", config.GithubUser)
		credentials = &github.UserCredentials{
			User:  config.GithubUser,
			Token: config.GithubToken,
		}
	} else {
		return nil, fmt.Errorf("no GitHub credentials provided - need either App credentials or static token")
	}

	// Create GitHub client
	ghConfig := github.Config{}
	client, err := github.New(hostname, credentials, ghConfig, 100, logger)
	if err != nil {
		return nil, fmt.Errorf("creating GitHub client: %w", err)
	}

	logger.Info("GitHub client initialized successfully")

	// Write git credentials for cloning (standard Atlantis approach)
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		homeDir = "/root"
	}

	// For GitHub, use x-access-token as username
	var gitUser string
	var gitToken string
	isGitHubAccessToken := false

	if config.GithubAppID != 0 {
		// GitHub App - get token from credentials
		gitUser = "x-access-token"
		isGitHubAccessToken = true
		token, err := credentials.GetToken()
		if err != nil {
			logger.Warn("failed to get GitHub App token for git credentials: %v", err)
		} else {
			gitToken = token
		}
	} else if config.GithubToken != "" {
		// Static token
		gitUser = config.GithubUser
		if gitUser == "" {
			gitUser = "x-access-token"
			isGitHubAccessToken = true
		}
		gitToken = config.GithubToken
	}

	if gitToken != "" {
		logger.Info("writing git credentials for GitHub authentication")
		if err := common.WriteGitCreds(gitUser, gitToken, hostname, homeDir, logger, isGitHubAccessToken); err != nil {
			logger.Warn("failed to write git credentials: %v", err)
		} else {
			logger.Info("git credentials written successfully")
		}
	}

	return client, nil
}

// initializeGitLabClient creates a GitLab VCS client
func initializeGitLabClient(config AgentInitConfig, logger logging.SimpleLogging) (vcs.Client, error) {
	logger.Info("initializing GitLab VCS client")

	hostname := config.GitlabHostname
	if hostname == "" {
		hostname = "gitlab.com"
	}

	if config.GitlabToken == "" {
		return nil, fmt.Errorf("GitLab token not provided")
	}

	client, err := gitlab.New(hostname, config.GitlabToken, nil, logger)
	if err != nil {
		return nil, fmt.Errorf("creating GitLab client: %w", err)
	}

	logger.Info("GitLab client initialized successfully")
	return client, nil
}

// initializeBitbucketClient creates a Bitbucket VCS client
func initializeBitbucketClient(config AgentInitConfig, logger logging.SimpleLogging) (vcs.Client, error) {
	logger.Info("initializing Bitbucket Cloud VCS client")

	if config.BitbucketUser == "" || config.BitbucketToken == "" {
		return nil, fmt.Errorf("Bitbucket credentials not provided")
	}

	// TODO: Implement Bitbucket Cloud support
	return nil, fmt.Errorf("Bitbucket Cloud support not yet implemented for agents")
}

// initializeAzureDevOpsClient creates an Azure DevOps VCS client
func initializeAzureDevOpsClient(config AgentInitConfig, logger logging.SimpleLogging) (vcs.Client, error) {
	logger.Info("initializing Azure DevOps VCS client")

	hostname := "dev.azure.com"
	if config.AzureDevopsToken == "" {
		return nil, fmt.Errorf("Azure DevOps token not provided")
	}

	client, err := azuredevops.New(hostname, config.AzureDevopsUser, config.AzureDevopsToken)
	if err != nil {
		return nil, fmt.Errorf("creating Azure DevOps client: %w", err)
	}

	logger.Info("Azure DevOps client initialized successfully")
	return client, nil
}

// noOpProjectLocker implements events.ProjectLocker for agents (no locking needed)
type noOpProjectLocker struct{}

func (n *noOpProjectLocker) TryLock(log logging.SimpleLogging, pull models.PullRequest, user models.User, workspace string, project models.Project, repoLocking bool) (*events.TryLockResponse, error) {
	return &events.TryLockResponse{
		LockAcquired:      true,
		LockFailureReason: "",
		UnlockFn: func() error {
			return nil
		},
	}, nil
}

// noOpLockURLGenerator is a placeholder for agents (they don't generate lock URLs)
type noOpLockURLGenerator struct{}

func (n *noOpLockURLGenerator) GenerateLockURL(lockID string) string {
	return ""
}

// noOpWebhooksSender is a placeholder for agents (they don't send webhooks)
type noOpWebhooksSender struct{}

func (n *noOpWebhooksSender) Send(log logging.SimpleLogging, res webhooks.ApplyResult) error {
	return nil
}

// noOpProjectCmdOutputHandler is a placeholder for agents
type noOpProjectCmdOutputHandler struct{}

func (n *noOpProjectCmdOutputHandler) Send(ctx command.ProjectContext, msg string, operationComplete bool) {
}

func (n *noOpProjectCmdOutputHandler) SendLog(jobID string, line string) {}

func (n *noOpProjectCmdOutputHandler) SendWorkflowHook(ctx models.WorkflowHookCommandContext, msg string, operationComplete bool) {
}

func (n *noOpProjectCmdOutputHandler) Register(jobID string, receiver chan string) {}

func (n *noOpProjectCmdOutputHandler) Deregister(jobID string, receiver chan string) {}

func (n *noOpProjectCmdOutputHandler) IsKeyExists(key string) bool { return false }

func (n *noOpProjectCmdOutputHandler) Handle() {}

func (n *noOpProjectCmdOutputHandler) GetReceiverBufferSize() int { return 0 }

func (n *noOpProjectCmdOutputHandler) GetPullToJobMapping() []jobs.PullInfoWithJobIDs { return nil }

func (n *noOpProjectCmdOutputHandler) SetJobURLWithStatus(ctx command.ProjectContext, cmdName command.Name, status models.CommitStatus, res *command.ProjectCommandOutput) error {
	return nil
}

func (n *noOpProjectCmdOutputHandler) CleanUp(pullInfo jobs.PullInfo) {
	// No-op for agent execution
}
