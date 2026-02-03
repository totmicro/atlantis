// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/events/vcs/common"
	"github.com/runatlantis/atlantis/server/events/vcs/github"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var agentExecutorCmd = &cobra.Command{
	Use:   "agent-executor",
	Short: "Execute a single job (used by ephemeral pods)",
	Long: `Execute a single job and exit. This command is used by ephemeral Kubernetes pods
spawned by static agents in ephemeral mode.

The agent-executor:
- Connects to the master server
- Fetches a specific job by ID
- Executes the job (plan/apply/unlock)
- Reports results
- Exits

This command should not be run directly by users. It's invoked automatically
by the static agent when running in ephemeral mode.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAgentExecutor(cmd)
	},
}

func init() {
	// Job execution
	agentExecutorCmd.Flags().String("job-id", "", "Job ID to execute (required)")

	// Connection settings (same as agent)
	agentExecutorCmd.Flags().String(AgentMasterAddressFlag, "", "Master server gRPC address (required)")
	agentExecutorCmd.Flags().String(AgentTokenFlag, "", "Authentication token for connecting to master (required)")
	agentExecutorCmd.Flags().String("agent-token-file", "", "Path to file containing authentication token")

	// Executor configuration (same as agent)
	agentExecutorCmd.Flags().String(AgentTerraformBinaryFlag, "terraform", "Path to terraform binary")
	agentExecutorCmd.Flags().String(AgentWorkDirFlag, "/tmp/atlantis-agent", "Working directory for job execution")
	agentExecutorCmd.Flags().String(AgentLogLevelFlag, "info", "Log level: debug, info, warn, error")

	// VCS credentials (same as agent - passed from parent pod)
	agentExecutorCmd.Flags().String(AgentGithubUserFlag, "", "GitHub username")
	agentExecutorCmd.Flags().String(AgentGithubTokenFlag, "", "GitHub token")
	agentExecutorCmd.Flags().Int64(AgentGithubAppIDFlag, 0, "GitHub App ID")
	agentExecutorCmd.Flags().String(AgentGithubAppKeyFileFlag, "", "GitHub App private key file")
	agentExecutorCmd.Flags().String(AgentGithubHostnameFlag, "github.com", "GitHub hostname")
	agentExecutorCmd.Flags().String(AgentGitlabUserFlag, "", "GitLab username")
	agentExecutorCmd.Flags().String(AgentGitlabTokenFlag, "", "GitLab token")
	agentExecutorCmd.Flags().String(AgentGitlabHostnameFlag, "gitlab.com", "GitLab hostname")

	// Bind flags to viper
	viper.BindPFlag("job-id", agentExecutorCmd.Flags().Lookup("job-id"))
	viper.BindPFlag(AgentMasterAddressFlag, agentExecutorCmd.Flags().Lookup(AgentMasterAddressFlag))
	viper.BindPFlag(AgentTokenFlag, agentExecutorCmd.Flags().Lookup(AgentTokenFlag))
	viper.BindPFlag("agent-token-file", agentExecutorCmd.Flags().Lookup("agent-token-file"))
	viper.BindPFlag(AgentTerraformBinaryFlag, agentExecutorCmd.Flags().Lookup(AgentTerraformBinaryFlag))
	viper.BindPFlag(AgentWorkDirFlag, agentExecutorCmd.Flags().Lookup(AgentWorkDirFlag))
	viper.BindPFlag(AgentLogLevelFlag, agentExecutorCmd.Flags().Lookup(AgentLogLevelFlag))
	viper.BindPFlag(AgentGithubUserFlag, agentExecutorCmd.Flags().Lookup(AgentGithubUserFlag))
	viper.BindPFlag(AgentGithubTokenFlag, agentExecutorCmd.Flags().Lookup(AgentGithubTokenFlag))
	viper.BindPFlag(AgentGithubAppIDFlag, agentExecutorCmd.Flags().Lookup(AgentGithubAppIDFlag))
	viper.BindPFlag(AgentGithubAppKeyFileFlag, agentExecutorCmd.Flags().Lookup(AgentGithubAppKeyFileFlag))
	viper.BindPFlag(AgentGithubHostnameFlag, agentExecutorCmd.Flags().Lookup(AgentGithubHostnameFlag))
	viper.BindPFlag(AgentGitlabUserFlag, agentExecutorCmd.Flags().Lookup(AgentGitlabUserFlag))
	viper.BindPFlag(AgentGitlabTokenFlag, agentExecutorCmd.Flags().Lookup(AgentGitlabTokenFlag))
	viper.BindPFlag(AgentGitlabHostnameFlag, agentExecutorCmd.Flags().Lookup(AgentGitlabHostnameFlag))

	RootCmd.AddCommand(agentExecutorCmd)
}

func runAgentExecutor(cmd *cobra.Command) error {
	// Get configuration
	jobID := viper.GetString("job-id")
	masterAddress := viper.GetString(AgentMasterAddressFlag)
	token := viper.GetString(AgentTokenFlag)
	tokenFile := viper.GetString("agent-token-file")
	workDir := viper.GetString(AgentWorkDirFlag)
	logLevel := viper.GetString(AgentLogLevelFlag)

	// Validate required fields
	if jobID == "" {
		return fmt.Errorf("--job-id is required")
	}
	if masterAddress == "" {
		return fmt.Errorf("--%s is required", AgentMasterAddressFlag)
	}

	// Read token from file if provided
	if tokenFile != "" && token == "" {
		tokenBytes, err := os.ReadFile(tokenFile)
		if err != nil {
			return fmt.Errorf("reading token file: %w", err)
		}
		token = strings.TrimSpace(string(tokenBytes))
	}

	if token == "" {
		return fmt.Errorf("--%s or --agent-token-file is required", AgentTokenFlag)
	}

	// Initialize logger
	var level logging.LogLevel
	switch strings.ToLower(logLevel) {
	case "debug":
		level = logging.Debug
	case "info":
		level = logging.Info
	case "warn":
		level = logging.Warn
	case "error":
		level = logging.Error
	default:
		level = logging.Info
	}

	logger, err := logging.NewStructuredLoggerFromLevel(level)
	if err != nil {
		return fmt.Errorf("initializing logger: %w", err)
	}

	logger.Info("starting agent-executor for job %s", jobID)
	logger.Info("master address: %s", masterAddress)
	logger.Info("work dir: %s", workDir)

	// Create working directory
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("creating work directory: %w", err)
	}

	// Get VCS credentials from environment/flags
	githubUser := viper.GetString(AgentGithubUserFlag)
	githubToken := viper.GetString(AgentGithubTokenFlag)
	githubAppID := viper.GetInt64(AgentGithubAppIDFlag)
	githubAppKeyFile := viper.GetString(AgentGithubAppKeyFileFlag)
	githubHostname := viper.GetString(AgentGithubHostnameFlag)
	gitlabUser := viper.GetString(AgentGitlabUserFlag)
	gitlabToken := viper.GetString(AgentGitlabTokenFlag)
	gitlabHostname := viper.GetString(AgentGitlabHostnameFlag)

	// Debug: Log what credentials we received
	logger.Debug("VCS credentials: githubAppID=%d githubAppKeyFile=%q githubToken=%q gitlabToken=%q",
		githubAppID, githubAppKeyFile,
		func() string {
			if githubToken != "" {
				return "***"
			} else {
				return ""
			}
		}(),
		func() string {
			if gitlabToken != "" {
				return "***"
			} else {
				return ""
			}
		}())

	// Initialize Atlantis components
	atlantisConfig := agent.AgentInitConfig{
		Logger:           logger,
		DataDir:          workDir,
		TerraformVersion: "", // Use latest/default
		GithubUser:       githubUser,
		GithubToken:      githubToken,
		GithubAppID:      githubAppID,
		GithubAppKeyFile: githubAppKeyFile,
		GithubHostname:   githubHostname,
		GitlabUser:       gitlabUser,
		GitlabToken:      gitlabToken,
		GitlabHostname:   gitlabHostname,
	}

	components, err := agent.InitializeAtlantisComponents(atlantisConfig)
	if err != nil {
		logger.Err("failed to initialize Atlantis components: %v", err)
		return fmt.Errorf("initializing components: %w", err)
	}

	logger.Info("Atlantis components initialized")

	// Write git credentials for this ephemeral pod
	// (credentials from parent pod are NOT inherited since ~/.git-credentials is not in a mounted volume)
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		homeDir = "/root"
	}

	logger.Info("checking VCS credentials: githubAppID=%d hasKeyFile=%v hasToken=%v hasGitLabToken=%v",
		githubAppID, githubAppKeyFile != "", githubToken != "", gitlabToken != "")

	if githubAppID != 0 && githubAppKeyFile != "" {
		logger.Info("writing git credentials for GitHub App authentication")
		// Read the GitHub App private key
		keyBytes, err := os.ReadFile(githubAppKeyFile)
		if err != nil {
			logger.Warn("failed to read GitHub App key file: %v", err)
		} else {
			// Create credentials object to get a token
			hostname := githubHostname
			if hostname == "" {
				hostname = "github.com"
			}

			// Use github package to create credentials and get token
			credentials := &github.AppCredentials{
				AppID:    int64(githubAppID),
				Key:      keyBytes,
				Hostname: hostname,
			}

			token, err := credentials.GetToken()
			if err != nil {
				logger.Warn("failed to get GitHub App token: %v", err)
			} else {
				if err := common.WriteGitCreds("x-access-token", token, hostname, homeDir, logger, true); err != nil {
					logger.Warn("failed to write git credentials: %v", err)
				} else {
					logger.Info("git credentials written successfully for GitHub App")
				}
			}
		}
	} else if githubToken != "" {
		logger.Info("writing git credentials for GitHub token authentication")
		gitUser := githubUser
		if gitUser == "" {
			gitUser = "x-access-token"
		}
		hostname := githubHostname
		if hostname == "" {
			hostname = "github.com"
		}
		if err := common.WriteGitCreds(gitUser, githubToken, hostname, homeDir, logger, gitUser == "x-access-token"); err != nil {
			logger.Warn("failed to write git credentials: %v", err)
		} else {
			logger.Info("git credentials written successfully")
		}
	} else if gitlabToken != "" {
		logger.Info("writing git credentials for GitLab authentication")
		gitUser := gitlabUser
		if gitUser == "" {
			gitUser = "oauth2"
		}
		hostname := gitlabHostname
		if hostname == "" {
			hostname = "gitlab.com"
		}
		if err := common.WriteGitCreds(gitUser, gitlabToken, hostname, homeDir, logger, false); err != nil {
			logger.Warn("failed to write git credentials: %v", err)
		} else {
			logger.Info("git credentials written successfully")
		}
	} else {
		logger.Warn("no VCS credentials configured - git clone may fail")
	}

	// Create agent executor
	executor := agent.NewAgentExecutor(
		logger,
		components.ProjectCommandRunner,
		components.VCSClient,
		components.WorkingDir,
	)

	// Connect to master and fetch job
	logger.Info("connecting to master at %s", masterAddress)

	// Create gRPC client to fetch job details
	grpcClient, err := agent.NewGRPCClient(masterAddress, token, logger)
	if err != nil {
		logger.Err("failed to connect to master: %v", err)
		return fmt.Errorf("connecting to master: %w", err)
	}
	defer grpcClient.Close()

	logger.Info("connected to master, fetching job %s", jobID)

	// Fetch job from master
	job, err := grpcClient.GetJob(context.Background(), jobID)
	if err != nil {
		logger.Err("failed to fetch job: %v", err)
		return fmt.Errorf("fetching job: %w", err)
	}

	logger.Info("fetched job: repo=%s project=%s command=%s", job.RepoFullName, job.ProjectName, job.Command)

	// Execute the job
	logger.Info("executing job...")
	result := executor.ExecuteJob(job)

	// Report result to master
	logger.Info("reporting result to master...")
	if err := grpcClient.ReportJobResult(context.Background(), jobID, result); err != nil {
		logger.Err("failed to report result: %v", err)
		return fmt.Errorf("reporting result: %w", err)
	}

	if result.Error != nil {
		logger.Err("job execution failed: %v", result.Error)
		return fmt.Errorf("job execution failed: %w", result.Error)
	}

	logger.Info("job completed successfully")
	return nil
}
