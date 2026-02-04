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

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/controller"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// Agent flag names
	AgentIDFlag              = "agent-id"
	AgentNameFlag            = "agent-name"
	AgentMasterAddressFlag   = "agent-master-address"
	AgentTokenFlag           = "agent-token"
	AgentClusterNameFlag     = "agent-cluster-name"
	AgentNamespaceFlag       = "agent-namespace"
	AgentCapacityFlag        = "agent-capacity"
	AgentLabelsFlag          = "agent-labels"
	AgentHeartbeatFlag       = "agent-heartbeat-interval"
	AgentTerraformBinaryFlag = "agent-terraform-binary"
	AgentGitBinaryFlag       = "agent-git-binary"
	AgentWorkDirFlag         = "agent-work-dir"
	AgentLogLevelFlag        = "agent-log-level"

	// Agent mode flags
	AgentModeFlag            = "agent-mode"
	AgentEphemeralCPUFlag    = "ephemeral-cpu"
	AgentEphemeralMemoryFlag = "ephemeral-memory"
	AgentEphemeralTTLFlag    = "ephemeral-ttl-seconds"
	AgentEphemeralImageFlag  = "ephemeral-image"

	// VCS credential flags
	AgentGithubUserFlag       = "gh-user"
	AgentGithubTokenFlag      = "gh-token"
	AgentGithubAppIDFlag      = "gh-app-id"
	AgentGithubAppKeyFileFlag = "gh-app-key-file"
	AgentGithubHostnameFlag   = "gh-hostname"
	AgentGitlabUserFlag       = "gitlab-user"
	AgentGitlabTokenFlag      = "gitlab-token"
	AgentGitlabHostnameFlag   = "gitlab-hostname"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Start an Atlantis agent controller",
	Long: `Start an Atlantis agent controller that connects to a master server
and executes Terraform jobs.

The agent controller:
- Connects to the master server via gRPC
- Registers itself with the master
- Receives job assignments
- Executes Terraform plan/apply/unlock commands
- Reports results back to the master

Example:
  atlantis agent \
    --agent-id=agent-us-east-1a-01 \
    --agent-master-address=atlantis-master:50051 \
    --agent-token=your-secret-token \
    --agent-labels=environment=production,region=us-east-1

Environment variables:
  All flags can be set via environment variables with the ATLANTIS_ prefix.
  For example: ATLANTIS_AGENT_ID, ATLANTIS_AGENT_TOKEN, etc.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAgent(cmd)
	},
}

func init() {
	// Agent identification
	agentCmd.Flags().String(AgentIDFlag, "", "Unique identifier for this agent (required). If not set, will use pod name from HOSTNAME or generate one.")
	agentCmd.Flags().String(AgentNameFlag, "", "Human-readable name for this agent (optional)")
	agentCmd.Flags().String(AgentClusterNameFlag, "", "Kubernetes cluster name (optional)")
	agentCmd.Flags().String(AgentNamespaceFlag, "atlantis", "Kubernetes namespace (optional)")

	// Connection settings
	agentCmd.Flags().String(AgentMasterAddressFlag, "", "Master server gRPC address (e.g. master:50051) (required)")
	agentCmd.Flags().String(AgentTokenFlag, "", "Authentication token for connecting to master (required, or use --agent-token-file)")
	agentCmd.Flags().String("agent-token-file", "", "Path to file containing authentication token")

	// Capacity and behavior
	agentCmd.Flags().Int(AgentCapacityFlag, 5, "Maximum number of concurrent jobs this agent can execute")
	agentCmd.Flags().String(AgentHeartbeatFlag, "30s", "Interval for sending heartbeats to master (e.g. 30s, 1m)")

	// Labels for job routing
	agentCmd.Flags().String(AgentLabelsFlag, "", "Comma-separated labels for job routing (e.g. environment=prod,region=us-east-1)")

	// Executor configuration
	agentCmd.Flags().String(AgentTerraformBinaryFlag, "terraform", "Path to terraform binary")
	agentCmd.Flags().String(AgentGitBinaryFlag, "git", "Path to git binary")
	agentCmd.Flags().String(AgentWorkDirFlag, "/tmp/atlantis-agent", "Working directory for job execution")

	// Logging
	agentCmd.Flags().String(AgentLogLevelFlag, "info", "Log level: debug, info, warn, error")

	// Agent mode and ephemeral configuration
	agentCmd.Flags().String(AgentModeFlag, "static", "Agent mode: 'static' (default) or 'ephemeral' (spawns jobs in k8s pods)")
	agentCmd.Flags().String(AgentEphemeralCPUFlag, "1000m", "CPU request for ephemeral pods (e.g. 500m, 1000m, 2)")
	agentCmd.Flags().String(AgentEphemeralMemoryFlag, "2Gi", "Memory request for ephemeral pods (e.g. 1Gi, 2Gi, 4Gi)")
	agentCmd.Flags().Int32(AgentEphemeralTTLFlag, 300, "TTL in seconds after which ephemeral pods are deleted (default: 300)")
	agentCmd.Flags().String(AgentEphemeralImageFlag, "", "Docker image for ephemeral pods (default: same as current pod)")

	// VCS credentials (required for cloning repos)
	agentCmd.Flags().String(AgentGithubUserFlag, "", "GitHub username for authentication")
	agentCmd.Flags().String(AgentGithubTokenFlag, "", "GitHub token for authentication")
	agentCmd.Flags().Int64(AgentGithubAppIDFlag, 0, "GitHub App ID (use instead of token)")
	agentCmd.Flags().String(AgentGithubAppKeyFileFlag, "", "Path to GitHub App private key file")
	agentCmd.Flags().String(AgentGithubHostnameFlag, "github.com", "GitHub hostname (for GitHub Enterprise)")
	agentCmd.Flags().String(AgentGitlabUserFlag, "", "GitLab username")
	agentCmd.Flags().String(AgentGitlabTokenFlag, "", "GitLab token")
	agentCmd.Flags().String(AgentGitlabHostnameFlag, "gitlab.com", "GitLab hostname")

	// Bind to viper for env var support
	viper.BindPFlag(AgentIDFlag, agentCmd.Flags().Lookup(AgentIDFlag))
	viper.BindPFlag(AgentNameFlag, agentCmd.Flags().Lookup(AgentNameFlag))
	viper.BindPFlag(AgentMasterAddressFlag, agentCmd.Flags().Lookup(AgentMasterAddressFlag))
	viper.BindPFlag(AgentTokenFlag, agentCmd.Flags().Lookup(AgentTokenFlag))
	viper.BindPFlag("agent-token-file", agentCmd.Flags().Lookup("agent-token-file"))
	viper.BindPFlag(AgentClusterNameFlag, agentCmd.Flags().Lookup(AgentClusterNameFlag))
	viper.BindPFlag(AgentNamespaceFlag, agentCmd.Flags().Lookup(AgentNamespaceFlag))
	viper.BindPFlag(AgentCapacityFlag, agentCmd.Flags().Lookup(AgentCapacityFlag))
	viper.BindPFlag(AgentLabelsFlag, agentCmd.Flags().Lookup(AgentLabelsFlag))
	viper.BindPFlag(AgentHeartbeatFlag, agentCmd.Flags().Lookup(AgentHeartbeatFlag))
	viper.BindPFlag(AgentTerraformBinaryFlag, agentCmd.Flags().Lookup(AgentTerraformBinaryFlag))
	viper.BindPFlag(AgentGitBinaryFlag, agentCmd.Flags().Lookup(AgentGitBinaryFlag))
	viper.BindPFlag(AgentWorkDirFlag, agentCmd.Flags().Lookup(AgentWorkDirFlag))
	viper.BindPFlag(AgentLogLevelFlag, agentCmd.Flags().Lookup(AgentLogLevelFlag))
	viper.BindPFlag(AgentModeFlag, agentCmd.Flags().Lookup(AgentModeFlag))
	viper.BindPFlag(AgentEphemeralCPUFlag, agentCmd.Flags().Lookup(AgentEphemeralCPUFlag))
	viper.BindPFlag(AgentEphemeralMemoryFlag, agentCmd.Flags().Lookup(AgentEphemeralMemoryFlag))
	viper.BindPFlag(AgentEphemeralTTLFlag, agentCmd.Flags().Lookup(AgentEphemeralTTLFlag))
	viper.BindPFlag(AgentEphemeralImageFlag, agentCmd.Flags().Lookup(AgentEphemeralImageFlag))
	viper.BindPFlag(AgentGithubUserFlag, agentCmd.Flags().Lookup(AgentGithubUserFlag))
	viper.BindPFlag(AgentGithubTokenFlag, agentCmd.Flags().Lookup(AgentGithubTokenFlag))
	viper.BindPFlag(AgentGithubAppIDFlag, agentCmd.Flags().Lookup(AgentGithubAppIDFlag))
	viper.BindPFlag(AgentGithubAppKeyFileFlag, agentCmd.Flags().Lookup(AgentGithubAppKeyFileFlag))
	viper.BindPFlag(AgentGithubHostnameFlag, agentCmd.Flags().Lookup(AgentGithubHostnameFlag))
	viper.BindPFlag(AgentGitlabUserFlag, agentCmd.Flags().Lookup(AgentGitlabUserFlag))
	viper.BindPFlag(AgentGitlabTokenFlag, agentCmd.Flags().Lookup(AgentGitlabTokenFlag))
	viper.BindPFlag(AgentGitlabHostnameFlag, agentCmd.Flags().Lookup(AgentGitlabHostnameFlag))

	// Configure viper for environment variables
	viper.SetEnvPrefix("ATLANTIS")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// Add to root command
	RootCmd.AddCommand(agentCmd)
}

func runAgent(cmd *cobra.Command) error {
	// Get configuration values from flags first (cobra), then fall back to viper (env vars)
	masterAddress, _ := cmd.Flags().GetString(AgentMasterAddressFlag)
	if masterAddress == "" {
		masterAddress = viper.GetString(AgentMasterAddressFlag)
	}

	token, _ := cmd.Flags().GetString(AgentTokenFlag)
	if token == "" {
		token = viper.GetString(AgentTokenFlag)
	}

	tokenFile, _ := cmd.Flags().GetString("agent-token-file")
	if tokenFile == "" {
		tokenFile = viper.GetString("agent-token-file")
	}

	// Get other configuration values
	agentID := viper.GetString(AgentIDFlag)
	agentName := viper.GetString(AgentNameFlag)
	clusterName := viper.GetString(AgentClusterNameFlag)
	namespace := viper.GetString(AgentNamespaceFlag)
	capacity := viper.GetInt(AgentCapacityFlag)
	heartbeatInterval := viper.GetString(AgentHeartbeatFlag)
	labelsStr := viper.GetString(AgentLabelsFlag)
	terraformBinary := viper.GetString(AgentTerraformBinaryFlag)
	workDir := viper.GetString(AgentWorkDirFlag)
	logLevel := viper.GetString(AgentLogLevelFlag)

	// Agent mode and ephemeral configuration
	agentMode := viper.GetString(AgentModeFlag)
	ephemeralCPU := viper.GetString(AgentEphemeralCPUFlag)
	ephemeralMemory := viper.GetString(AgentEphemeralMemoryFlag)
	ephemeralTTL := viper.GetInt32(AgentEphemeralTTLFlag)
	ephemeralImage := viper.GetString(AgentEphemeralImageFlag)

	// Validate required fields
	if masterAddress == "" {
		return fmt.Errorf("--%s is required", AgentMasterAddressFlag)
	}

	// Read token from file if provided
	if tokenFile != "" && token == "" {
		tokenBytes, err := os.ReadFile(tokenFile)
		if err != nil {
			return fmt.Errorf("failed to read token file: %w", err)
		}
		token = strings.TrimSpace(string(tokenBytes))
	}

	if token == "" {
		return fmt.Errorf("--%s or --agent-token-file is required", AgentTokenFlag)
	}

	// Generate agent ID if not provided (use hostname or random)
	if agentID == "" {
		hostname := os.Getenv("HOSTNAME")
		if hostname != "" {
			agentID = hostname
		} else {
			agentID = fmt.Sprintf("agent-%d", time.Now().Unix())
		}
	}

	// Generate agent name if not provided
	if agentName == "" {
		agentName = agentID
	}

	// Parse labels
	labels := make(map[string]string)
	if labelsStr != "" {
		pairs := strings.Split(labelsStr, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(kv) == 2 {
				labels[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
	}

	// Setup logger
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
		return fmt.Errorf("failed to create logger: %w", err)
	}

	// Log startup information
	logger.Info("Starting Atlantis Agent Controller")
	logger.Info("Agent ID: %s", agentID)
	logger.Info("Agent Name: %s", agentName)
	logger.Info("Master Address: %s", masterAddress)
	logger.Info("Agent Mode: %s", agentMode)
	logger.Info("Cluster: %s", clusterName)
	logger.Info("Namespace: %s", namespace)
	logger.Info("Capacity: %d", capacity)
	logger.Info("Labels: %v", labels)
	logger.Info("Heartbeat Interval: %s", heartbeatInterval)

	if agentMode == "ephemeral" {
		logger.Info("Ephemeral Mode: CPU=%s Memory=%s TTL=%ds Image=%s",
			ephemeralCPU, ephemeralMemory, ephemeralTTL, ephemeralImage)
	}

	// Get VCS credentials
	githubUser := viper.GetString(AgentGithubUserFlag)
	githubToken := viper.GetString(AgentGithubTokenFlag)
	githubAppID := viper.GetInt64(AgentGithubAppIDFlag)
	githubAppKeyFile := viper.GetString(AgentGithubAppKeyFileFlag)
	githubHostname := viper.GetString(AgentGithubHostnameFlag)
	gitlabUser := viper.GetString(AgentGitlabUserFlag)
	gitlabToken := viper.GetString(AgentGitlabTokenFlag)
	gitlabHostname := viper.GetString(AgentGitlabHostnameFlag)

	// Validate that at least one VCS is configured
	hasGithub := githubToken != "" || githubAppID != 0
	hasGitlab := gitlabToken != ""
	if !hasGithub && !hasGitlab {
		logger.Err("No VCS credentials configured. Must provide GitHub or GitLab credentials.")
		return fmt.Errorf("VCS credentials required: use --gh-token/--gh-app-id or --gitlab-token")
	}

	logger.Info("Initializing Atlantis components...")

	// Initialize Atlantis components (VCS clients, terraform, etc.)
	atlantisConfig := agent.AgentInitConfig{
		DataDir:          workDir,
		TerraformBinPath: terraformBinary,
		TerraformVersion: "", // Use latest/default
		GithubUser:       githubUser,
		GithubToken:      githubToken,
		GithubAppID:      githubAppID,
		GithubAppKeyFile: githubAppKeyFile,
		GithubHostname:   githubHostname,
		GitlabUser:       gitlabUser,
		GitlabToken:      gitlabToken,
		GitlabHostname:   gitlabHostname,
		Logger:           logger,
	}

	components, err := agent.InitializeAtlantisComponents(atlantisConfig)
	if err != nil {
		logger.Err("Failed to initialize Atlantis components: %v", err)
		return fmt.Errorf("initializing components: %w", err)
	}

	logger.Info("Atlantis components initialized successfully")

	// Create agent executor using standard Atlantis infrastructure
	executor := agent.NewAgentExecutor(
		logger,
		components.ProjectCommandRunner,
		components.VCSClient,
		components.WorkingDir,
	)

	logger.Info("Agent executor created")

	// Create ephemeral pod manager if in ephemeral mode
	var ephemeralPodManager *controller.EphemeralPodManager
	if agentMode == "ephemeral" {
		logger.Info("Initializing ephemeral pod manager...")
		ephemeralConfig := controller.EphemeralPodConfig{
			CPU:       ephemeralCPU,
			Memory:    ephemeralMemory,
			TTL:       ephemeralTTL,
			Image:     ephemeralImage,
			Namespace: namespace,
		}

		ephemeralPodManager, err = controller.NewEphemeralPodManager(ephemeralConfig, logger)
		if err != nil {
			logger.Err("Failed to create ephemeral pod manager: %v", err)
			return fmt.Errorf("creating ephemeral pod manager: %w", err)
		}
		logger.Info("Ephemeral pod manager initialized")
	}

	// Parse heartbeat interval
	heartbeatDuration, err := time.ParseDuration(heartbeatInterval)
	if err != nil {
		logger.Err("Invalid heartbeat interval: %v", err)
		heartbeatDuration = 30 * time.Second
	}

	// Determine if using TLS based on URL scheme or port
	useTLS := false
	cleanAddress := masterAddress
	if strings.HasPrefix(masterAddress, "grpcs://") || strings.HasPrefix(masterAddress, "https://") {
		useTLS = true
		// Strip scheme prefix
		if strings.HasPrefix(masterAddress, "grpcs://") {
			cleanAddress = strings.TrimPrefix(masterAddress, "grpcs://")
		} else {
			cleanAddress = strings.TrimPrefix(masterAddress, "https://")
		}
		logger.Info("detected secure scheme, enabling TLS for gRPC connection to %s", cleanAddress)
	} else if strings.HasPrefix(masterAddress, "grpc://") || strings.HasPrefix(masterAddress, "http://") {
		useTLS = false
		// Strip scheme prefix
		if strings.HasPrefix(masterAddress, "grpc://") {
			cleanAddress = strings.TrimPrefix(masterAddress, "grpc://")
		} else {
			cleanAddress = strings.TrimPrefix(masterAddress, "http://")
		}
		logger.Info("detected insecure scheme, using plaintext gRPC connection to %s", cleanAddress)
	} else if strings.HasSuffix(masterAddress, ":443") {
		// Fallback: assume TLS if port 443 is used without explicit scheme
		useTLS = true
		logger.Info("detected port 443 without scheme, enabling TLS for gRPC connection to %s", cleanAddress)
	} else {
		logger.Info("no TLS indicators found, using plaintext gRPC connection to %s", cleanAddress)
	}

	// Create agent configuration
	config := controller.Config{
		ControllerID:      agentID,
		Token:             token,
		ClusterName:       clusterName,
		Namespace:         namespace,
		MasterAddress:     cleanAddress,
		UseTLS:            useTLS,
		MaxConcurrentJobs: capacity,
		Labels:            labels,
		Version:           "dev", // TODO: Get from build
		HeartbeatInterval: heartbeatDuration,
		WorkDir:           workDir,
		TerraformBinPath:  terraformBinary,
	}

	// Create and start agent controller
	agentController, err := controller.NewAgentController(config, logger, executor, ephemeralPodManager)
	if err != nil {
		logger.Err("Failed to create agent controller: %v", err)
		return err
	}

	logger.Info("Connecting to master at %s...", masterAddress)
	ctx := context.Background()
	if err := agentController.Start(ctx); err != nil {
		logger.Err("Failed to start agent controller: %v", err)
		return err
	}

	logger.Info("Agent controller started successfully")
	logger.Info("Waiting for job assignments...")

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("Received signal %v, shutting down gracefully...", sig)

	// Stop agent controller
	agentController.Stop()
	logger.Info("Agent controller stopped")

	return nil
}
