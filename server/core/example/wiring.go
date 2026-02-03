// Package example provides reference implementations for wiring together
// the distributed master-agent architecture components.
//
// NOTE: This is documentation/reference code showing the initialization
// sequence and configuration patterns. It is not meant to be compiled
// directly, as it demonstrates concepts that depend on your specific
// deployment environment and may reference interfaces not yet fully
// implemented in the codebase.
//
// Use this as a guide for:
// - Understanding component initialization order
// - Seeing configuration patterns
// - Learning graceful shutdown patterns
// - Reference for your own implementations
package example

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/runatlantis/atlantis/server/logging"
)

// This example demonstrates how to wire together all components of the
// distributed master-agent architecture. This is intended as a reference
// implementation showing the initialization and startup sequence.

func ExampleMasterServer() {
	// 1. Initialize logger
	logger := logging.NewSimpleLogger("atlantis-master", log.New(os.Stdout, "", log.LstdFlags), true, logging.Info)
	logger.Info("Starting Atlantis Master Server")

	// 2. Connect to database
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Err("DATABASE_URL environment variable not set")
		os.Exit(1)
	}

	dbConn, err := sql.Open("postgres", dbURL)
	if err != nil {
		logger.Err("Failed to connect to database: %v", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	// Verify connection
	if err := dbConn.Ping(); err != nil {
		logger.Err("Database ping failed: %v", err)
		os.Exit(1)
	}
	logger.Info("Database connection established")

	// 3. Run migrations
	if err := runMigrations(dbConn); err != nil {
		logger.Err("Failed to run migrations: %v", err)
		os.Exit(1)
	}
	logger.Info("Database migrations completed")

	// 4. Create data stores
	jobStore := db.NewPostgresJobStore(dbConn)
	agentStore := db.NewPostgresAgentStore(dbConn)
	logger.Info("Data stores initialized")

	// 5. Configure and create enhanced scheduler
	schedulerConfig := scheduler.DefaultEnhancedSchedulerConfig()
	// Customize configuration as needed
	schedulerConfig.MaxQueueSize = 1000
	schedulerConfig.AssignmentInterval = 5 * time.Second
	schedulerConfig.RetryPolicy.MaxAttempts = 3
	schedulerConfig.RetryPolicy.InitialDelay = 30 * time.Second
	schedulerConfig.CircuitBreaker.ErrorThreshold = 5
	schedulerConfig.CircuitBreaker.ResetTimeout = 60 * time.Second

	enhancedScheduler := scheduler.NewEnhancedScheduler(
		jobStore,
		agentStore,
		logger,
		schedulerConfig,
	)
	logger.Info("Enhanced scheduler created with priority queue and retry policies")

	// 6. Create agent manager
	// Note: In a real implementation, you would also create a gRPC server here
	// and pass it to the agent manager. This is omitted for simplicity since
	// the gRPC server implementation depends on your specific setup.

	agentManagerConfig := agent.DefaultAgentManagerConfig()
	// agentManager := agent.NewAgentManager(
	// 	enhancedScheduler,
	// 	grpcServer,  // Your gRPC server instance
	// 	jobStore,
	// 	agentStore,
	// 	logger,
	// 	agentManagerConfig,
	// )

	// 7. Start agent manager
	// if err := agentManager.Start(); err != nil {
	// 	logger.Err("Failed to start agent manager: %v", err)
	// 	os.Exit(1)
	// }
	// logger.Info("Agent manager started")

	// 8. Setup graceful shutdown
	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	// sig := <-sigChan
	// logger.Info("Received signal %v, shutting down gracefully", sig)

	// 9. Graceful shutdown sequence
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop agent manager (stops assignment goroutines)
	// logger.Info("Stopping agent manager...")
	// agentManager.Stop()

	// Close database connection
	logger.Info("Closing database connection...")
	dbConn.Close()

	logger.Info("Shutdown complete")

	// Suppress unused variable warnings in example
	_ = ctx
	_ = agentManagerConfig
}

// ExampleSubmitJob demonstrates how to submit a job to the scheduler
func ExampleSubmitJob(schedulerInstance scheduler.Scheduler, logger logging.SimpleLogging) error {
	ctx := context.Background()

	// Create a job
	job := &db.Job{
		RepoFullName:   "myorg/myrepo",
		RepoCloneURL:   "https://github.com/myorg/myrepo.git",
		PullNum:        123,
		PullBranch:     "feature/new-infrastructure",
		PullBaseBranch: "main",
		PullCommitSHA:  "abc123def456",
		Command:        "plan",
		Workspace:      "production",
		ProjectDir:     "terraform/prod",
		Labels: map[string]string{
			"environment": "production",
			"team":        "platform",
			"priority":    "high",
		},
		TimeoutSeconds: 600,
		TriggeredBy:    "user@example.com",
		Priority:       80, // High priority
		AttemptCount:   0,
	}

	// Submit to scheduler
	if err := schedulerInstance.SubmitJob(ctx, job); err != nil {
		logger.Err("Failed to submit job: %v", err)
		return err
	}

	logger.Info("Job %s submitted successfully for %s PR #%d",
		job.ID, job.RepoFullName, job.PullNum)

	return nil
}

// ExampleAgentRegistration demonstrates how an agent registers itself
func ExampleAgentRegistration(agentStore db.AgentStore, logger logging.SimpleLogging) error {
	ctx := context.Background()

	// Create agent registration
	agent := &db.AgentController{
		ID:          "agent-us-east-1a-01", // Unique agent identifier
		Name:        "US East 1A Agent 01",
		ClusterName: "production-us-east",
		Namespace:   "atlantis-agents",
		Labels: map[string]string{
			"region":      "us-east-1",
			"zone":        "us-east-1a",
			"environment": "production",
			"terraform":   "1.11.1",
		},
		Capacity:    10, // Max concurrent jobs
		CurrentJobs: 0,
		Status:      string(db.AgentStatusActive),
	}

	// Register with the master
	if err := agentStore.Register(ctx, agent); err != nil {
		logger.Err("Failed to register agent: %v", err)
		return err
	}

	logger.Info("Agent %s registered successfully", agent.ID)

	// Send periodic heartbeats (every 30 seconds)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if err := agentStore.Heartbeat(ctx, agent.ID); err != nil {
				logger.Err("Heartbeat failed for agent %s: %v", agent.ID, err)
			} else {
				logger.Debug("Heartbeat sent for agent %s", agent.ID)
			}
		}
	}()

	return nil
}

// ExampleQueryJobStatus demonstrates how to query job status
func ExampleQueryJobStatus(jobStore db.JobStore, jobID string, logger logging.SimpleLogging) {
	ctx := context.Background()

	job, err := jobStore.Get(ctx, jobID)
	if err != nil {
		logger.Err("Failed to get job: %v", err)
		return
	}

	logger.Info("Job Status Report:")
	logger.Info("  ID: %s", job.ID)
	logger.Info("  Repo: %s", job.RepoFullName)
	logger.Info("  PR: #%d", job.PullNum)
	logger.Info("  Command: %s", job.Command)
	logger.Info("  Status: %s", job.Status)
	logger.Info("  Agent: %s", job.AgentID)
	logger.Info("  Priority: %d", job.Priority)
	logger.Info("  Attempts: %d", job.AttemptCount)

	if job.SubmittedAt != nil {
		logger.Info("  Submitted: %s", job.SubmittedAt.Format(time.RFC3339))
	}
	if job.StartedAt != nil {
		logger.Info("  Started: %s", job.StartedAt.Format(time.RFC3339))
	}
	if job.CompletedAt != nil {
		logger.Info("  Completed: %s", job.CompletedAt.Format(time.RFC3339))
		duration := job.CompletedAt.Sub(*job.StartedAt)
		logger.Info("  Duration: %s", duration)
	}

	if job.ErrorMessage != nil {
		logger.Warn("  Error: %s", *job.ErrorMessage)
	}
}

// runMigrations applies database migrations
func runMigrations(dbConn *sql.DB) error {
	// In production, use a migration library like golang-migrate
	// This is a placeholder showing the concept
	migrations := []string{
		// Migration files would be read and executed here
		// "000001_initial_schema.up.sql",
		// "000002_add_agents_table.up.sql",
		// ...
	}

	for _, migration := range migrations {
		// Execute migration SQL
		fmt.Printf("Would apply migration: %s\n", migration)
	}

	return nil
}

// ExampleConfiguration shows the complete configuration structure
type Configuration struct {
	// Database
	DatabaseURL string

	// Server
	GRPCPort int
	HTTPPort int

	// Scheduler
	Scheduler struct {
		MaxQueueSize       int
		AssignmentInterval time.Duration
		StaleJobTimeout    time.Duration

		RetryPolicy struct {
			MaxAttempts       int
			InitialDelay      time.Duration
			MaxDelay          time.Duration
			BackoffMultiplier float64
		}

		CircuitBreaker struct {
			ErrorThreshold int
			ResetTimeout   time.Duration
		}
	}

	// Agent Manager
	AgentManager struct {
		AssignmentQueueSize int
		ResultQueueSize     int
		HealthCheckInterval time.Duration
		StreamStaleTimeout  time.Duration
	}

	// Logging
	LogLevel string
}

// DefaultConfiguration returns sensible defaults
func DefaultConfiguration() Configuration {
	config := Configuration{
		DatabaseURL: "postgres://atlantis:password@localhost:5432/atlantis?sslmode=disable",
		GRPCPort:    50051,
		HTTPPort:    8080,
		LogLevel:    "info",
	}

	config.Scheduler.MaxQueueSize = 1000
	config.Scheduler.AssignmentInterval = 5 * time.Second
	config.Scheduler.StaleJobTimeout = 10 * time.Minute

	config.Scheduler.RetryPolicy.MaxAttempts = 3
	config.Scheduler.RetryPolicy.InitialDelay = 30 * time.Second
	config.Scheduler.RetryPolicy.MaxDelay = 5 * time.Minute
	config.Scheduler.RetryPolicy.BackoffMultiplier = 2.0

	config.Scheduler.CircuitBreaker.ErrorThreshold = 5
	config.Scheduler.CircuitBreaker.ResetTimeout = 60 * time.Second

	config.AgentManager.AssignmentQueueSize = 100
	config.AgentManager.ResultQueueSize = 100
	config.AgentManager.HealthCheckInterval = 30 * time.Second
	config.AgentManager.StreamStaleTimeout = 5 * time.Minute

	return config
}
