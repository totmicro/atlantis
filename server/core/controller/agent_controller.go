package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// AgentController manages the connection to the master and executes jobs
type AgentController struct {
	// Configuration
	config Config
	logger logging.SimpleLogging

	// gRPC connection
	conn   *grpc.ClientConn
	client proto.AgentServiceClient
	stream proto.AgentService_StreamJobsClient

	// Job execution - uses standard Atlantis infrastructure
	executor            *agent.AgentExecutor
	ephemeralPodManager *EphemeralPodManager // For ephemeral mode
	currentJobs         map[string]*JobExecution
	jobsMu              sync.RWMutex

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Status
	connected bool
	mu        sync.RWMutex
}

// Config holds agent controller configuration
type Config struct {
	// Identity
	ControllerID string
	Token        string
	ClusterName  string
	Namespace    string

	// Master connection
	MasterAddress string // e.g., "atlantis-master:50051"
	UseTLS        bool

	// Capacity
	MaxConcurrentJobs int

	// Labels for job routing
	Labels map[string]string

	// Atlantis version
	Version string

	// Heartbeat interval
	HeartbeatInterval time.Duration

	// Job execution
	WorkDir          string // Working directory for job execution
	TerraformBinPath string // Path to terraform binary
}

// JobExecution tracks a running job
type JobExecution struct {
	Job       *proto.JobAssignment
	StartTime time.Time
	Cancel    context.CancelFunc
}

// NewAgentController creates a new agent controller
// ephemeralPodManager can be nil for static mode
func NewAgentController(config Config, logger logging.SimpleLogging, executor *agent.AgentExecutor, ephemeralPodManager *EphemeralPodManager) (*AgentController, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &AgentController{
		config:              config,
		logger:              logger,
		executor:            executor,
		ephemeralPodManager: ephemeralPodManager,
		currentJobs:         make(map[string]*JobExecution),
	}, nil
}

// Start begins the agent controller lifecycle
func (ac *AgentController) Start(ctx context.Context) error {
	ac.mu.Lock()
	if ac.connected {
		ac.mu.Unlock()
		return fmt.Errorf("agent already started")
	}
	ac.ctx, ac.cancel = context.WithCancel(ctx)
	ac.mu.Unlock()

	// Connect to master
	if err := ac.connect(); err != nil {
		return fmt.Errorf("connecting to master: %w", err)
	}

	// Start background goroutines
	ac.wg.Add(3)
	go ac.receiveJobs()
	go ac.sendHeartbeats()
	go ac.monitorJobs()

	ac.logger.Info("agent controller started (ID: %s, master: %s)", ac.config.ControllerID, ac.config.MasterAddress)
	return nil
}

// Stop gracefully shuts down the agent controller
func (ac *AgentController) Stop() error {
	ac.mu.Lock()
	if !ac.connected {
		ac.mu.Unlock()
		return nil
	}
	ac.mu.Unlock()

	ac.logger.Info("stopping agent controller")

	// Cancel context to stop goroutines
	if ac.cancel != nil {
		ac.cancel()
	}

	// Wait for goroutines to finish
	ac.wg.Wait()

	// Cancel all running jobs
	ac.jobsMu.Lock()
	for jobID, execution := range ac.currentJobs {
		ac.logger.Info("canceling job %s", jobID)
		if execution.Cancel != nil {
			execution.Cancel()
		}
	}
	ac.jobsMu.Unlock()

	// Close gRPC connection
	if ac.conn != nil {
		if err := ac.conn.Close(); err != nil {
			ac.logger.Err("error closing gRPC connection: %s", err)
		}
	}

	ac.mu.Lock()
	ac.connected = false
	ac.mu.Unlock()

	ac.logger.Info("agent controller stopped")
	return nil
}

// connect establishes gRPC connection to master
func (ac *AgentController) connect() error {
	var opts []grpc.DialOption

	if ac.config.UseTLS {
		// Use system TLS certificates for secure connection
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false, // Validate server certificate
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
		ac.logger.Info("using TLS for gRPC connection")
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		ac.logger.Info("using insecure gRPC connection")
	}

	// Add metadata interceptor for authentication
	opts = append(opts, grpc.WithUnaryInterceptor(ac.unaryAuthInterceptor))
	opts = append(opts, grpc.WithStreamInterceptor(ac.streamAuthInterceptor))

	// Add keepalive to detect dead connections (must be >= server MinTime of 20s)
	opts = append(opts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Send ping every 30s (server MinTime is 20s)
			Timeout:             10 * time.Second, // Wait 10s for ping ack
			PermitWithoutStream: true,
		}),
	)

	ac.logger.Info("connecting to master at %s", ac.config.MasterAddress)

	conn, err := grpc.Dial(ac.config.MasterAddress, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	ac.conn = conn
	ac.client = proto.NewAgentServiceClient(conn)

	// Open bidirectional stream
	stream, err := ac.client.StreamJobs(ac.ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open stream: %w", err)
	}

	ac.stream = stream

	// Send registration message
	if err := ac.register(); err != nil {
		conn.Close()
		return fmt.Errorf("failed to register: %w", err)
	}

	ac.mu.Lock()
	ac.connected = true
	ac.mu.Unlock()

	ac.logger.Info("connected and registered with master")
	return nil
}

// register sends registration message to master
func (ac *AgentController) register() error {
	msg := &proto.AgentMessage{
		Message: &proto.AgentMessage_Registration{
			Registration: &proto.Registration{
				ControllerId: ac.config.ControllerID,
				Token:        ac.config.Token,
				ClusterName:  ac.config.ClusterName,
				Namespace:    ac.config.Namespace,
				Labels:       ac.config.Labels,
				Capacity:     int32(ac.config.MaxConcurrentJobs),
				Version:      ac.config.Version,
			},
		},
	}

	if err := ac.stream.Send(msg); err != nil {
		return fmt.Errorf("sending registration: %w", err)
	}

	ac.logger.Info("sent registration message")
	return nil
}

// receiveJobs listens for job assignments from master
func (ac *AgentController) receiveJobs() {
	defer ac.wg.Done()

	ac.logger.Info("started receiving jobs")

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ac.ctx.Done():
			ac.logger.Info("stopping job receiver")
			return
		default:
		}

		assignment, err := ac.stream.Recv()
		if err != nil {
			// Check if this is a graceful shutdown
			if ac.ctx.Err() != nil {
				ac.logger.Info("job receiver context cancelled, stopping")
				return
			}

			// Network error, connection lost, or master restarted
			ac.logger.Warn("error receiving job (will reconnect): %s", err)

			// Try to reconnect
			ac.logger.Info("attempting to reconnect to master (backoff: %v)", backoff)
			time.Sleep(backoff)

			// Exponential backoff
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			// Attempt reconnection
			if err := ac.reconnect(); err != nil {
				ac.logger.Err("failed to reconnect: %s", err)
				continue // Try again on next iteration
			}

			// Reset backoff on successful reconnection
			backoff = time.Second
			ac.logger.Info("successfully reconnected to master")

			// Query for any jobs that were assigned while disconnected
			go ac.syncAssignedJobs()

			continue
		}

		// Reset backoff on successful message receive
		backoff = time.Second

		ac.logger.Info("received job assignment: %s", assignment.JobId)
		ac.handleJobAssignment(assignment)
	}
}

// reconnect attempts to re-establish connection to master
func (ac *AgentController) reconnect() error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// Close old connection if it exists
	if ac.conn != nil {
		ac.conn.Close()
		ac.conn = nil
		ac.stream = nil
	}

	// Re-establish connection using existing connect logic
	var opts []grpc.DialOption

	if ac.config.UseTLS {
		// Use system TLS certificates for secure connection
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: false, // Validate server certificate
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add metadata interceptor for authentication
	opts = append(opts, grpc.WithUnaryInterceptor(ac.unaryAuthInterceptor))
	opts = append(opts, grpc.WithStreamInterceptor(ac.streamAuthInterceptor))

	// Add keepalive to detect dead connections (must be >= server MinTime of 20s)
	opts = append(opts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Send ping every 30s (server MinTime is 20s)
			Timeout:             10 * time.Second, // Wait 10s for ping ack
			PermitWithoutStream: true,
		}),
	)

	ac.logger.Info("reconnecting to master at %s", ac.config.MasterAddress)

	conn, err := grpc.Dial(ac.config.MasterAddress, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	ac.conn = conn
	ac.client = proto.NewAgentServiceClient(conn)

	// Open bidirectional stream
	stream, err := ac.client.StreamJobs(ac.ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open stream: %w", err)
	}

	ac.stream = stream

	// Send registration message
	msg := &proto.AgentMessage{
		Message: &proto.AgentMessage_Registration{
			Registration: &proto.Registration{
				ControllerId: ac.config.ControllerID,
				Token:        ac.config.Token,
				ClusterName:  ac.config.ClusterName,
				Namespace:    ac.config.Namespace,
				Labels:       ac.config.Labels,
				Capacity:     int32(ac.config.MaxConcurrentJobs),
				Version:      ac.config.Version,
			},
		},
	}

	if err := stream.Send(msg); err != nil {
		conn.Close()
		return fmt.Errorf("failed to send registration: %w", err)
	}

	ac.connected = true
	ac.logger.Info("reconnected and registered with master")
	return nil
}

// syncAssignedJobs queries the master for any jobs assigned to this agent
// This is called after reconnection to pick up jobs that were assigned while disconnected
func (ac *AgentController) syncAssignedJobs() {
	if ac.client == nil {
		ac.logger.Warn("cannot sync jobs: not connected to master")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &proto.SyncJobsRequest{
		ControllerId: ac.config.ControllerID,
	}

	resp, err := ac.client.SyncAssignedJobs(ctx, req)
	if err != nil {
		ac.logger.Warn("failed to sync assigned jobs: %v", err)
		return
	}

	if len(resp.Jobs) == 0 {
		ac.logger.Info("no pending jobs to sync after reconnection")
		return
	}

	ac.logger.Info("syncing %d assigned jobs after reconnection", len(resp.Jobs))
	for _, job := range resp.Jobs {
		ac.handleJobAssignment(job)
	}
}

// handleJobAssignment processes a new job assignment
func (ac *AgentController) handleJobAssignment(assignment *proto.JobAssignment) {
	// Check capacity
	ac.jobsMu.RLock()
	currentCount := len(ac.currentJobs)
	ac.jobsMu.RUnlock()

	if currentCount >= ac.config.MaxConcurrentJobs {
		ac.logger.Warn("at capacity, rejecting job %s", assignment.JobId)
		// TODO: Send rejection message
		return
	}

	// Create job execution context
	jobCtx, cancel := context.WithCancel(ac.ctx)

	execution := &JobExecution{
		Job:       assignment,
		StartTime: time.Now(),
		Cancel:    cancel,
	}

	ac.jobsMu.Lock()
	ac.currentJobs[assignment.JobId] = execution
	ac.jobsMu.Unlock()

	ac.logger.Info("starting job %s", assignment.JobId)

	// Execute job in background
	go ac.executeJob(jobCtx, execution)
}

// executeJob runs a job and sends results back to master
func (ac *AgentController) executeJob(ctx context.Context, execution *JobExecution) {
	job := execution.Job

	// Send status update: running
	ac.sendStatusUpdate(job.JobId, "running", "")

	// Check if we should use ephemeral pod execution
	if ac.ephemeralPodManager != nil {
		ac.logger.Info("ephemeral mode: spawning pod for job %s", job.JobId)

		// Convert to db.Job for the spawner
		dbJob := &db.Job{
			ID:                 job.JobId,
			RepoFullName:       job.RepoFullName,
			RepoCloneURL:       job.RepoCloneUrl,
			PullNum:            int(job.PullNum),
			Command:            job.Command,
			ProjectName:        job.ProjectName,
			ProjectDir:         job.ProjectDir,
			Workspace:          job.Workspace,
			ProjectContextJSON: job.ProjectContextJson,
			PlanData:           job.PlanData,
		}

		// Spawn ephemeral pod - it will execute and report back to master directly
		if err := ac.ephemeralPodManager.SpawnJobExecutor(ctx, dbJob); err != nil {
			ac.logger.Err("failed to spawn ephemeral pod for job %s: %v", job.JobId, err)

			// Send failure result
			resultMsg := &proto.AgentMessage{
				Message: &proto.AgentMessage_JobResult{
					JobResult: &proto.JobResult{
						JobId:        job.JobId,
						Status:       "failed",
						Output:       "",
						ErrorMessage: fmt.Sprintf("failed to spawn ephemeral pod: %v", err),
						AgentPodName: ac.getPodName(),
						StartedAt:    execution.StartTime.Unix(),
						CompletedAt:  time.Now().Unix(),
					},
				},
			}
			ac.stream.Send(resultMsg)
		} else {
			ac.logger.Info("ephemeral pod spawned for job %s", job.JobId)
			// Note: Ephemeral pod will report results directly to master
		}

		// Remove from current jobs
		ac.jobsMu.Lock()
		delete(ac.currentJobs, job.JobId)
		ac.jobsMu.Unlock()
		return
	}

	// Static mode: execute locally
	ac.logger.Info("static mode: executing job %s locally", job.JobId)

	// Convert proto.JobAssignment to db.Job for the executor
	dbJob := &db.Job{
		ID:                 job.JobId,
		RepoFullName:       job.RepoFullName,
		RepoCloneURL:       job.RepoCloneUrl,
		PullNum:            int(job.PullNum),
		PullBranch:         job.PullBranch,
		PullBaseBranch:     job.PullBaseBranch,
		PullCommitSHA:      job.PullCommitSha,
		Command:            job.Command,
		ProjectName:        job.ProjectName,
		ProjectDir:         job.ProjectDir,
		Workspace:          job.Workspace,
		ProjectContextJSON: job.ProjectContextJson, // Contains serialized command.ProjectContext
		PlanData:           job.PlanData,           // Plan file for apply commands
		EnvVars:            job.EnvVars,
	}

	// Execute using standard Atlantis infrastructure
	result := ac.executor.ExecuteJob(dbJob)

	// Determine status - use "completed" to match what grpc_server expects
	status := "completed"
	errorMsg := ""
	if result.Error != nil {
		status = "failed"
		errorMsg = result.Error.Error()
	} else if !result.PlanSuccess {
		status = "failed"
	}

	// Send result back to master
	resultMsg := &proto.AgentMessage{
		Message: &proto.AgentMessage_JobResult{
			JobResult: &proto.JobResult{
				JobId:        job.JobId,
				Status:       status,
				Output:       result.Output,
				PlanData:     result.PlanData, // Send plan file to master
				ExitCode:     0,
				ErrorMessage: errorMsg,
				AgentPodName: ac.getPodName(),
				StartedAt:    execution.StartTime.Unix(),
				CompletedAt:  time.Now().Unix(),
			},
		},
	}

	if err := ac.stream.Send(resultMsg); err != nil {
		ac.logger.Err("failed to send job result for %s: %s", job.JobId, err)
	} else {
		ac.logger.Info("sent job result for %s: status=%s", job.JobId, status)
	}

	// Remove from current jobs
	ac.jobsMu.Lock()
	delete(ac.currentJobs, job.JobId)
	ac.jobsMu.Unlock()
}

// sendHeartbeats periodically sends heartbeat messages
func (ac *AgentController) sendHeartbeats() {
	defer ac.wg.Done()

	ticker := time.NewTicker(ac.config.HeartbeatInterval)
	defer ticker.Stop()

	ac.logger.Info("started heartbeat sender (interval: %s)", ac.config.HeartbeatInterval)

	for {
		select {
		case <-ac.ctx.Done():
			ac.logger.Info("stopping heartbeat sender")
			return
		case <-ticker.C:
			ac.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a single heartbeat message
func (ac *AgentController) sendHeartbeat() {
	ac.jobsMu.RLock()
	currentJobs := len(ac.currentJobs)
	ac.jobsMu.RUnlock()

	availableCapacity := ac.config.MaxConcurrentJobs - currentJobs

	msg := &proto.AgentMessage{
		Message: &proto.AgentMessage_Heartbeat{
			Heartbeat: &proto.Heartbeat{
				ControllerId:      ac.config.ControllerID,
				CurrentJobs:       int32(currentJobs),
				AvailableCapacity: int32(availableCapacity),
				Timestamp:         time.Now().Unix(),
			},
		},
	}

	if err := ac.stream.Send(msg); err != nil {
		ac.logger.Err("failed to send heartbeat: %s", err)
	} else {
		ac.logger.Debug("sent heartbeat (current jobs: %d, available: %d)", currentJobs, availableCapacity)
	}
}

// sendStatusUpdate sends a job status update
func (ac *AgentController) sendStatusUpdate(jobID, status, message string) {
	update := &proto.JobStatusUpdate{
		JobId:        jobID,
		Status:       status,
		AgentPodName: ac.getPodName(),
		Message:      message,
		Timestamp:    time.Now().Unix(),
	}

	if _, err := ac.client.ReportJobStatus(ac.ctx, update); err != nil {
		ac.logger.Err("failed to send status update for job %s: %s", jobID, err)
	}
}

// monitorJobs monitors running jobs for timeouts
func (ac *AgentController) monitorJobs() {
	defer ac.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ac.ctx.Done():
			return
		case <-ticker.C:
			ac.checkJobTimeouts()
		}
	}
}

// checkJobTimeouts checks for and cancels timed-out jobs
func (ac *AgentController) checkJobTimeouts() {
	ac.jobsMu.RLock()
	defer ac.jobsMu.RUnlock()

	for jobID, execution := range ac.currentJobs {
		if execution.Job.TimeoutSeconds > 0 {
			elapsed := time.Since(execution.StartTime)
			timeout := time.Duration(execution.Job.TimeoutSeconds) * time.Second

			if elapsed > timeout {
				ac.logger.Warn("job %s timed out after %s", jobID, elapsed)
				if execution.Cancel != nil {
					execution.Cancel()
				}
			}
		}
	}
}

// getPodName returns the pod name (for Kubernetes deployments)
func (ac *AgentController) getPodName() string {
	// TODO: Get from environment variable or config
	return ac.config.ControllerID
}

// unaryAuthInterceptor adds authentication metadata to unary RPCs
func (ac *AgentController) unaryAuthInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx = ac.addAuthMetadata(ctx)
	return invoker(ctx, method, req, reply, cc, opts...)
}

// streamAuthInterceptor adds authentication metadata to streaming RPCs
func (ac *AgentController) streamAuthInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	ctx = ac.addAuthMetadata(ctx)
	return streamer(ctx, desc, cc, method, opts...)
}

// addAuthMetadata adds authentication headers to context
func (ac *AgentController) addAuthMetadata(ctx context.Context) context.Context {
	md := metadata.New(map[string]string{
		"agent-id": ac.config.ControllerID,
		"token":    ac.config.Token,
	})
	return metadata.NewOutgoingContext(ctx, md)
}

// Validate validates the agent controller configuration
func (c *Config) Validate() error {
	if c.ControllerID == "" {
		return fmt.Errorf("controller ID is required")
	}
	if c.Token == "" {
		return fmt.Errorf("token is required")
	}
	if c.MasterAddress == "" {
		return fmt.Errorf("master address is required")
	}
	if c.MaxConcurrentJobs <= 0 {
		return fmt.Errorf("max concurrent jobs must be positive")
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
	if c.WorkDir == "" {
		c.WorkDir = "/tmp/atlantis-agent"
	}
	return nil
}
