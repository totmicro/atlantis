package agent

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/runatlantis/atlantis/server/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// VCSCredentialsGetter generates VCS credentials for repository cloning (e.g., GitHub Apps)
type VCSCredentialsGetter interface {
	GetCredentialsForRepo(repo string) (username, token string, err error)
}

// JobResultHandler handles completed job results (posting to PRs, etc.)
type JobResultHandler interface {
	HandleJobResult(ctx context.Context, job *db.Job, result *proto.JobResult) error
}

// GRPCServer implements the AgentService gRPC server
type GRPCServer struct {
	proto.UnimplementedAgentServiceServer

	registry      *Registry
	scheduler     scheduler.Scheduler
	jobStore      db.JobStore
	resultHandler JobResultHandler // Handles posting results to PRs
	logger        logging.SimpleLogging
	sharedToken   string // Shared secret for initial agent authentication

	// VCS credentials for repository cloning
	vcsUser       string
	vcsToken      string
	vcsCredGetter VCSCredentialsGetter // For dynamic credentials (GitHub Apps)

	// Connection tracking
	connections   map[string]*AgentConnection
	connectionsMu sync.RWMutex

	// Job assignment channels per agent
	jobChannels   map[string]chan *proto.JobAssignment
	jobChannelsMu sync.RWMutex
}

// AgentConnection tracks an active agent connection
type AgentConnection struct {
	ControllerID  string
	Stream        proto.AgentService_StreamJobsServer
	Connected     time.Time
	LastHeartbeat time.Time
	mu            sync.Mutex
}

// NewGRPCServer creates a new gRPC server for agent communication
func NewGRPCServer(
	registry *Registry,
	scheduler scheduler.Scheduler,
	jobStore db.JobStore,
	sharedToken string,
	logger logging.SimpleLogging,
) *GRPCServer {
	return &GRPCServer{
		registry:    registry,
		scheduler:   scheduler,
		jobStore:    jobStore,
		sharedToken: sharedToken,
		logger:      logger,
		connections: make(map[string]*AgentConnection),
		jobChannels: make(map[string]chan *proto.JobAssignment),
	}
}

// SetVCSCredentials sets static VCS credentials for repository cloning
func (s *GRPCServer) SetVCSCredentials(user, token string) {
	s.vcsUser = user
	s.vcsToken = token
}

// SetVCSCredentialsGetter sets dynamic VCS credentials getter (for GitHub Apps)
func (s *GRPCServer) SetVCSCredentialsGetter(getter VCSCredentialsGetter) {
	s.vcsCredGetter = getter
}

// SetResultHandler sets the handler for posting job results to PRs
func (s *GRPCServer) SetResultHandler(handler JobResultHandler) {
	s.resultHandler = handler
}

// StreamJobs handles the bidirectional stream for agent communication
func (s *GRPCServer) StreamJobs(stream proto.AgentService_StreamJobsServer) error {
	ctx := stream.Context()
	var controllerID string
	var conn *AgentConnection

	// Cleanup on disconnect
	defer func() {
		if controllerID != "" {
			s.handleDisconnect(controllerID)
		}
	}()

	// Start a goroutine to send jobs to this agent
	jobChan := make(chan *proto.JobAssignment, 10)
	errChan := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-jobChan:
				if err := stream.Send(job); err != nil {
					s.logger.Err("failed to send job to agent %s: %s", controllerID, err)
					errChan <- err
					return
				}
				s.logger.Debug("sent job %s to agent %s", job.JobId, controllerID)
			}
		}
	}()

	// Main loop: receive messages from agent
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("agent %s stream context done", controllerID)
			return ctx.Err()
		case err := <-errChan:
			return err
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			// Handle graceful disconnection vs actual errors
			if isGracefulDisconnect(err) {
				s.logger.Info("agent %s disconnected gracefully", controllerID)
				return nil
			}
			s.logger.Warn("agent %s stream receive error: %s", controllerID, err)
			return err
		}

		switch m := msg.Message.(type) {
		case *proto.AgentMessage_Registration:
			// Handle registration
			id, authErr := s.handleRegistration(ctx, m.Registration, stream)
			if authErr != nil {
				return authErr
			}
			controllerID = id

			// Track connection
			conn = &AgentConnection{
				ControllerID:  controllerID,
				Stream:        stream,
				Connected:     time.Now(),
				LastHeartbeat: time.Now(),
			}
			s.addConnection(controllerID, conn)

			// Register job channel
			s.jobChannelsMu.Lock()
			s.jobChannels[controllerID] = jobChan
			s.jobChannelsMu.Unlock()

			s.logger.Info("agent %s registered and connected", controllerID)

		case *proto.AgentMessage_Heartbeat:
			if controllerID == "" {
				return status.Error(codes.Unauthenticated, "agent must register before sending heartbeats")
			}

			if err := s.handleHeartbeat(ctx, controllerID, m.Heartbeat); err != nil {
				s.logger.Err("heartbeat error for agent %s: %s", controllerID, err)
			}

			// Update connection timestamp
			if conn != nil {
				conn.mu.Lock()
				conn.LastHeartbeat = time.Now()
				conn.mu.Unlock()
			}

		case *proto.AgentMessage_JobResult:
			if controllerID == "" {
				return status.Error(codes.Unauthenticated, "agent must register before sending results")
			}

			if err := s.handleJobResult(ctx, m.JobResult); err != nil {
				s.logger.Err("job result error for agent %s: %s", controllerID, err)
			}

		case *proto.AgentMessage_CapacityUpdate:
			if controllerID == "" {
				return status.Error(codes.Unauthenticated, "agent must register before updating capacity")
			}

			if err := s.handleCapacityUpdate(ctx, m.CapacityUpdate); err != nil {
				s.logger.Err("capacity update error for agent %s: %s", controllerID, err)
			}
		}
	}
}

// handleRegistration processes agent registration
func (s *GRPCServer) handleRegistration(ctx context.Context, reg *proto.Registration, stream proto.AgentService_StreamJobsServer) (string, error) {
	// Check if this is an ephemeral agent (just needs to report a result and exit)
	// Ephemeral agents have capacity=0 and/or version="ephemeral"
	if reg.Capacity == 0 || reg.Version == "ephemeral" {
		s.logger.Info("ephemeral agent %s registered (no database entry)", reg.ControllerId)
		return reg.ControllerId, nil
	}

	// Check if agent exists by name (ControllerId is the agent name)
	agent, err := s.registry.GetAgentByName(ctx, reg.ControllerId)

	// If agent doesn't exist (sql.ErrNoRows or "not found" error), auto-register with shared token
	agentNotFound := err != nil && (err == sql.ErrNoRows || err.Error() == "agent not found" || agent == nil)

	if agentNotFound {
		// Agent doesn't exist - validate shared token and auto-register
		if reg.Token != s.sharedToken {
			s.logger.Warn("invalid shared token for new agent %s", reg.ControllerId)
			return "", status.Error(codes.Unauthenticated, "invalid credentials")
		}

		// Auto-register the agent
		s.logger.Info("auto-registering new agent %s", reg.ControllerId)
		config := AgentConfig{
			Name:            reg.ControllerId, // Use controller ID as name
			ClusterName:     reg.ClusterName,
			Namespace:       reg.Namespace,
			Labels:          reg.Labels,
			Capacity:        int(reg.Capacity),
			AtlantisVersion: reg.Version,
			Token:           reg.Token,
		}

		agent, err = s.registry.Register(ctx, config)
		if err != nil {
			s.logger.Err("failed to register agent %s: %s", reg.ControllerId, err)
			return "", status.Error(codes.Internal, "registration failed")
		}
		s.logger.Info("agent %s registered successfully (ID: %s)", reg.ControllerId, agent.ID)
		return agent.ID, nil
	}

	// Some other error occurred
	if err != nil {
		s.logger.Err("failed to check agent %s: %s", reg.ControllerId, err)
		return "", status.Error(codes.Internal, "authentication failed")
	}

	// Agent exists - authenticate with stored token using agent ID
	authenticated, err := s.registry.Authenticate(ctx, agent.ID, reg.Token)
	if err != nil {
		s.logger.Err("authentication error for agent %s: %s", reg.ControllerId, err)
		return "", status.Error(codes.Internal, "authentication failed")
	}

	if !authenticated {
		s.logger.Warn("authentication failed for agent %s", reg.ControllerId)
		return "", status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Set agent to active status using agent ID
	if err := s.registry.SetStatus(ctx, agent.ID, AgentStatusActive); err != nil {
		s.logger.Err("failed to set agent %s to active: %s", reg.ControllerId, err)
		return "", status.Error(codes.Internal, "failed to update agent status")
	}

	s.logger.Info("agent %s authenticated successfully (cluster: %s, capacity: %d)",
		reg.ControllerId, agent.ClusterName, agent.Capacity)

	return agent.ID, nil
}

// handleHeartbeat processes agent heartbeat messages
func (s *GRPCServer) handleHeartbeat(ctx context.Context, agentID string, hb *proto.Heartbeat) error {
	if err := s.registry.UpdateHeartbeat(ctx, agentID, int(hb.CurrentJobs)); err != nil {
		return fmt.Errorf("updating heartbeat: %w", err)
	}

	s.logger.Debug("received heartbeat from agent %s (current jobs: %d, available: %d)",
		hb.ControllerId, hb.CurrentJobs, hb.AvailableCapacity)

	return nil
}

// handleJobResult processes job completion results
func (s *GRPCServer) handleJobResult(ctx context.Context, result *proto.JobResult) error {
	s.logger.Info("received job result for job %s from pod %s: status=%s planData=%d bytes",
		result.JobId, result.AgentPodName, result.Status, len(result.PlanData))

	// Determine final status
	var finalStatus db.JobStatus
	switch result.Status {
	case "completed":
		finalStatus = db.JobStatusCompleted
	case "failed":
		finalStatus = db.JobStatusFailed
	case "timeout":
		finalStatus = db.JobStatusFailed
	default:
		finalStatus = db.JobStatusFailed
	}

	// Store job output and result in database
	s.logger.Info("storing job result in database: jobID=%s status=%s planData=%d bytes",
		result.JobId, finalStatus, len(result.PlanData))
	if err := s.jobStore.UpdateResult(ctx, result.JobId, finalStatus, result.Output, result.PlanData, int(result.ExitCode), result.ErrorMessage); err != nil {
		return fmt.Errorf("updating job result: %w", err)
	}

	s.logger.Info("updated job %s to status %s with output (len=%d) and planData (len=%d)",
		result.JobId, finalStatus, len(result.Output), len(result.PlanData))

	// Post result to PR if handler is configured
	if s.resultHandler != nil {
		s.logger.Info("result handler is configured, posting to PR for job %s", result.JobId)
		// Fetch full job details from database
		job, err := s.jobStore.Get(ctx, result.JobId)
		if err != nil {
			s.logger.Err("failed to get job %s for result handling: %s", result.JobId, err)
		} else {
			s.logger.Info("fetched job details, calling HandleJobResult for job %s", result.JobId)
			if err := s.resultHandler.HandleJobResult(ctx, job, result); err != nil {
				s.logger.Err("failed to handle job result for %s: %s", result.JobId, err)
			} else {
				s.logger.Info("job result %s posted to PR successfully", result.JobId)
			}
		}
	} else {
		s.logger.Warn("result handler is NOT configured - job result will not be posted to PR")
	}

	return nil
}

// handleCapacityUpdate processes agent capacity changes
func (s *GRPCServer) handleCapacityUpdate(ctx context.Context, update *proto.CapacityUpdate) error {
	s.logger.Info("agent %s updated capacity to %d (reason: %s)",
		update.ControllerId, update.NewCapacity, update.Reason)

	// Update capacity in agent store
	agent, err := s.registry.GetAgent(ctx, update.ControllerId)
	if err != nil {
		return fmt.Errorf("getting agent: %w", err)
	}

	if err := s.registry.agentStore.UpdateCapacity(ctx, update.ControllerId, int(update.NewCapacity)); err != nil {
		return fmt.Errorf("updating capacity: %w", err)
	}

	// If reason is "draining", set agent to draining status
	if update.Reason == "draining" {
		if err := s.registry.SetStatus(ctx, update.ControllerId, AgentStatusDraining); err != nil {
			s.logger.Err("failed to set agent to draining: %s", err)
		}
	}

	s.logger.Info("agent %s capacity updated from %d to %d",
		update.ControllerId, agent.Capacity, update.NewCapacity)

	return nil
}

// handleDisconnect cleans up when an agent disconnects
func (s *GRPCServer) handleDisconnect(controllerID string) {
	s.logger.Info("agent %s disconnected", controllerID)

	// Remove connection
	s.connectionsMu.Lock()
	delete(s.connections, controllerID)
	s.connectionsMu.Unlock()

	// Close and remove job channel
	s.jobChannelsMu.Lock()
	if ch, exists := s.jobChannels[controllerID]; exists {
		close(ch)
		delete(s.jobChannels, controllerID)
	}
	s.jobChannelsMu.Unlock()

	// Set agent to offline (only for persistent agents, not ephemeral)
	// controllerID is the agent UUID for persistent agents, or pod name for ephemeral
	// Ephemeral agents are not in the database, so skip status update
	ctx := context.Background()

	// Try to set status - it will fail for ephemeral agents (not in DB), which is expected
	if err := s.registry.SetStatus(ctx, controllerID, AgentStatusOffline); err != nil {
		// Only log as warning if it's not a "not found" error (ephemeral agents)
		if err.Error() != "agent not found" && !isUUIDError(err) {
			s.logger.Err("failed to set agent %s to offline: %s", controllerID, err)
		} else {
			s.logger.Info("ephemeral agent %s disconnected (not in database)", controllerID)
		}
	}
}

// isUUIDError checks if an error is due to invalid UUID syntax
func isUUIDError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "invalid input syntax for type uuid") ||
		strings.Contains(errMsg, "invalid UUID format")
}

// addConnection tracks a new agent connection
func (s *GRPCServer) addConnection(controllerID string, conn *AgentConnection) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections[controllerID] = conn
}

// AssignJobToAgent sends a job to a connected agent
func (s *GRPCServer) AssignJobToAgent(controllerID string, assignment interface{}) error {
	// Convert db.Job to proto.JobAssignment
	var protoAssignment *proto.JobAssignment

	switch job := assignment.(type) {
	case *db.Job:
		protoAssignment = s.jobToProto(job)
	case *proto.JobAssignment:
		protoAssignment = job
	default:
		return fmt.Errorf("unsupported assignment type: %T", assignment)
	}

	s.jobChannelsMu.RLock()
	ch, exists := s.jobChannels[controllerID]
	s.jobChannelsMu.RUnlock()

	if !exists {
		return fmt.Errorf("agent %s not connected", controllerID)
	}

	select {
	case ch <- protoAssignment:
		s.logger.Info("queued job %s for agent %s", protoAssignment.JobId, controllerID)
		return nil
	default:
		return fmt.Errorf("agent %s job channel full", controllerID)
	}
}

// jobToProto converts a db.Job to proto.JobAssignment
func (s *GRPCServer) jobToProto(job *db.Job) *proto.JobAssignment {
	tfVersion := ""
	if job.TerraformVersion != nil {
		tfVersion = *job.TerraformVersion
	}

	// Get VCS credentials (dynamic or static)
	var vcsCredentials *proto.VCSCredentials
	var username, token string

	if s.vcsCredGetter != nil {
		// Try dynamic credentials first (GitHub Apps)
		var err error
		username, token, err = s.vcsCredGetter.GetCredentialsForRepo(job.RepoFullName)
		if err != nil {
			s.logger.Err("failed to get dynamic VCS credentials for %s: %s, falling back to static", job.RepoFullName, err)
			// Fallback to static
			username = s.vcsUser
			token = s.vcsToken
		} else {
			s.logger.Info("using dynamic VCS credentials for job %s repo %s", job.ID, job.RepoFullName)
		}
	} else {
		// Use static credentials
		username = s.vcsUser
		token = s.vcsToken
	}

	if username != "" && token != "" {
		vcsCredentials = &proto.VCSCredentials{
			VcsType:  "github", // TODO: detect from BaseRepo
			Username: username,
			Token:    token,
		}
		s.logger.Info("adding VCS credentials to job %s (user=%s)", job.ID, username)
	} else {
		s.logger.Warn("no VCS credentials available for job %s", job.ID)
	}

	return &proto.JobAssignment{
		JobId:              job.ID,
		RepoFullName:       job.RepoFullName,
		RepoCloneUrl:       job.RepoCloneURL,
		PullNum:            int32(job.PullNum),
		PullBranch:         job.PullBranch,
		PullBaseBranch:     job.PullBaseBranch,
		PullCommitSha:      job.PullCommitSHA,
		Command:            job.Command,
		Workspace:          job.Workspace,
		ProjectName:        job.ProjectName,
		ProjectDir:         job.ProjectDir,
		PlanData:           job.PlanData,
		VcsCredentials:     vcsCredentials,
		EnvVars:            job.EnvVars,
		TerraformVersion:   tfVersion,
		ProjectContextJson: job.ProjectContextJSON, // Serialized command.ProjectContext
	}
}

// GetConnectedAgents returns a list of currently connected agent IDs
func (s *GRPCServer) GetConnectedAgents() []string {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()

	agents := make([]string, 0, len(s.connections))
	for id := range s.connections {
		agents = append(agents, id)
	}
	return agents
}

// GetConnection returns connection info for an agent
func (s *GRPCServer) GetConnection(controllerID string) (*AgentConnection, bool) {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()

	conn, exists := s.connections[controllerID]
	return conn, exists
}

// ReportJobStatus handles status update RPCs from agents
func (s *GRPCServer) ReportJobStatus(ctx context.Context, update *proto.JobStatusUpdate) (*proto.Ack, error) {
	s.logger.Info("job status update: job=%s, status=%s, pod=%s",
		update.JobId, update.Status, update.AgentPodName)

	// Update job status in database
	if err := s.jobStore.UpdateStatus(ctx, update.JobId, db.JobStatus(update.Status)); err != nil {
		s.logger.Err("failed to update job status: %s", err)
		return &proto.Ack{
			Success: false,
			Message: fmt.Sprintf("failed to update status: %v", err),
		}, nil
	}

	return &proto.Ack{
		Success: true,
		Message: "status updated",
	}, nil
}

// GetJob retrieves job details by ID
func (s *GRPCServer) GetJob(ctx context.Context, req *proto.JobRequest) (*proto.JobDetails, error) {
	job, err := s.jobStore.Get(ctx, req.JobId)
	if err != nil {
		s.logger.Err("failed to get job %s: %s", req.JobId, err)
		return nil, status.Error(codes.NotFound, "job not found")
	}

	// Convert db.Job to proto.JobAssignment with all fields
	assignment := &proto.JobAssignment{
		JobId:              job.ID,
		RepoFullName:       job.RepoFullName,
		PullNum:            int32(job.PullNum),
		Command:            job.Command,
		Workspace:          job.Workspace,
		ProjectName:        job.ProjectName,
		ProjectDir:         job.ProjectDir,
		ProjectContextJson: job.ProjectContextJSON,
		PlanData:           job.PlanData,
		Metadata:           make(map[string]string), // Empty metadata for now
	}

	return &proto.JobDetails{
		Assignment: assignment,
		Status:     job.Status,
		CreatedAt:  job.CreatedAt.Unix(),
	}, nil
}

// SyncAssignedJobs returns all jobs currently assigned to an agent
// Used after reconnection to catch up on missed assignments
func (s *GRPCServer) SyncAssignedJobs(ctx context.Context, req *proto.SyncJobsRequest) (*proto.SyncJobsResponse, error) {
	// Look up agent by name to get UUID
	agent, err := s.registry.GetAgentByName(ctx, req.ControllerId)
	if err != nil {
		s.logger.Err("failed to get agent by name %s: %s", req.ControllerId, err)
		return nil, status.Error(codes.NotFound, "agent not found")
	}

	// Query for jobs assigned to this agent (by UUID)
	jobs, err := s.jobStore.GetByAgentAndStatus(ctx, agent.ID, "assigned")
	if err != nil {
		s.logger.Err("failed to get assigned jobs for agent %s (id: %s): %s", req.ControllerId, agent.ID, err)
		return nil, status.Error(codes.Internal, "failed to query jobs")
	}

	assignments := make([]*proto.JobAssignment, 0, len(jobs))
	for _, job := range jobs {
		assignments = append(assignments, s.jobToProto(job))
	}

	s.logger.Info("returning %d assigned jobs for agent %s", len(assignments), req.ControllerId)
	return &proto.SyncJobsResponse{
		Jobs: assignments,
	}, nil
}

// ExtractAgentID gets the agent controller ID from gRPC metadata
func ExtractAgentID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no metadata in context")
	}

	ids := md.Get("agent-id")
	if len(ids) == 0 {
		return "", fmt.Errorf("no agent-id in metadata")
	}

	return ids[0], nil
}

// isGracefulDisconnect checks if an error is from an expected disconnection
func isGracefulDisconnect(err error) bool {
	if err == nil {
		return false
	}

	// Check for context cancellation (normal shutdown)
	if err == context.Canceled {
		return true
	}

	// Check for EOF (connection closed normally)
	if err.Error() == "EOF" {
		return true
	}

	// Check for gRPC Canceled status
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Canceled
	}

	return false
}
