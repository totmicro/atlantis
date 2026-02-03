package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/runatlantis/atlantis/server/logging"
)

// AgentManager coordinates between the scheduler and gRPC server for job distribution
type AgentManager struct {
	scheduler      *scheduler.EnhancedScheduler
	grpcServer     *GRPCServer
	jobStore       db.JobStore
	agentStore     db.AgentStore
	logger         logging.SimpleLogging
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	activeStreams  map[string]*AgentStream // agentID -> stream
	streamsMu      sync.RWMutex
	assignmentChan chan *JobAssignment
	resultChan     chan *JobResult
	checkInterval  time.Duration
}

// AgentStream represents an active gRPC stream to an agent
type AgentStream struct {
	AgentID     string
	Stream      proto.AgentService_StreamJobsServer
	LastSeen    time.Time
	JobsRunning int
	mu          sync.Mutex
}

// JobAssignment pairs a job with its assigned agent
type JobAssignment struct {
	Job     *db.Job
	AgentID string
}

// JobResult represents a completed job result from an agent
type JobResult struct {
	JobID        string
	AgentID      string
	Status       string
	Output       string
	ErrorMessage string
	PlanOutput   string
	ExitCode     int
	CompletedAt  time.Time
}

// AgentManagerConfig configuration for agent manager
type AgentManagerConfig struct {
	CheckInterval    time.Duration
	AssignmentBuffer int
	ResultBuffer     int
	StreamTimeout    time.Duration
	EnableMetrics    bool
}

// DefaultAgentManagerConfig returns sensible defaults
func DefaultAgentManagerConfig() AgentManagerConfig {
	return AgentManagerConfig{
		CheckInterval:    5 * time.Second,
		AssignmentBuffer: 100,
		ResultBuffer:     100,
		StreamTimeout:    2 * time.Minute,
		EnableMetrics:    true,
	}
}

// NewAgentManager creates a new agent manager
func NewAgentManager(
	sched *scheduler.EnhancedScheduler,
	grpcServer *GRPCServer,
	jobStore db.JobStore,
	agentStore db.AgentStore,
	logger logging.SimpleLogging,
	config AgentManagerConfig,
) *AgentManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &AgentManager{
		scheduler:      sched,
		grpcServer:     grpcServer,
		jobStore:       jobStore,
		agentStore:     agentStore,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		activeStreams:  make(map[string]*AgentStream),
		assignmentChan: make(chan *JobAssignment, config.AssignmentBuffer),
		resultChan:     make(chan *JobResult, config.ResultBuffer),
		checkInterval:  config.CheckInterval,
	}
}

// Start begins the agent manager
func (am *AgentManager) Start() error {
	am.logger.Info("starting agent manager")

	// Start the scheduler
	if err := am.scheduler.Start(); err != nil {
		return fmt.Errorf("starting scheduler: %w", err)
	}

	// Start background workers
	am.wg.Add(3)
	go am.processAssignments()
	go am.processResults()
	go am.monitorStreams()

	am.logger.Info("agent manager started")
	return nil
}

// Stop stops the agent manager
func (am *AgentManager) Stop() {
	am.logger.Info("stopping agent manager")

	am.cancel()
	am.wg.Wait()

	// Stop scheduler
	am.scheduler.Stop()

	// Close channels
	close(am.assignmentChan)
	close(am.resultChan)

	am.logger.Info("agent manager stopped")
}

// RegisterAgentStream registers a new agent gRPC stream
func (am *AgentManager) RegisterAgentStream(agentID string, stream proto.AgentService_StreamJobsServer) error {
	am.streamsMu.Lock()
	defer am.streamsMu.Unlock()

	if _, exists := am.activeStreams[agentID]; exists {
		am.logger.Warn("agent %s stream already registered, replacing", agentID)
	}

	am.activeStreams[agentID] = &AgentStream{
		AgentID:     agentID,
		Stream:      stream,
		LastSeen:    time.Now(),
		JobsRunning: 0,
	}

	am.logger.Info("registered stream for agent %s", agentID)
	return nil
}

// UnregisterAgentStream removes an agent stream
func (am *AgentManager) UnregisterAgentStream(agentID string) {
	am.streamsMu.Lock()
	defer am.streamsMu.Unlock()

	delete(am.activeStreams, agentID)
	am.logger.Info("unregistered stream for agent %s", agentID)
}

// AssignJob assigns a job to an agent (called by scheduler)
func (am *AgentManager) AssignJob(job *db.Job, agentID string) error {
	assignment := &JobAssignment{
		Job:     job,
		AgentID: agentID,
	}

	select {
	case am.assignmentChan <- assignment:
		am.logger.Debug("queued job %s for assignment to agent %s", job.ID, agentID)
		return nil
	case <-am.ctx.Done():
		return fmt.Errorf("agent manager stopped")
	default:
		return fmt.Errorf("assignment channel full")
	}
}

// ReportResult handles a job result from an agent
func (am *AgentManager) ReportResult(result *JobResult) error {
	select {
	case am.resultChan <- result:
		am.logger.Debug("queued result for job %s from agent %s", result.JobID, result.AgentID)
		return nil
	case <-am.ctx.Done():
		return fmt.Errorf("agent manager stopped")
	default:
		return fmt.Errorf("result channel full")
	}
}

// processAssignments processes job assignments and sends to agents
func (am *AgentManager) processAssignments() {
	defer am.wg.Done()

	for {
		select {
		case <-am.ctx.Done():
			return
		case assignment, ok := <-am.assignmentChan:
			if !ok {
				return
			}
			am.handleAssignment(assignment)
		}
	}
}

// handleAssignment sends a job to an agent via gRPC stream
func (am *AgentManager) handleAssignment(assignment *JobAssignment) {
	am.streamsMu.RLock()
	stream, exists := am.activeStreams[assignment.AgentID]
	am.streamsMu.RUnlock()

	if !exists {
		am.logger.Err("no stream for agent %s, cannot assign job %s", assignment.AgentID, assignment.Job.ID)
		// Requeue the job
		if err := am.scheduler.SubmitJob(am.ctx, assignment.Job); err != nil {
			am.logger.Err("failed to requeue job %s: %v", assignment.Job.ID, err)
		}
		return
	}

	// Build job assignment message
	jobMsg := am.jobToProto(assignment.Job)

	// Send to agent via stream
	stream.mu.Lock()
	err := stream.Stream.Send(jobMsg)
	stream.mu.Unlock()

	if err != nil {
		am.logger.Err("failed to send job %s to agent %s: %v", assignment.Job.ID, assignment.AgentID, err)

		// Remove dead stream
		am.UnregisterAgentStream(assignment.AgentID)

		// Requeue job
		if err := am.scheduler.SubmitJob(am.ctx, assignment.Job); err != nil {
			am.logger.Err("failed to requeue job %s: %v", assignment.Job.ID, err)
		}
		return
	}

	// Update stream state
	stream.mu.Lock()
	stream.JobsRunning++
	stream.LastSeen = time.Now()
	stream.mu.Unlock()

	// Update job status to running
	if err := am.jobStore.UpdateStatus(am.ctx, assignment.Job.ID, db.JobStatusRunning); err != nil {
		am.logger.Err("failed to update job %s status to running: %v", assignment.Job.ID, err)
	}

	am.logger.Info("assigned job %s to agent %s", assignment.Job.ID, assignment.AgentID)
}

// processResults processes job results from agents
func (am *AgentManager) processResults() {
	defer am.wg.Done()

	for {
		select {
		case <-am.ctx.Done():
			return
		case result, ok := <-am.resultChan:
			if !ok {
				return
			}
			am.handleResult(result)
		}
	}
}

// handleResult processes a job result from an agent
func (am *AgentManager) handleResult(result *JobResult) {
	am.logger.Info("processing result for job %s from agent %s: status=%s", result.JobID, result.AgentID, result.Status)

	// Update stream job count
	am.streamsMu.RLock()
	if stream, exists := am.activeStreams[result.AgentID]; exists {
		stream.mu.Lock()
		if stream.JobsRunning > 0 {
			stream.JobsRunning--
		}
		stream.mu.Unlock()
	}
	am.streamsMu.RUnlock()

	// Convert to scheduler result format
	schedResult := &scheduler.JobResult{
		JobID:        result.JobID,
		Status:       result.Status,
		Output:       result.Output,
		ErrorMessage: result.ErrorMessage,
		PlanOutput:   result.PlanOutput,
		ExitCode:     result.ExitCode,
		CompletedAt:  result.CompletedAt,
		AgentID:      result.AgentID,
	}

	// Pass to scheduler for handling (updates DB, triggers callbacks, handles retries)
	if err := am.scheduler.HandleJobResult(am.ctx, schedResult); err != nil {
		am.logger.Err("failed to handle result for job %s: %v", result.JobID, err)
	}
}

// monitorStreams monitors agent streams for timeouts
func (am *AgentManager) monitorStreams() {
	defer am.wg.Done()

	ticker := time.NewTicker(am.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-am.ctx.Done():
			return
		case <-ticker.C:
			am.checkStreamHealth()
		}
	}
}

// checkStreamHealth checks for stale agent streams
func (am *AgentManager) checkStreamHealth() {
	am.streamsMu.RLock()
	staleAgents := []string{}
	for agentID, stream := range am.activeStreams {
		stream.mu.Lock()
		timeSinceLastSeen := time.Since(stream.LastSeen)
		stream.mu.Unlock()

		if timeSinceLastSeen > 2*time.Minute {
			staleAgents = append(staleAgents, agentID)
		}
	}
	am.streamsMu.RUnlock()

	// Remove stale streams
	for _, agentID := range staleAgents {
		am.logger.Warn("removing stale stream for agent %s", agentID)
		am.UnregisterAgentStream(agentID)
	}

	if len(am.activeStreams) > 0 {
		am.logger.Debug("active streams: %d", len(am.activeStreams))
	}
}

// UpdateStreamHeartbeat updates the last seen time for an agent stream
func (am *AgentManager) UpdateStreamHeartbeat(agentID string) {
	am.streamsMu.RLock()
	defer am.streamsMu.RUnlock()

	if stream, exists := am.activeStreams[agentID]; exists {
		stream.mu.Lock()
		stream.LastSeen = time.Now()
		stream.mu.Unlock()
	}
}

// GetActiveStreamCount returns the number of active agent streams
func (am *AgentManager) GetActiveStreamCount() int {
	am.streamsMu.RLock()
	defer am.streamsMu.RUnlock()
	return len(am.activeStreams)
}

// GetStreamInfo returns info about a specific agent stream
func (am *AgentManager) GetStreamInfo(agentID string) (jobsRunning int, lastSeen time.Time, exists bool) {
	am.streamsMu.RLock()
	defer am.streamsMu.RUnlock()

	if stream, ok := am.activeStreams[agentID]; ok {
		stream.mu.Lock()
		jobsRunning = stream.JobsRunning
		lastSeen = stream.LastSeen
		stream.mu.Unlock()
		exists = true
	}
	return
}

// jobToProto converts a db.Job to proto.JobAssignment
func (am *AgentManager) jobToProto(job *db.Job) *proto.JobAssignment {
	tfVersion := ""
	if job.TerraformVersion != nil {
		tfVersion = *job.TerraformVersion
	}

	return &proto.JobAssignment{
		JobId:            job.ID,
		RepoFullName:     job.RepoFullName,
		RepoCloneUrl:     job.RepoCloneURL,
		PullNum:          int32(job.PullNum),
		PullBranch:       job.PullBranch,
		PullBaseBranch:   job.PullBaseBranch,
		PullCommitSha:    job.PullCommitSHA,
		Command:          job.Command,
		Workspace:        job.Workspace,
		ProjectName:      job.ProjectName,
		ProjectDir:       job.ProjectDir,
		PlanData:         job.PlanData,
		EnvVars:          job.EnvVars,
		TerraformVersion: tfVersion,
	}
}

// GetStats returns agent manager statistics
func (am *AgentManager) GetStats() map[string]interface{} {
	am.streamsMu.RLock()
	totalJobsRunning := 0
	for _, stream := range am.activeStreams {
		stream.mu.Lock()
		totalJobsRunning += stream.JobsRunning
		stream.mu.Unlock()
	}
	streamCount := len(am.activeStreams)
	am.streamsMu.RUnlock()

	stats := am.scheduler.GetSchedulerStats()
	stats["active_streams"] = streamCount
	stats["total_jobs_running"] = totalJobsRunning
	stats["assignment_queue_depth"] = len(am.assignmentChan)
	stats["result_queue_depth"] = len(am.resultChan)

	return stats
}
