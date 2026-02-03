package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// EnhancedScheduler implements comprehensive job scheduling with queuing, retries, and callbacks
type EnhancedScheduler struct {
	jobStore        db.JobStore
	agentStore      db.AgentStore
	router          *Router
	queue           JobQueue
	queueMonitor    *JobQueueMonitor
	retryManager    *RetryManager
	circuitBreaker  *CircuitBreaker
	callbackManager *CallbackManager
	statusUpdater   *JobStatusUpdater
	logger          logging.SimpleLogging
	config          EnhancedSchedulerConfig
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	running         bool
	mu              sync.RWMutex
}

// EnhancedSchedulerConfig configuration for the enhanced scheduler
type EnhancedSchedulerConfig struct {
	RouterConfig         RouterConfig
	MaxQueueSize         int
	QueueCheckInterval   time.Duration
	AssignmentInterval   time.Duration
	RetryPolicy          RetryPolicy
	CircuitBreakerConfig struct {
		FailureThreshold int
		ResetTimeout     time.Duration
	}
	EnableMetrics bool
}

// DefaultEnhancedSchedulerConfig returns sensible defaults
func DefaultEnhancedSchedulerConfig() EnhancedSchedulerConfig {
	return EnhancedSchedulerConfig{
		RouterConfig:       RouterConfig{},
		MaxQueueSize:       1000,
		QueueCheckInterval: 30 * time.Second,
		AssignmentInterval: 5 * time.Second,
		RetryPolicy:        DefaultRetryPolicy(),
		CircuitBreakerConfig: struct {
			FailureThreshold int
			ResetTimeout     time.Duration
		}{
			FailureThreshold: 5,
			ResetTimeout:     5 * time.Minute,
		},
		EnableMetrics: true,
	}
}

// NewEnhancedScheduler creates a new enhanced scheduler
func NewEnhancedScheduler(
	jobStore db.JobStore,
	agentStore db.AgentStore,
	logger logging.SimpleLogging,
	config EnhancedSchedulerConfig,
) *EnhancedScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	// Create queue
	queue := NewPriorityJobQueue(logger)

	// Create circuit breaker
	circuitBreaker := NewCircuitBreaker(
		config.CircuitBreakerConfig.FailureThreshold,
		config.CircuitBreakerConfig.ResetTimeout,
		logger,
	)

	scheduler := &EnhancedScheduler{
		jobStore:       jobStore,
		agentStore:     agentStore,
		router:         NewRouter(config.RouterConfig),
		queue:          queue,
		circuitBreaker: circuitBreaker,
		logger:         logger,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Create retry manager (needs scheduler reference)
	scheduler.retryManager = NewRetryManager(config.RetryPolicy, jobStore, scheduler, logger)

	// Create callback manager
	scheduler.callbackManager = NewCallbackManager(jobStore, scheduler.retryManager, circuitBreaker, logger)

	// Create status updater
	scheduler.statusUpdater = NewJobStatusUpdater(jobStore, logger)

	// Create queue monitor
	queueMonitorConfig := JobQueueMonitorConfig{
		MaxQueueSize:  config.MaxQueueSize,
		CheckInterval: config.QueueCheckInterval,
	}
	scheduler.queueMonitor = NewJobQueueMonitor(queue, jobStore, queueMonitorConfig, logger)

	return scheduler
}

// Start starts the scheduler and all its components
func (s *EnhancedScheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("scheduler already running")
	}

	s.logger.Info("starting enhanced scheduler")

	// Start components
	s.retryManager.Start()
	s.callbackManager.Start()
	s.statusUpdater.Start()
	s.queueMonitor.Start()

	// Start assignment loop
	s.wg.Add(1)
	go s.assignmentLoop()

	s.running = true
	s.logger.Info("enhanced scheduler started")
	return nil
}

// Stop stops the scheduler and all its components
func (s *EnhancedScheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("stopping enhanced scheduler")

	s.cancel()
	s.wg.Wait()

	// Stop components
	s.retryManager.Stop()
	s.statusUpdater.Stop()
	s.queueMonitor.Stop()

	s.logger.Info("enhanced scheduler stopped")
}

// SubmitJob adds a job to the queue
func (s *EnhancedScheduler) SubmitJob(ctx context.Context, job *db.Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	// Update job status to queued
	if err := s.jobStore.UpdateStatus(ctx, job.ID, db.JobStatusQueued); err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}

	// Add to queue
	if err := s.queue.Enqueue(job); err != nil {
		return fmt.Errorf("enqueuing job: %w", err)
	}

	s.logger.Info("job %s submitted (queue size: %d)", job.ID, s.queue.Size())
	return nil
}

// Start callback manager (was missing)
func (cm *CallbackManager) Start() {
	// Callback manager doesn't need a background goroutine
	// It's called synchronously when results arrive
}

// assignmentLoop continuously assigns jobs to agents
func (s *EnhancedScheduler) assignmentLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.AssignmentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.processQueue()
		}
	}
}

// processQueue attempts to assign queued jobs to available agents
func (s *EnhancedScheduler) processQueue() {
	queueSize := s.queue.Size()
	if queueSize == 0 {
		return
	}

	s.logger.Debug("processing queue: %d jobs pending", queueSize)

	// Get available agents
	agents, err := s.agentStore.GetAvailable(s.ctx, nil, 100)
	if err != nil {
		s.logger.Err("failed to get available agents: %v", err)
		return
	}

	if len(agents) == 0 {
		s.logger.Debug("no available agents for job assignment")
		return
	}

	// Filter agents by circuit breaker
	availableAgents := make([]*db.AgentController, 0, len(agents))
	for _, agent := range agents {
		if !s.circuitBreaker.IsOpen(agent.ID) {
			availableAgents = append(availableAgents, agent)
		} else {
			s.logger.Debug("agent %s filtered by circuit breaker", agent.ID)
		}
	}

	if len(availableAgents) == 0 {
		s.logger.Warn("no agents available (all circuit breakers open)")
		return
	}

	// Assign jobs to agents
	assigned := 0
	for len(availableAgents) > 0 && s.queue.Size() > 0 {
		// Get next job
		job, err := s.queue.Peek()
		if err != nil {
			break
		}

		// Find matching agent
		agent := s.router.SelectBestAgentForDBJob(job, availableAgents)
		if agent == nil {
			s.logger.Debug("no matching agent for job %s", job.ID)
			break
		}

		// Dequeue and assign
		job, _ = s.queue.Dequeue()
		if err := s.assignJobToAgent(job, agent); err != nil {
			s.logger.Err("failed to assign job %s to agent %s: %v", job.ID, agent.ID, err)
			// Re-queue the job
			_ = s.queue.Enqueue(job)
			break
		}

		assigned++

		// Remove agent from available list
		for i, a := range availableAgents {
			if a.ID == agent.ID {
				availableAgents = append(availableAgents[:i], availableAgents[i+1:]...)
				break
			}
		}
	}

	if assigned > 0 {
		s.logger.Info("assigned %d jobs to agents", assigned)
	}
}

// assignJobToAgent assigns a job to a specific agent
func (s *EnhancedScheduler) assignJobToAgent(job *db.Job, agent *db.AgentController) error {
	// Update job status and assign to agent
	if err := s.jobStore.UpdateStatus(s.ctx, job.ID, db.JobStatusAssigned); err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}

	if err := s.jobStore.AssignToAgent(s.ctx, job.ID, agent.ID); err != nil {
		return fmt.Errorf("assigning job to agent: %w", err)
	}

	s.logger.Info("assigned job %s to agent %s", job.ID, agent.ID)

	// TODO: Send job to agent via gRPC
	// This would be implemented by the agent manager that uses the scheduler

	return nil
}

// HandleJobResult processes a job result from an agent
func (s *EnhancedScheduler) HandleJobResult(ctx context.Context, result *JobResult) error {
	return s.callbackManager.HandleResult(ctx, result)
}

// RegisterCallback registers a callback for job completion
func (s *EnhancedScheduler) RegisterCallback(jobID string, callback ResultCallback) {
	s.callbackManager.RegisterCallback(jobID, callback)
}

// CancelJob cancels a pending or running job
func (s *EnhancedScheduler) CancelJob(ctx context.Context, jobID string) error {
	// Try to remove from queue first
	if err := s.queue.Remove(jobID); err == nil {
		s.logger.Info("job %s removed from queue", jobID)

		// Update status in database
		if err := s.jobStore.UpdateStatus(ctx, jobID, db.JobStatusCancelled); err != nil {
		}

		return nil
	}

	// Job not in queue, might be running - update status
	job, err := s.jobStore.Get(ctx, jobID)
	if err != nil {
		return fmt.Errorf("getting job: %w", err)
	}

	if string(job.Status) == "running" {
		// TODO: Send cancellation signal to agent
		s.logger.Info("job %s is running, cancellation signal would be sent to agent", jobID)
	}

	if err := s.jobStore.UpdateStatus(ctx, jobID, db.JobStatusCancelled); err != nil {
		return fmt.Errorf("updating job: %w", err)
	}

	return nil
}

// GetQueueSize returns the current queue size
func (s *EnhancedScheduler) GetQueueSize() int {
	return s.queue.Size()
}

// GetQueuedJobs returns all queued jobs
func (s *EnhancedScheduler) GetQueuedJobs() []*db.Job {
	return s.queue.GetAll()
}

// GetSchedulerStats returns scheduler statistics
func (s *EnhancedScheduler) GetSchedulerStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"running":           s.running,
		"queue_size":        s.queue.Size(),
		"pending_callbacks": s.callbackManager.GetPendingCallbacks(),
		"max_queue_size":    s.config.MaxQueueSize,
	}
}
