package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// ResultCallback is invoked when a job completes
type ResultCallback func(ctx context.Context, job *db.Job, result *JobResult) error

// JobResult represents the outcome of a job execution
type JobResult struct {
	JobID        string
	Status       string // "success", "failed"
	Output       string
	ErrorMessage string
	PlanOutput   string
	ExitCode     int
	StartedAt    time.Time
	CompletedAt  time.Time
	AgentID      string
}

// CallbackManager manages result callbacks from agents
type CallbackManager struct {
	mu             sync.RWMutex
	callbacks      map[string]ResultCallback // jobID -> callback
	jobStore       db.JobStore
	retryManager   *RetryManager
	circuitBreaker *CircuitBreaker
	logger         logging.SimpleLogging
}

// NewCallbackManager creates a new callback manager
func NewCallbackManager(jobStore db.JobStore, retryManager *RetryManager, circuitBreaker *CircuitBreaker, logger logging.SimpleLogging) *CallbackManager {
	return &CallbackManager{
		callbacks:      make(map[string]ResultCallback),
		jobStore:       jobStore,
		retryManager:   retryManager,
		circuitBreaker: circuitBreaker,
		logger:         logger,
	}
}

// RegisterCallback registers a callback for a job
func (cm *CallbackManager) RegisterCallback(jobID string, callback ResultCallback) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.callbacks[jobID] = callback
	cm.logger.Debug("registered callback for job %s", jobID)
}

// UnregisterCallback removes a callback for a job
func (cm *CallbackManager) UnregisterCallback(jobID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.callbacks, jobID)
	cm.logger.Debug("unregistered callback for job %s", jobID)
}

// HandleResult processes a job result from an agent
func (cm *CallbackManager) HandleResult(ctx context.Context, result *JobResult) error {
	if result == nil {
		return fmt.Errorf("result is nil")
	}

	cm.logger.Info("handling result for job %s: status=%s, agent=%s", result.JobID, result.Status, result.AgentID)

	// Get job from store
	job, err := cm.jobStore.Get(ctx, result.JobID)
	if err != nil {
		cm.logger.Err("failed to get job %s: %v", result.JobID, err)
		return fmt.Errorf("getting job: %w", err)
	}

	// Update job status based on result
	var newStatus db.JobStatus
	if result.Status == "success" {
		newStatus = db.JobStatusCompleted // Use Completed instead of Success
	} else {
		newStatus = db.JobStatusFailed
	}

	if err := cm.jobStore.UpdateStatus(ctx, job.ID, newStatus); err != nil {
		cm.logger.Err("failed to update job %s: %v", result.JobID, err)
		return fmt.Errorf("updating job: %w", err)
	}

	// Store error message if failed
	if result.Status != "success" && result.ErrorMessage != "" {
		if err := cm.jobStore.SetError(ctx, job.ID, result.ErrorMessage, result.ExitCode); err != nil {
			cm.logger.Err("failed to set error for job %s: %v", job.ID, err)
		}
	}

	// Update circuit breaker
	if result.Status == "success" {
		if cm.circuitBreaker != nil {
			cm.circuitBreaker.RecordSuccess(result.AgentID)
		}
	} else {
		if cm.circuitBreaker != nil {
			cm.circuitBreaker.RecordFailure(result.AgentID)
		}

		// Attempt retry if eligible
		if cm.retryManager != nil {
			err := fmt.Errorf("%s", result.ErrorMessage)
			if retryErr := cm.retryManager.ScheduleRetry(job, err); retryErr != nil {
				cm.logger.Debug("job %s: not scheduling retry: %v", job.ID, retryErr)
			}
		}
	}

	// Invoke registered callback if exists
	cm.mu.RLock()
	callback, exists := cm.callbacks[result.JobID]
	cm.mu.RUnlock()

	if exists {
		if err := callback(ctx, job, result); err != nil {
			cm.logger.Err("callback error for job %s: %v", result.JobID, err)
			return fmt.Errorf("callback error: %w", err)
		}
		cm.UnregisterCallback(result.JobID) // Clean up after callback
	}

	// For plan jobs, store the plan output
	if job.Command == "plan" && result.PlanOutput != "" {
		cm.logger.Debug("job %s: plan output received (%d bytes)", job.ID, len(result.PlanOutput))
		if err := cm.jobStore.SavePlan(ctx, job.ID, []byte(result.PlanOutput)); err != nil {
			cm.logger.Err("failed to save plan for job %s: %v", job.ID, err)
		}
	}

	return nil
}

// GetPendingCallbacks returns count of pending callbacks
func (cm *CallbackManager) GetPendingCallbacks() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.callbacks)
}

// JobStatusUpdater handles periodic job status updates
type JobStatusUpdater struct {
	jobStore   db.JobStore
	logger     logging.SimpleLogging
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	updateChan chan *StatusUpdate
}

// StatusUpdate represents a job status update
type StatusUpdate struct {
	JobID     string
	Status    string
	Progress  int // 0-100
	Message   string
	Timestamp time.Time
}

// NewJobStatusUpdater creates a new status updater
func NewJobStatusUpdater(jobStore db.JobStore, logger logging.SimpleLogging) *JobStatusUpdater {
	ctx, cancel := context.WithCancel(context.Background())

	return &JobStatusUpdater{
		jobStore:   jobStore,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		updateChan: make(chan *StatusUpdate, 100),
	}
}

// Start begins processing status updates
func (u *JobStatusUpdater) Start() {
	u.wg.Add(1)
	go u.processUpdates()
	u.logger.Info("job status updater started")
}

// Stop stops the status updater
func (u *JobStatusUpdater) Stop() {
	u.cancel()
	close(u.updateChan)
	u.wg.Wait()
	u.logger.Info("job status updater stopped")
}

// UpdateStatus queues a status update
func (u *JobStatusUpdater) UpdateStatus(update *StatusUpdate) error {
	select {
	case u.updateChan <- update:
		return nil
	case <-u.ctx.Done():
		return fmt.Errorf("status updater stopped")
	default:
		u.logger.Warn("status update channel full, dropping update for job %s", update.JobID)
		return fmt.Errorf("update channel full")
	}
}

// processUpdates processes the update queue
func (u *JobStatusUpdater) processUpdates() {
	defer u.wg.Done()

	for {
		select {
		case <-u.ctx.Done():
			return
		case update, ok := <-u.updateChan:
			if !ok {
				return
			}
			u.handleUpdate(update)
		}
	}
}

// handleUpdate processes a single status update
func (u *JobStatusUpdater) handleUpdate(update *StatusUpdate) {
	_, err := u.jobStore.Get(u.ctx, update.JobID)
	if err != nil {
		u.logger.Err("failed to get job %s for status update: %v", update.JobID, err)
		return
	}

	// Convert status string to JobStatus
	var status db.JobStatus
	switch update.Status {
	case "pending":
		status = db.JobStatusQueued
	case "running":
		status = db.JobStatusRunning
	case "success":
		status = db.JobStatusCompleted
	case "failed":
		status = db.JobStatusFailed
	default:
		u.logger.Warn("unknown status %s for job %s", update.Status, update.JobID)
		return
	}

	// Update in database
	if err := u.jobStore.UpdateStatus(u.ctx, update.JobID, status); err != nil {
		u.logger.Err("failed to update job %s status: %v", update.JobID, err)
		return
	}

	// Store error message if failed
	if update.Status == "failed" && update.Message != "" {
		if err := u.jobStore.SetError(u.ctx, update.JobID, update.Message, 0); err != nil {
			u.logger.Err("failed to set error for job %s: %v", update.JobID, err)
		}
	}

	u.logger.Debug("job %s: status updated to %s", update.JobID, update.Status)
}

// ResultAggregator collects and aggregates job results
type ResultAggregator struct {
	mu      sync.RWMutex
	results map[string]*JobResult // jobID -> result
	logger  logging.SimpleLogging
}

// NewResultAggregator creates a new result aggregator
func NewResultAggregator(logger logging.SimpleLogging) *ResultAggregator {
	return &ResultAggregator{
		results: make(map[string]*JobResult),
		logger:  logger,
	}
}

// AddResult adds a job result
func (ra *ResultAggregator) AddResult(result *JobResult) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.results[result.JobID] = result
	ra.logger.Debug("added result for job %s", result.JobID)
}

// GetResult retrieves a job result
func (ra *ResultAggregator) GetResult(jobID string) (*JobResult, bool) {
	ra.mu.RLock()
	defer ra.mu.RUnlock()
	result, exists := ra.results[jobID]
	return result, exists
}

// RemoveResult removes a job result
func (ra *ResultAggregator) RemoveResult(jobID string) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	delete(ra.results, jobID)
	ra.logger.Debug("removed result for job %s", jobID)
}

// GetStats returns aggregated statistics
func (ra *ResultAggregator) GetStats() map[string]int {
	ra.mu.RLock()
	defer ra.mu.RUnlock()

	stats := map[string]int{
		"total":   len(ra.results),
		"success": 0,
		"failed":  0,
	}

	for _, result := range ra.results {
		if result.Status == "success" {
			stats["success"]++
		} else if result.Status == "failed" {
			stats["failed"]++
		}
	}

	return stats
}

// Clear removes all results
func (ra *ResultAggregator) Clear() {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	ra.results = make(map[string]*JobResult)
	ra.logger.Debug("cleared all results")
}
