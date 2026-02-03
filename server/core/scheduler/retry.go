package scheduler

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// RetryPolicy defines retry behavior for failed jobs
type RetryPolicy struct {
	MaxAttempts     int           // Maximum retry attempts
	InitialDelay    time.Duration // Initial delay before first retry
	MaxDelay        time.Duration // Maximum delay between retries
	BackoffFactor   float64       // Exponential backoff multiplier
	RetryableErrors []string      // Error patterns that should trigger retry
}

// DefaultRetryPolicy returns sensible defaults
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  10 * time.Second,
		MaxDelay:      5 * time.Minute,
		BackoffFactor: 2.0,
		RetryableErrors: []string{
			"connection refused",
			"timeout",
			"network",
			"temporary failure",
			"unavailable",
		},
	}
}

// RetryManager manages job retries with exponential backoff
type RetryManager struct {
	policy     RetryPolicy
	jobStore   db.JobStore
	scheduler  *EnhancedScheduler // Use concrete type instead of interface
	logger     logging.SimpleLogging
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	retryQueue chan *retryItem
}

// retryItem represents a job pending retry
type retryItem struct {
	job        *db.Job
	attempt    int
	retryAfter time.Time
}

// NewRetryManager creates a new retry manager
func NewRetryManager(policy RetryPolicy, jobStore db.JobStore, scheduler *EnhancedScheduler, logger logging.SimpleLogging) *RetryManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &RetryManager{
		policy:     policy,
		jobStore:   jobStore,
		scheduler:  scheduler,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		retryQueue: make(chan *retryItem, 100),
	}
}

// Start begins processing retries
func (rm *RetryManager) Start() {
	rm.wg.Add(1)
	go rm.processRetries()
	rm.logger.Info("retry manager started (max attempts: %d, initial delay: %v)", rm.policy.MaxAttempts, rm.policy.InitialDelay)
}

// Stop stops the retry manager
func (rm *RetryManager) Stop() {
	rm.cancel()
	close(rm.retryQueue)
	rm.wg.Wait()
	rm.logger.Info("retry manager stopped")
}

// ScheduleRetry schedules a job for retry if eligible
func (rm *RetryManager) ScheduleRetry(job *db.Job, err error) error {
	if job == nil {
		return fmt.Errorf("cannot retry nil job")
	}

	// Check if error is retryable
	if !rm.isRetryableError(err) {
		rm.logger.Debug("job %s: error not retryable: %v", job.ID, err)
		return fmt.Errorf("error not retryable")
	}

	// Check max attempts
	if job.AttemptCount >= rm.policy.MaxAttempts {
		rm.logger.Info("job %s: max retry attempts reached (%d)", job.ID, job.AttemptCount)
		return fmt.Errorf("max retry attempts reached")
	}

	// Calculate retry delay
	delay := rm.calculateDelay(job.AttemptCount)
	retryTime := time.Now().Add(delay)

	item := &retryItem{
		job:        job,
		attempt:    job.AttemptCount + 1,
		retryAfter: retryTime,
	}

	select {
	case rm.retryQueue <- item:
		rm.logger.Info("job %s: scheduled retry attempt %d after %v", job.ID, item.attempt, delay)
		return nil
	case <-rm.ctx.Done():
		return fmt.Errorf("retry manager stopped")
	default:
		rm.logger.Warn("job %s: retry queue full, dropping retry", job.ID)
		return fmt.Errorf("retry queue full")
	}
}

// processRetries processes the retry queue
func (rm *RetryManager) processRetries() {
	defer rm.wg.Done()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case item, ok := <-rm.retryQueue:
			if !ok {
				return
			}
			rm.handleRetry(item)
		}
	}
}

// handleRetry waits for retry time and resubmits job
func (rm *RetryManager) handleRetry(item *retryItem) {
	// Wait until retry time
	waitDuration := time.Until(item.retryAfter)
	if waitDuration > 0 {
		timer := time.NewTimer(waitDuration)
		select {
		case <-rm.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			// Continue with retry
		}
	}

	// Update job for retry
	if err := rm.jobStore.UpdateStatus(rm.ctx, item.job.ID, db.JobStatusQueued); err != nil {
		rm.logger.Err("job %s: failed to update for retry: %v", item.job.ID, err)
		return
	}

	// Update attempt count in memory (would need dedicated store method)
	item.job.AttemptCount = item.attempt

	// Resubmit to scheduler
	if err := rm.scheduler.SubmitJob(rm.ctx, item.job); err != nil {
		rm.logger.Err("job %s: failed to resubmit for retry: %v", item.job.ID, err)
		// Mark as failed
		if err := rm.jobStore.UpdateStatus(rm.ctx, item.job.ID, db.JobStatusFailed); err != nil {
			rm.logger.Err("failed to update job %s status: %v", item.job.ID, err)
		}
		errMsg := fmt.Sprintf("retry submission failed: %v", err)
		if err := rm.jobStore.SetError(rm.ctx, item.job.ID, errMsg, 0); err != nil {
			rm.logger.Err("failed to set error for job %s: %v", item.job.ID, err)
		}
		return
	}

	rm.logger.Info("job %s: retry attempt %d submitted", item.job.ID, item.attempt)
}

// calculateDelay calculates exponential backoff delay
func (rm *RetryManager) calculateDelay(attempt int) time.Duration {
	// delay = initialDelay * (backoffFactor ^ attempt)
	delay := float64(rm.policy.InitialDelay) * math.Pow(rm.policy.BackoffFactor, float64(attempt))

	// Add jitter (±10%)
	jitter := (float64(time.Now().UnixNano()%100) / 100.0) * 0.2 // 0-20% variation
	delay = delay * (0.9 + jitter)

	// Cap at max delay
	if time.Duration(delay) > rm.policy.MaxDelay {
		delay = float64(rm.policy.MaxDelay)
	}

	return time.Duration(delay)
}

// isRetryableError checks if an error should trigger retry
func (rm *RetryManager) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	for _, pattern := range rm.policy.RetryableErrors {
		if contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// contains checks if s contains substr (case-insensitive)
func contains(s, substr string) bool {
	// Simple case-insensitive contains
	sLower := toLower(s)
	substrLower := toLower(substr)
	return len(sLower) >= len(substrLower) && findSubstring(sLower, substrLower)
}

func toLower(s string) string {
	result := make([]rune, len(s))
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			result[i] = r + 32
		} else {
			result[i] = r
		}
	}
	return string(result)
}

func findSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// CircuitBreaker prevents repeated failures to same agent
type CircuitBreaker struct {
	mu               sync.RWMutex
	failures         map[string]*circuitState // agentID -> state
	failureThreshold int
	resetTimeout     time.Duration
	logger           logging.SimpleLogging
}

type circuitState struct {
	failures    int
	lastFailure time.Time
	isOpen      bool
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration, logger logging.SimpleLogging) *CircuitBreaker {
	return &CircuitBreaker{
		failures:         make(map[string]*circuitState),
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		logger:           logger,
	}
}

// RecordSuccess records a successful job execution
func (cb *CircuitBreaker) RecordSuccess(agentID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if state, exists := cb.failures[agentID]; exists {
		if state.isOpen {
			cb.logger.Info("circuit breaker closed for agent %s", agentID)
		}
		delete(cb.failures, agentID)
	}
}

// RecordFailure records a failed job execution
func (cb *CircuitBreaker) RecordFailure(agentID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	state, exists := cb.failures[agentID]
	if !exists {
		state = &circuitState{}
		cb.failures[agentID] = state
	}

	state.failures++
	state.lastFailure = time.Now()

	if state.failures >= cb.failureThreshold && !state.isOpen {
		state.isOpen = true
		cb.logger.Warn("circuit breaker opened for agent %s after %d failures", agentID, state.failures)
	}
}

// IsOpen checks if circuit is open for an agent
func (cb *CircuitBreaker) IsOpen(agentID string) bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state, exists := cb.failures[agentID]
	if !exists {
		return false
	}

	// Check if reset timeout has passed
	if state.isOpen && time.Since(state.lastFailure) > cb.resetTimeout {
		cb.mu.RUnlock()
		cb.mu.Lock()
		// Double-check after acquiring write lock
		if time.Since(state.lastFailure) > cb.resetTimeout {
			cb.logger.Info("circuit breaker auto-reset for agent %s after timeout", agentID)
			delete(cb.failures, agentID)
			cb.mu.Unlock()
			cb.mu.RLock()
			return false
		}
		cb.mu.Unlock()
		cb.mu.RLock()
	}

	return state.isOpen
}

// Reset manually resets the circuit for an agent
func (cb *CircuitBreaker) Reset(agentID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if _, exists := cb.failures[agentID]; exists {
		delete(cb.failures, agentID)
		cb.logger.Info("circuit breaker manually reset for agent %s", agentID)
	}
}
