package scheduler

import (
	"context"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// StaleJobMonitor detects and reassigns jobs stuck in "assigned" status
// when their assigned agent is no longer active
type StaleJobMonitor struct {
	jobStore      db.JobStore
	agentStore    db.AgentStore
	logger        logging.SimpleLogging
	checkInterval time.Duration
	staleTimeout  time.Duration
	stopCh        chan struct{}
	stoppedCh     chan struct{}
}

// NewStaleJobMonitor creates a new stale job monitor
func NewStaleJobMonitor(
	jobStore db.JobStore,
	agentStore db.AgentStore,
	logger logging.SimpleLogging,
	checkIntervalSeconds int,
	staleTimeoutSeconds int,
) *StaleJobMonitor {
	return &StaleJobMonitor{
		jobStore:      jobStore,
		agentStore:    agentStore,
		logger:        logger,
		checkInterval: time.Duration(checkIntervalSeconds) * time.Second,
		staleTimeout:  time.Duration(staleTimeoutSeconds) * time.Second,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

// Start begins monitoring for stale assigned jobs
func (m *StaleJobMonitor) Start() {
	m.logger.Info("starting stale job monitor (interval: %v, timeout: %v)", m.checkInterval, m.staleTimeout)

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	defer close(m.stoppedCh)

	for {
		select {
		case <-m.stopCh:
			m.logger.Info("stopping stale job monitor")
			return
		case <-ticker.C:
			m.checkAndRequeueStaleJobs()
		}
	}
}

// Stop stops the monitor
func (m *StaleJobMonitor) Stop() {
	close(m.stopCh)
	<-m.stoppedCh
}

// checkAndRequeueStaleJobs checks for assigned jobs with offline/missing agents
func (m *StaleJobMonitor) checkAndRequeueStaleJobs() {
	ctx := context.Background()

	// Get all assigned jobs
	assignedJobs, err := m.jobStore.GetByStatus(ctx, "assigned", 1000)
	if err != nil {
		m.logger.Warn("failed to get assigned jobs: %v", err)
		return
	}

	if len(assignedJobs) == 0 {
		return
	}

	m.logger.Debug("checking %d assigned jobs for stale assignments", len(assignedJobs))

	requeuedCount := 0
	for _, job := range assignedJobs {
		// Skip if job was assigned very recently (give agent time to start)
		// Use AssignedAt if available, otherwise CreatedAt
		checkTime := job.CreatedAt
		if job.AssignedAt != nil {
			checkTime = *job.AssignedAt
		}
		if checkTime.Add(m.staleTimeout).After(time.Now()) {
			continue
		}

		// Check if assigned agent exists and is active
		if job.AgentControllerID == nil {
			m.logger.Warn("job %s is assigned but has no agent ID, requeuing", job.ID)
			if err := m.requeueJob(ctx, job); err != nil {
				m.logger.Warn("failed to requeue job %s: %v", job.ID, err)
			} else {
				requeuedCount++
			}
			continue
		}

		// Check agent status
		agent, err := m.agentStore.Get(ctx, *job.AgentControllerID)
		if err != nil {
			// Agent doesn't exist in database
			m.logger.Info("job %s assigned to non-existent agent %s, requeuing", job.ID, *job.AgentControllerID)
			if err := m.requeueJob(ctx, job); err != nil {
				m.logger.Warn("failed to requeue job %s: %v", job.ID, err)
			} else {
				requeuedCount++
			}
			continue
		}

		// Check if agent is offline
		if agent.Status == "offline" {
			m.logger.Info("job %s assigned to offline agent %s, requeuing", job.ID, agent.ID)
			if err := m.requeueJob(ctx, job); err != nil {
				m.logger.Warn("failed to requeue job %s: %v", job.ID, err)
			} else {
				requeuedCount++
			}
			continue
		}
	}

	if requeuedCount > 0 {
		m.logger.Info("requeued %d stale jobs", requeuedCount)
	}
}

// requeueJob resets a job back to queued status
func (m *StaleJobMonitor) requeueJob(ctx context.Context, job *db.Job) error {
	m.logger.Info("requeuing job %s (was assigned to %v)", job.ID, job.AgentControllerID)

	// Update job status back to queued and clear agent assignment
	return m.jobStore.RequeueJob(ctx, job.ID)
}
