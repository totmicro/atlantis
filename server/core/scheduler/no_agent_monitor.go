package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// JobResultUpdater posts job results back to VCS
type JobResultUpdater interface {
	PostJobFailureComment(logger logging.SimpleLogging, repoFullName string, pullNum int, jobID string, errorMsg string) error
	PostBatchJobFailureComment(logger logging.SimpleLogging, repoFullName string, pullNum int, jobs []*db.Job) error
}

// NoAgentMonitor checks for jobs that can't find matching agents
type NoAgentMonitor struct {
	jobStore      db.JobStore
	logger        logging.SimpleLogging
	maxRetries    int
	checkInterval time.Duration
	resultUpdater JobResultUpdater
	stopCh        chan struct{}
	stoppedCh     chan struct{}
}

// NewNoAgentMonitor creates a new no-agent monitor
func NewNoAgentMonitor(
	jobStore db.JobStore,
	logger logging.SimpleLogging,
	maxRetries int,
	checkIntervalSeconds int,
	resultUpdater JobResultUpdater,
) *NoAgentMonitor {
	return &NoAgentMonitor{
		jobStore:      jobStore,
		logger:        logger,
		maxRetries:    maxRetries,
		checkInterval: time.Duration(checkIntervalSeconds) * time.Second,
		resultUpdater: resultUpdater,
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

// Start begins monitoring for jobs that can't be assigned
func (m *NoAgentMonitor) Start() {
	m.logger.Info("starting no-agent monitor (max retries: %d, interval: %v)", m.maxRetries, m.checkInterval)

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	defer close(m.stoppedCh)

	for {
		select {
		case <-m.stopCh:
			m.logger.Info("stopping no-agent monitor")
			return
		case <-ticker.C:
			m.checkAndCancelStuckJobs()
		}
	}
}

// Stop stops the monitor
func (m *NoAgentMonitor) Stop() {
	close(m.stopCh)
	<-m.stoppedCh
}

// checkAndCancelStuckJobs checks for jobs stuck in queued state with high attempt counts
func (m *NoAgentMonitor) checkAndCancelStuckJobs() {
	ctx := context.Background()

	// Get queued jobs
	queuedJobs, err := m.jobStore.GetQueued(ctx, 1000)
	if err != nil {
		m.logger.Warn("failed to get queued jobs: %v", err)
		return
	}

	if len(queuedJobs) == 0 {
		return
	}

	m.logger.Info("checking %d queued jobs for auto-cancel (threshold: %d attempts)", len(queuedJobs), m.maxRetries)

	var jobsToCancel []*db.Job
	for _, job := range queuedJobs {
		m.logger.Info("job %s (labels: %v) has %d attempts (max: %d)", job.ID, job.Labels, job.AttemptCount, m.maxRetries)
		if job.AttemptCount >= m.maxRetries {
			jobsToCancel = append(jobsToCancel, job)
		}
	}

	if len(jobsToCancel) == 0 {
		return
	}

	m.logger.Info("canceling %d jobs that exceeded retry threshold", len(jobsToCancel))

	// Cancel all jobs in database first
	var cancelledJobs []*db.Job
	for _, job := range jobsToCancel {
		m.logger.Info("canceling job %s after %d failed attempts", job.ID, job.AttemptCount)

		// Format labels for error message
		labelsStr := m.formatLabels(job.Labels)

		errorMsg := fmt.Sprintf(
			"Job cancelled: No agents available with required labels: %s. "+
				"Job failed to find a matching agent after %d retry attempts over %d minutes.",
			labelsStr,
			job.AttemptCount,
			(m.maxRetries*int(m.checkInterval.Seconds()))/60,
		)

		// Mark job as failed in database
		if err := m.jobStore.UpdateResult(
			ctx,
			job.ID,
			db.JobStatusFailed,
			"",  // no output
			nil, // no plan data
			1,   // exit code 1
			errorMsg,
		); err != nil {
			m.logger.Warn("failed to update job %s status: %v", job.ID, err)
			continue
		}

		cancelledJobs = append(cancelledJobs, job)
	}

	if len(cancelledJobs) == 0 {
		m.logger.Warn("no jobs were successfully cancelled")
		return
	}

	// Group cancelled jobs by repo and PR for batched comments
	type prKey struct {
		repoFullName string
		pullNum      int
	}
	jobsByPR := make(map[prKey][]*db.Job)
	for _, job := range cancelledJobs {
		key := prKey{repoFullName: job.RepoFullName, pullNum: job.PullNum}
		jobsByPR[key] = append(jobsByPR[key], job)
	}

	// Post one batched comment per PR
	for key, jobs := range jobsByPR {
		if m.resultUpdater != nil {
			if err := m.resultUpdater.PostBatchJobFailureComment(
				m.logger,
				key.repoFullName,
				key.pullNum,
				jobs,
			); err != nil {
				m.logger.Warn("failed to post batch failure comment for PR %s#%d: %v", key.repoFullName, key.pullNum, err)
			} else {
				m.logger.Info("posted batch failure comment for %d jobs on PR %s#%d", len(jobs), key.repoFullName, key.pullNum)
			}
		}
	}

	m.logger.Info("cancelled %d jobs in this check", len(cancelledJobs))
}

// formatLabels formats labels map as a readable string
func (m *NoAgentMonitor) formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}

	// Use JSON formatting for consistency
	data, err := json.Marshal(labels)
	if err != nil {
		return fmt.Sprintf("%v", labels)
	}
	return string(data)
}
