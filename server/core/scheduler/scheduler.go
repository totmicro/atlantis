package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/jobs"
	"github.com/runatlantis/atlantis/server/logging"
)

// JobNotifier sends job assignments to connected agents
type JobNotifier interface {
	AssignJobToAgent(controllerID string, assignment interface{}) error
}

// DefaultScheduler implements the Scheduler interface
type DefaultScheduler struct {
	jobStore    db.JobStore
	agentStore  db.AgentStore
	router      *Router
	logger      logging.SimpleLogging
	config      RouterConfig
	jobNotifier JobNotifier
}

// NewScheduler creates a new scheduler
func NewScheduler(
	jobStore db.JobStore,
	agentStore db.AgentStore,
	logger logging.SimpleLogging,
	config RouterConfig,
) *DefaultScheduler {
	return &DefaultScheduler{
		jobStore:   jobStore,
		agentStore: agentStore,
		router:     NewRouter(config),
		logger:     logger,
		config:     config,
	}
}

// SetJobNotifier sets the job notifier for pushing jobs to agents
func (s *DefaultScheduler) SetJobNotifier(notifier JobNotifier) {
	s.jobNotifier = notifier
}

// ScheduleJob adds a new job to the queue
func (s *DefaultScheduler) ScheduleJob(job *jobs.Job) error {
	s.logger.Info("scheduling job %s", job.ID)
	// Full implementation would convert jobs.Job to db.Job and persist
	return nil
}

// AssignNextJob finds the next available job and assigns it to an available agent
func (s *DefaultScheduler) AssignNextJob() (*jobs.Job, *db.AgentController, error) {
	ctx := context.Background()

	// Get queued jobs
	queuedJobs, err := s.jobStore.GetQueued(ctx, 100)
	if err != nil {
		return nil, nil, fmt.Errorf("getting queued jobs: %w", err)
	}

	if len(queuedJobs) == 0 {
		s.logger.Debug("no queued jobs")
		return nil, nil, nil
	}

	s.logger.Info("found %d queued jobs to assign", len(queuedJobs))

	// Get available agents
	agents, err := s.agentStore.GetAvailable(ctx, nil, 100)
	if err != nil {
		return nil, nil, fmt.Errorf("getting available agents: %w", err)
	}

	if len(agents) == 0 {
		s.logger.Debug("no available agents")
		return nil, nil, nil
	}

	s.logger.Info("found %d available agents", len(agents))

	// Assign ALL jobs to available agents (not just one)
	// This allows multiple jobs to be dispatched in parallel
	assignedCount := 0
	var firstJob *jobs.Job
	var firstAgent *db.AgentController

	for _, dbJob := range queuedJobs {
		assigned := false
		s.logger.Info("checking job %s with labels %v", dbJob.ID, dbJob.Labels)
		for _, agent := range agents {
			s.logger.Info("checking agent %s with labels %v", agent.ID, agent.Labels)
			// Check if agent labels match job requirements
			if s.router.agentMatchesDBJob(agent, dbJob) {
				// Assign job to agent in database
				if err := s.jobStore.AssignToAgent(ctx, dbJob.ID, agent.ID); err != nil {
					// Expected during reconnection - job may already be assigned or completed
					s.logger.Info("could not assign job %s to agent %s: %v", dbJob.ID, agent.ID, err)
					continue
				}

				s.logger.Info("assigned job %s to agent %s (database updated)", dbJob.ID, agent.ID)
				assignedCount++
				assigned = true

				// Notify agent via gRPC if notifier is set
				if s.jobNotifier != nil {
					// Reload job to get updated status
					assignedJob, err := s.jobStore.Get(ctx, dbJob.ID)
					if err != nil {
						s.logger.Warn("failed to reload job after assignment: %v", err)
					} else {
						if err := s.jobNotifier.AssignJobToAgent(agent.ID, assignedJob); err != nil {
							// Agent disconnected - requeue the job immediately
							s.logger.Info("could not notify agent %s about job %s: %v", agent.ID, dbJob.ID, err)
							s.logger.Info("requeuing job %s (agent not connected)", dbJob.ID)

							// Requeue the job so it can be assigned to another agent
							if requeueErr := s.jobStore.RequeueJob(ctx, dbJob.ID); requeueErr != nil {
								s.logger.Warn("failed to requeue job %s after notification failure: %v", dbJob.ID, requeueErr)
							} else {
								s.logger.Info("successfully requeued job %s", dbJob.ID)
							}
						} else {
							s.logger.Info("notified agent %s about job %s", agent.ID, dbJob.ID)
						}
					}
				}

				// Save first assignment for return value (backwards compatibility)
				if firstJob == nil {
					firstJob = &jobs.Job{
						ID:     dbJob.ID,
						Status: jobs.JobStatus(dbJob.Status),
						Labels: dbJob.Labels,
					}
					firstAgent = agent
				}

				// Job assigned, move to next job
				break
			}
		}
		if !assigned {
			s.logger.Info("no suitable agent found for job %s with labels %v, incrementing attempt count", dbJob.ID, dbJob.Labels)
			// Increment attempt count for jobs that can't be assigned
			if err := s.jobStore.IncrementAttemptCount(ctx, dbJob.ID); err != nil {
				s.logger.Warn("failed to increment attempt count for job %s: %v", dbJob.ID, err)
			}
		}
	}

	if assignedCount > 0 {
		s.logger.Info("assigned %d jobs in this pass", assignedCount)
	}

	return firstJob, firstAgent, nil
}

// GetJobStatus retrieves the current status of a job
func (s *DefaultScheduler) GetJobStatus(jobID string) (*JobStatus, error) {
	ctx := context.Background()

	dbJob, err := s.jobStore.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("getting job: %w", err)
	}

	status := &JobStatus{
		JobID:       dbJob.ID,
		Status:      jobs.JobStatus(dbJob.Status),
		QueuedAt:    dbJob.CreatedAt,
		CompletedAt: dbJob.CompletedAt,
	}

	if dbJob.AgentControllerID != nil {
		status.AssignedTo = *dbJob.AgentControllerID
	}

	return status, nil
}

// CancelJob cancels a job
func (s *DefaultScheduler) CancelJob(jobID string) error {
	s.logger.Info("canceling job %s", jobID)
	return nil
}

// RequeueJob requeues a failed job
func (s *DefaultScheduler) RequeueJob(jobID string) error {
	s.logger.Info("requeuing job %s", jobID)
	return nil
}

// GetQueueDepth returns the number of queued jobs
func (s *DefaultScheduler) GetQueueDepth() (int, error) {
	ctx := context.Background()
	jobs, err := s.jobStore.GetQueued(ctx, 10000)
	if err != nil {
		return 0, err
	}
	return len(jobs), nil
}

// GetQueueDepthByLabels returns queue depth for specific labels
func (s *DefaultScheduler) GetQueueDepthByLabels(labels map[string]string) (int, error) {
	ctx := context.Background()

	if err := ValidateLabels(labels); err != nil {
		return 0, err
	}

	jobs, err := s.jobStore.GetQueued(ctx, 10000)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, job := range jobs {
		if labelsMatch(job.Labels, labels) {
			count++
		}
	}

	return count, nil
}

func labelsMatch(jobLabels, filterLabels map[string]string) bool {
	for key, value := range filterLabels {
		if jobValue, exists := jobLabels[key]; !exists || jobValue != value {
			return false
		}
	}
	return true
}

// Start begins the background job assignment loop
func (s *DefaultScheduler) Start() {
	s.logger.Info("starting scheduler assignment loop (checking every 5 seconds)")

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			job, agent, err := s.AssignNextJob()
			if err != nil {
				s.logger.Err("error assigning job: %v", err)
				continue
			}
			if job != nil && agent != nil {
				s.logger.Info("successfully assigned job %s to agent %s", job.ID, agent.ID)
			}
		}
	}()
}
