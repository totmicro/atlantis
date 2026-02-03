package scheduler

import (
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/jobs"
)

// Scheduler handles job scheduling and assignment to agents
type Scheduler interface {
	// ScheduleJob adds a new job to the queue
	ScheduleJob(job *jobs.Job) error

	// AssignNextJob finds the next available job and assigns it to an available agent
	// Returns nil if no jobs or agents available
	AssignNextJob() (*jobs.Job, *db.AgentController, error)

	// GetJobStatus retrieves the current status of a job
	GetJobStatus(jobID string) (*JobStatus, error)

	// CancelJob cancels a job if it hasn't started yet
	CancelJob(jobID string) error

	// RequeueJob requeues a failed job for retry
	RequeueJob(jobID string) error

	// GetQueueDepth returns the number of queued jobs
	GetQueueDepth() (int, error)

	// GetQueueDepthByLabels returns queue depth for specific labels
	GetQueueDepthByLabels(labels map[string]string) (int, error)
}

// JobStatus represents the current state of a job in the scheduler
type JobStatus struct {
	JobID         string
	Status        jobs.JobStatus
	AssignedTo    string // Agent ID
	QueuedAt      time.Time
	AssignedAt    *time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
	Error         string
	QueuePosition int // Position in queue if still queued
}

// SchedulingPolicy defines the strategy for job assignment
type SchedulingPolicy int

const (
	// PolicyRoundRobin assigns jobs to agents in round-robin fashion
	PolicyRoundRobin SchedulingPolicy = iota

	// PolicyLeastLoaded assigns jobs to agents with least current load
	PolicyLeastLoaded

	// PolicyPriorityFirst prioritizes high-priority jobs
	PolicyPriorityFirst
)

// RouterConfig configures the job routing behavior
type RouterConfig struct {
	// Policy determines the scheduling strategy
	Policy SchedulingPolicy

	// MaxRetries is the maximum number of times to retry assignment
	MaxRetries int

	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration

	// EnableStrictLabelMatching requires all labels to match
	EnableStrictLabelMatching bool
}

// DefaultRouterConfig returns sensible defaults
func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		Policy:                    PolicyPriorityFirst,
		MaxRetries:                3,
		RetryDelay:                5 * time.Second,
		EnableStrictLabelMatching: false,
	}
}
