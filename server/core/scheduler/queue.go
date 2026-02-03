package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// JobQueue manages pending jobs with priority ordering
type JobQueue interface {
	// Enqueue adds a job to the queue
	Enqueue(job *db.Job) error
	// Dequeue removes and returns the highest priority job
	Dequeue() (*db.Job, error)
	// Peek returns the highest priority job without removing it
	Peek() (*db.Job, error)
	// Remove removes a specific job from the queue
	Remove(jobID string) error
	// Size returns the number of jobs in the queue
	Size() int
	// GetAll returns all jobs in priority order
	GetAll() []*db.Job
	// Clear removes all jobs from the queue
	Clear()
}

// PriorityJobQueue implements JobQueue with priority ordering
type PriorityJobQueue struct {
	mu     sync.RWMutex
	heap   *jobHeap
	index  map[string]*heapItem // jobID -> heapItem for fast lookups
	logger logging.SimpleLogging
}

// heapItem wraps a job with heap metadata
type heapItem struct {
	job      *db.Job
	index    int       // index in heap
	priority int       // higher number = higher priority
	addedAt  time.Time // for FIFO within same priority
}

// jobHeap implements heap.Interface
type jobHeap []*heapItem

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	// Higher priority comes first
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	// For same priority, FIFO (earlier addedAt comes first)
	return h[i].addedAt.Before(h[j].addedAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *jobHeap) Push(x interface{}) {
	item := x.(*heapItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // mark as removed
	*h = old[0 : n-1]
	return item
}

// NewPriorityJobQueue creates a new priority-based job queue
func NewPriorityJobQueue(logger logging.SimpleLogging) *PriorityJobQueue {
	h := &jobHeap{}
	heap.Init(h)
	return &PriorityJobQueue{
		heap:   h,
		index:  make(map[string]*heapItem),
		logger: logger,
	}
}

// Enqueue adds a job to the queue
func (q *PriorityJobQueue) Enqueue(job *db.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if job == nil {
		return fmt.Errorf("cannot enqueue nil job")
	}

	if _, exists := q.index[job.ID]; exists {
		return fmt.Errorf("job %s already in queue", job.ID)
	}

	item := &heapItem{
		job:      job,
		priority: q.calculatePriority(job),
		addedAt:  time.Now(),
	}

	heap.Push(q.heap, item)
	q.index[job.ID] = item

	q.logger.Debug("enqueued job %s with priority %d (queue size: %d)", job.ID, item.priority, len(*q.heap))
	return nil
}

// Dequeue removes and returns the highest priority job
func (q *PriorityJobQueue) Dequeue() (*db.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(*q.heap) == 0 {
		return nil, fmt.Errorf("queue is empty")
	}

	item := heap.Pop(q.heap).(*heapItem)
	delete(q.index, item.job.ID)

	q.logger.Debug("dequeued job %s (remaining: %d)", item.job.ID, len(*q.heap))
	return item.job, nil
}

// Peek returns the highest priority job without removing it
func (q *PriorityJobQueue) Peek() (*db.Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if len(*q.heap) == 0 {
		return nil, fmt.Errorf("queue is empty")
	}

	return (*q.heap)[0].job, nil
}

// Remove removes a specific job from the queue
func (q *PriorityJobQueue) Remove(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	item, exists := q.index[jobID]
	if !exists {
		return fmt.Errorf("job %s not found in queue", jobID)
	}

	heap.Remove(q.heap, item.index)
	delete(q.index, jobID)

	q.logger.Debug("removed job %s from queue (remaining: %d)", jobID, len(*q.heap))
	return nil
}

// Size returns the number of jobs in the queue
func (q *PriorityJobQueue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(*q.heap)
}

// GetAll returns all jobs in priority order
func (q *PriorityJobQueue) GetAll() []*db.Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*db.Job, len(*q.heap))
	for i, item := range *q.heap {
		jobs[i] = item.job
	}
	return jobs
}

// Clear removes all jobs from the queue
func (q *PriorityJobQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.heap = &jobHeap{}
	heap.Init(q.heap)
	q.index = make(map[string]*heapItem)
	q.logger.Debug("cleared job queue")
}

// calculatePriority determines job priority based on multiple factors
func (q *PriorityJobQueue) calculatePriority(job *db.Job) int {
	priority := 0

	// Base priority from job.Priority (0-100)
	priority += job.Priority

	// Command type priority: apply > plan > unlock
	switch job.Command {
	case "apply":
		priority += 30
	case "plan":
		priority += 10
	case "unlock":
		priority += 5
	}

	// Age factor: older jobs get slight boost (up to 10 points)
	age := time.Since(job.CreatedAt)
	ageMinutes := int(age.Minutes())
	if ageMinutes > 60 {
		ageMinutes = 60 // cap at 1 hour
	}
	priority += ageMinutes / 6 // +1 every 6 minutes, max +10

	return priority
}

// JobQueueMonitor monitors queue metrics and handles overflow
type JobQueueMonitor struct {
	queue      JobQueue
	jobStore   db.JobStore
	maxSize    int
	logger     logging.SimpleLogging
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	checkEvery time.Duration
}

// JobQueueMonitorConfig configuration for queue monitor
type JobQueueMonitorConfig struct {
	MaxQueueSize        int
	CheckInterval       time.Duration
	OverflowAction      string // "reject" or "drop-oldest"
	EnableMetrics       bool
	EnableOverflowAlert bool
}

// NewJobQueueMonitor creates a new queue monitor
func NewJobQueueMonitor(queue JobQueue, jobStore db.JobStore, config JobQueueMonitorConfig, logger logging.SimpleLogging) *JobQueueMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.MaxQueueSize == 0 {
		config.MaxQueueSize = 1000
	}

	return &JobQueueMonitor{
		queue:      queue,
		jobStore:   jobStore,
		maxSize:    config.MaxQueueSize,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		checkEvery: config.CheckInterval,
	}
}

// Start begins monitoring the queue
func (m *JobQueueMonitor) Start() {
	m.wg.Add(1)
	go m.monitorLoop()
	m.logger.Info("job queue monitor started (max size: %d)", m.maxSize)
}

// Stop stops monitoring
func (m *JobQueueMonitor) Stop() {
	m.cancel()
	m.wg.Wait()
	m.logger.Info("job queue monitor stopped")
}

// monitorLoop periodically checks queue health
func (m *JobQueueMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.checkEvery)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkQueueHealth()
		}
	}
}

// checkQueueHealth monitors queue metrics
func (m *JobQueueMonitor) checkQueueHealth() {
	size := m.queue.Size()

	if size > 0 {
		m.logger.Debug("queue health check: %d jobs pending", size)
	}

	if size > m.maxSize {
		m.logger.Warn("queue overflow: %d jobs (max: %d)", size, m.maxSize)
		m.handleOverflow(size - m.maxSize)
	}

	// Log queue utilization percentage
	utilization := (float64(size) / float64(m.maxSize)) * 100
	if utilization > 80 {
		m.logger.Warn("queue utilization high: %.1f%% (%d/%d)", utilization, size, m.maxSize)
	}
}

// handleOverflow handles queue overflow by dropping oldest jobs
func (m *JobQueueMonitor) handleOverflow(excess int) {
	m.logger.Warn("handling queue overflow: dropping %d oldest jobs", excess)

	jobs := m.queue.GetAll()
	if len(jobs) == 0 {
		return
	}

	// Remove oldest jobs (at the end after sorting)
	for i := 0; i < excess && i < len(jobs); i++ {
		job := jobs[len(jobs)-1-i]
		if err := m.queue.Remove(job.ID); err != nil {
			m.logger.Err("failed to remove overflow job %s: %v", job.ID, err)
			continue
		}

		// Update job status to failed
		if err := m.jobStore.UpdateStatus(m.ctx, job.ID, db.JobStatusFailed); err != nil {
			m.logger.Err("failed to update dropped job %s: %v", job.ID, err)
			continue
		}

		// Store error message
		errMsg := "dropped due to queue overflow"
		if err := m.jobStore.SetError(m.ctx, job.ID, errMsg, 0); err != nil {
			m.logger.Err("failed to set error for dropped job %s: %v", job.ID, err)
		}

		m.logger.Info("dropped job %s due to queue overflow", job.ID)
	}
}
