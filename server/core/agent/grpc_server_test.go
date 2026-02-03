package agent_test

import (
	"context"
	"database/sql"
	"io"
	"sync"
	"testing"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/jobs"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MockScheduler implements scheduler.Scheduler for testing
type MockScheduler struct{}

func (m *MockScheduler) ScheduleJob(job *jobs.Job) error {
	return nil
}

func (m *MockScheduler) AssignNextJob() (*jobs.Job, *db.AgentController, error) {
	return nil, nil, nil
}

func (m *MockScheduler) GetJobStatus(jobID string) (*scheduler.JobStatus, error) {
	return &scheduler.JobStatus{JobID: jobID, Status: "queued"}, nil
}

func (m *MockScheduler) CancelJob(jobID string) error {
	return nil
}

func (m *MockScheduler) RequeueJob(jobID string) error {
	return nil
}

func (m *MockScheduler) GetQueueDepth() (int, error) {
	return 0, nil
}

func (m *MockScheduler) GetQueueDepthByLabels(labels map[string]string) (int, error) {
	return 0, nil
}

// MockJobStore implements db.JobStore for testing
type MockJobStore struct {
	jobs map[string]*db.Job
	mu   sync.Mutex
}

func NewMockJobStore() *MockJobStore {
	return &MockJobStore{
		jobs: make(map[string]*db.Job),
	}
}

func (m *MockJobStore) Create(ctx context.Context, job *db.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	return nil
}

func (m *MockJobStore) Get(ctx context.Context, id string) (*db.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, exists := m.jobs[id]
	if !exists {
		return nil, sql.ErrNoRows
	}
	return job, nil
}

func (m *MockJobStore) UpdateStatus(ctx context.Context, id string, status db.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, exists := m.jobs[id]
	if !exists {
		return sql.ErrNoRows
	}
	job.Status = string(status)
	return nil
}

func (m *MockJobStore) UpdateAgent(ctx context.Context, id, agentID string) error {
	return nil
}

func (m *MockJobStore) AssignToAgent(ctx context.Context, jobID, agentID string) error {
	return nil
}

func (m *MockJobStore) List(ctx context.Context, filters map[string]string, limit int) ([]*db.Job, error) {
	return nil, nil
}

func (m *MockJobStore) Delete(ctx context.Context, id string) error {
	return nil
}

func (m *MockJobStore) GetQueued(ctx context.Context, limit int) ([]*db.Job, error) {
	return nil, nil
}

func (m *MockJobStore) UpdateAgentPod(ctx context.Context, jobID, podName string) error {
	return nil
}

func (m *MockJobStore) SavePlan(ctx context.Context, jobID string, plan []byte) error {
	return nil
}

func (m *MockJobStore) GetPlan(ctx context.Context, jobID string) ([]byte, error) {
	return nil, nil
}

func (m *MockJobStore) DeletePlan(ctx context.Context, jobID string) error {
	return nil
}

func (m *MockJobStore) SetOutput(ctx context.Context, jobID string, output string) error {
	return nil
}

func (m *MockJobStore) SetError(ctx context.Context, jobID string, errorMsg string, exitCode int) error {
	return nil
}

func (m *MockJobStore) Complete(ctx context.Context, jobID string, status db.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, exists := m.jobs[jobID]
	if !exists {
		return sql.ErrNoRows
	}
	job.Status = string(status)
	return nil
}

func (m *MockJobStore) ListByRepo(ctx context.Context, repoFullName string, pullNum int) ([]*db.Job, error) {
	return nil, nil
}

func (m *MockJobStore) UpdateResult(ctx context.Context, id string, status db.JobStatus, output string, planData []byte, exitCode int, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, exists := m.jobs[id]
	if !exists {
		return sql.ErrNoRows
	}
	job.Status = string(status)
	job.Output = &output
	job.ExitCode = &exitCode
	job.ErrorMessage = &errorMsg
	return nil
}

// MockStream implements proto.AgentService_StreamJobsServer for testing
type MockStream struct {
	ctx      context.Context
	sent     []*proto.JobAssignment
	received []*proto.AgentMessage
	recvIdx  int
	mu       sync.Mutex
}

func NewMockStream(ctx context.Context) *MockStream {
	return &MockStream{
		ctx:      ctx,
		sent:     make([]*proto.JobAssignment, 0),
		received: make([]*proto.AgentMessage, 0),
	}
}

func (m *MockStream) Send(assignment *proto.JobAssignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, assignment)
	return nil
}

func (m *MockStream) Recv() (*proto.AgentMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.recvIdx >= len(m.received) {
		return nil, io.EOF
	}

	msg := m.received[m.recvIdx]
	m.recvIdx++
	return msg, nil
}

func (m *MockStream) AddMessage(msg *proto.AgentMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, msg)
}

func (m *MockStream) Context() context.Context {
	return m.ctx
}

func (m *MockStream) SetHeader(md metadata.MD) error {
	return nil
}

func (m *MockStream) SendHeader(md metadata.MD) error {
	return nil
}

func (m *MockStream) SetTrailer(md metadata.MD) {
}

func (m *MockStream) SendMsg(msg interface{}) error {
	return nil
}

func (m *MockStream) RecvMsg(msg interface{}) error {
	return nil
}

func TestGRPCServer_ReportJobStatus(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	scheduler := &MockScheduler{}
	jobStore := NewMockJobStore()

	server := agent.NewGRPCServer(registry, scheduler, jobStore, "test-token", logger)
	ctx := context.Background()

	// Create a job
	job := &db.Job{
		ID:     "job-123",
		Status: "running",
	}
	err := jobStore.Create(ctx, job)
	require.NoError(t, err)

	t.Run("updates job status", func(t *testing.T) {
		update := &proto.JobStatusUpdate{
			JobId:        "job-123",
			Status:       "completed",
			AgentPodName: "agent-pod-1",
		}

		ack, err := server.ReportJobStatus(ctx, update)
		require.NoError(t, err)
		assert.True(t, ack.Success)

		// Verify status updated
		updatedJob, err := jobStore.Get(ctx, "job-123")
		require.NoError(t, err)
		assert.Equal(t, "completed", updatedJob.Status)
	})
}

func TestGRPCServer_GetJob(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	scheduler := &MockScheduler{}
	jobStore := NewMockJobStore()

	server := agent.NewGRPCServer(registry, scheduler, jobStore, "test-token", logger)
	ctx := context.Background()

	// Create a job
	job := &db.Job{
		ID:           "job-456",
		RepoFullName: "owner/repo",
		PullNum:      123,
		Command:      "plan",
		Workspace:    "default",
		ProjectName:  "myproject",
		ProjectDir:   ".",
		Status:       "queued",
	}
	err := jobStore.Create(ctx, job)
	require.NoError(t, err)

	t.Run("retrieves job details", func(t *testing.T) {
		req := &proto.JobRequest{
			JobId: "job-456",
		}

		details, err := server.GetJob(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, "job-456", details.Assignment.JobId)
		assert.Equal(t, "owner/repo", details.Assignment.RepoFullName)
		assert.Equal(t, int32(123), details.Assignment.PullNum)
		assert.Equal(t, "queued", details.Status)
	})

	t.Run("returns not found for non-existent job", func(t *testing.T) {
		req := &proto.JobRequest{
			JobId: "non-existent",
		}

		_, err := server.GetJob(ctx, req)
		assert.Error(t, err)
		statusErr, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, statusErr.Code())
	})
}

func TestGRPCServer_AssignJobToAgent(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	scheduler := &MockScheduler{}
	jobStore := NewMockJobStore()

	server := agent.NewGRPCServer(registry, scheduler, jobStore, "test-token", logger)

	t.Run("fails for non-connected agent", func(t *testing.T) {
		assignment := &proto.JobAssignment{
			JobId: "job-123",
		}

		err := server.AssignJobToAgent("non-existent", assignment)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected")
	})
}

func TestGRPCServer_GetConnectedAgents(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	scheduler := &MockScheduler{}
	jobStore := NewMockJobStore()

	server := agent.NewGRPCServer(registry, scheduler, jobStore, "test-token", logger)

	t.Run("returns empty list initially", func(t *testing.T) {
		agents := server.GetConnectedAgents()
		assert.Empty(t, agents)
	})
}
