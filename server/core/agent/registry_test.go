package agent_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockAgentStore is a mock implementation of db.AgentStore for testing
type MockAgentStore struct {
	agents map[string]*db.AgentController
	tokens map[string]string
}

func NewMockAgentStore() *MockAgentStore {
	return &MockAgentStore{
		agents: make(map[string]*db.AgentController),
		tokens: make(map[string]string),
	}
}

func (m *MockAgentStore) Register(ctx context.Context, agent *db.AgentController, token string) error {
	if agent.ID == "" {
		agent.ID = "agent-" + agent.Name
	}
	m.agents[agent.ID] = agent
	m.tokens[agent.ID] = token
	return nil
}

func (m *MockAgentStore) Get(ctx context.Context, id string) (*db.AgentController, error) {
	agent, exists := m.agents[id]
	if !exists {
		return nil, sql.ErrNoRows
	}
	return agent, nil
}

func (m *MockAgentStore) GetByName(ctx context.Context, name string) (*db.AgentController, error) {
	for _, agent := range m.agents {
		if agent.Name == name {
			return agent, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (m *MockAgentStore) Authenticate(ctx context.Context, controllerID, token string) (bool, error) {
	storedToken, exists := m.tokens[controllerID]
	if !exists {
		return false, nil
	}
	return storedToken == token, nil
}

func (m *MockAgentStore) UpdateHeartbeat(ctx context.Context, controllerID string, currentJobs int) error {
	agent, exists := m.agents[controllerID]
	if !exists {
		return sql.ErrNoRows
	}
	agent.CurrentJobs = currentJobs
	return nil
}

func (m *MockAgentStore) UpdateStatus(ctx context.Context, controllerID string, status db.AgentStatus) error {
	agent, exists := m.agents[controllerID]
	if !exists {
		return sql.ErrNoRows
	}
	agent.Status = string(status)
	return nil
}

func (m *MockAgentStore) UpdateCapacity(ctx context.Context, controllerID string, capacity int) error {
	return nil
}

func (m *MockAgentStore) IncrementJobs(ctx context.Context, controllerID string) error {
	return nil
}

func (m *MockAgentStore) DecrementJobs(ctx context.Context, controllerID string) error {
	return nil
}

func (m *MockAgentStore) GetAvailable(ctx context.Context, labels map[string]string, limit int) ([]*db.AgentController, error) {
	var available []*db.AgentController
	for _, agent := range m.agents {
		if agent.Status == "active" && agent.CurrentJobs < agent.Capacity {
			available = append(available, agent)
		}
	}
	return available, nil
}

func (m *MockAgentStore) MarkStaleAsUnhealthy(ctx context.Context, thresholdSeconds int) (int, error) {
	return 0, nil
}

func (m *MockAgentStore) ListAll(ctx context.Context) ([]*db.AgentController, error) {
	var agents []*db.AgentController
	for _, agent := range m.agents {
		agents = append(agents, agent)
	}
	return agents, nil
}

func (m *MockAgentStore) Delete(ctx context.Context, controllerID string) error {
	delete(m.agents, controllerID)
	delete(m.tokens, controllerID)
	return nil
}

func TestRegistry_Register(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	ctx := context.Background()

	t.Run("successfully registers new agent", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:            "test-agent",
			Token:           "test-token-with-at-least-32-chars-12345",
			ClusterName:     "test-cluster",
			Namespace:       "atlantis",
			Capacity:        10,
			AtlantisVersion: "1.0.0",
			Labels: map[string]string{
				"env": "test",
			},
		}

		agentCtrl, err := registry.Register(ctx, config)
		require.NoError(t, err)
		require.NotNil(t, agentCtrl)

		assert.Equal(t, "test-agent", agentCtrl.Name)
		assert.Equal(t, "test-cluster", agentCtrl.ClusterName)
		assert.Equal(t, "atlantis", agentCtrl.Namespace)
		assert.Equal(t, 10, agentCtrl.Capacity)
		assert.Equal(t, "active", agentCtrl.Status)
		assert.Equal(t, 0, agentCtrl.CurrentJobs)
	})

	t.Run("fails with invalid config", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:  "",
			Token: "short",
		}

		agentCtrl, err := registry.Register(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, agentCtrl)
	})

	t.Run("fails when agent name already exists", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:            "duplicate-agent",
			Token:           "test-token-with-at-least-32-chars-12345",
			ClusterName:     "test-cluster",
			Namespace:       "atlantis",
			Capacity:        10,
			AtlantisVersion: "1.0.0",
		}

		// Register first time
		_, err := registry.Register(ctx, config)
		require.NoError(t, err)

		// Try to register again with same name
		_, err = registry.Register(ctx, config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})
}

func TestRegistry_Authenticate(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	ctx := context.Background()

	// Register an agent first
	config := agent.AgentConfig{
		Name:            "auth-test-agent",
		Token:           "correct-token-with-32-chars-123456",
		ClusterName:     "test-cluster",
		Namespace:       "atlantis",
		Capacity:        10,
		AtlantisVersion: "1.0.0",
	}

	agentCtrl, err := registry.Register(ctx, config)
	require.NoError(t, err)

	t.Run("authenticates with correct token", func(t *testing.T) {
		authenticated, err := registry.Authenticate(ctx, agentCtrl.ID, "correct-token-with-32-chars-123456")
		require.NoError(t, err)
		assert.True(t, authenticated)
	})

	t.Run("fails with incorrect token", func(t *testing.T) {
		authenticated, err := registry.Authenticate(ctx, agentCtrl.ID, "wrong-token")
		require.NoError(t, err)
		assert.False(t, authenticated)
	})

	t.Run("fails with non-existent agent ID", func(t *testing.T) {
		authenticated, err := registry.Authenticate(ctx, "non-existent-id", "any-token")
		require.NoError(t, err)
		assert.False(t, authenticated)
	})
}

func TestRegistry_UpdateHeartbeat(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	ctx := context.Background()

	// Register an agent
	config := agent.AgentConfig{
		Name:            "heartbeat-agent",
		Token:           "test-token-with-at-least-32-chars-12345",
		ClusterName:     "test-cluster",
		Namespace:       "atlantis",
		Capacity:        10,
		AtlantisVersion: "1.0.0",
	}

	agentCtrl, err := registry.Register(ctx, config)
	require.NoError(t, err)

	t.Run("updates heartbeat successfully", func(t *testing.T) {
		err := registry.UpdateHeartbeat(ctx, agentCtrl.ID, 3)
		require.NoError(t, err)

		// Verify current jobs updated
		agent, err := store.Get(ctx, agentCtrl.ID)
		require.NoError(t, err)
		assert.Equal(t, 3, agent.CurrentJobs)
	})
}

func TestRegistry_SetStatus(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	ctx := context.Background()

	// Register an agent
	config := agent.AgentConfig{
		Name:            "status-agent",
		Token:           "test-token-with-at-least-32-chars-12345",
		ClusterName:     "test-cluster",
		Namespace:       "atlantis",
		Capacity:        10,
		AtlantisVersion: "1.0.0",
	}

	agentCtrl, err := registry.Register(ctx, config)
	require.NoError(t, err)

	t.Run("updates status successfully", func(t *testing.T) {
		err := registry.SetStatus(ctx, agentCtrl.ID, agent.AgentStatusDraining)
		require.NoError(t, err)

		agent, err := store.Get(ctx, agentCtrl.ID)
		require.NoError(t, err)
		assert.Equal(t, "draining", agent.Status)
	})
}

func TestRegistry_Deregister(t *testing.T) {
	store := NewMockAgentStore()
	logger := logging.NewNoopLogger(t)
	registry := agent.NewRegistry(store, logger)
	ctx := context.Background()

	t.Run("deregisters agent with no jobs", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:            "dereg-agent",
			Token:           "test-token-with-at-least-32-chars-12345",
			ClusterName:     "test-cluster",
			Namespace:       "atlantis",
			Capacity:        10,
			AtlantisVersion: "1.0.0",
		}

		agentCtrl, err := registry.Register(ctx, config)
		require.NoError(t, err)

		err = registry.Deregister(ctx, agentCtrl.ID)
		require.NoError(t, err)

		// Verify agent deleted
		_, err = store.Get(ctx, agentCtrl.ID)
		assert.Error(t, err)
	})

	t.Run("fails to deregister agent with running jobs", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:            "busy-agent",
			Token:           "test-token-with-at-least-32-chars-12345",
			ClusterName:     "test-cluster",
			Namespace:       "atlantis",
			Capacity:        10,
			AtlantisVersion: "1.0.0",
		}

		agentCtrl, err := registry.Register(ctx, config)
		require.NoError(t, err)

		// Set current jobs
		err = registry.UpdateHeartbeat(ctx, agentCtrl.ID, 5)
		require.NoError(t, err)

		// Try to deregister
		err = registry.Deregister(ctx, agentCtrl.ID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "running jobs")
	})
}

func TestAgentConfig_Validate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:        "test-agent",
			Token:       "test-token-with-at-least-32-chars-12345",
			ClusterName: "test-cluster",
			Capacity:    10,
		}

		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("empty name fails", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:        "",
			Token:       "test-token-with-at-least-32-chars-12345",
			ClusterName: "test-cluster",
			Capacity:    10,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "name")
	})

	t.Run("short token fails", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:        "test-agent",
			Token:       "short",
			ClusterName: "test-cluster",
			Capacity:    10,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "32 characters")
	})

	t.Run("zero capacity fails", func(t *testing.T) {
		config := agent.AgentConfig{
			Name:        "test-agent",
			Token:       "test-token-with-at-least-32-chars-12345",
			ClusterName: "test-cluster",
			Capacity:    0,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "capacity")
	})
}
