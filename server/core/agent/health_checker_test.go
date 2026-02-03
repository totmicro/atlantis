package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/agent"
	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockAgentStoreWithHealth extends MockAgentStore with health check tracking
type MockAgentStoreWithHealth struct {
	*MockAgentStore
	markStaleCallCount int
	lastThreshold      int
}

func NewMockAgentStoreWithHealth() *MockAgentStoreWithHealth {
	return &MockAgentStoreWithHealth{
		MockAgentStore: NewMockAgentStore(),
	}
}

func (m *MockAgentStoreWithHealth) MarkStaleAsUnhealthy(ctx context.Context, thresholdSeconds int) (int, error) {
	m.markStaleCallCount++
	m.lastThreshold = thresholdSeconds

	// Mark agents as unhealthy if they haven't updated in threshold
	count := 0
	for _, agent := range m.agents {
		if agent.Status == "active" {
			agent.Status = "unhealthy"
			count++
		}
	}
	return count, nil
}

func TestHealthChecker_StartStop(t *testing.T) {
	store := NewMockAgentStoreWithHealth()
	logger := logging.NewNoopLogger(t)

	config := agent.HealthCheckConfig{
		CheckInterval:  100 * time.Millisecond,
		StaleThreshold: 5 * time.Second,
	}

	checker := agent.NewHealthChecker(store, logger, config)

	t.Run("starts and stops successfully", func(t *testing.T) {
		checker.Start()

		// Wait for at least one check cycle
		time.Sleep(150 * time.Millisecond)

		checker.Stop()

		// Verify at least one check was performed
		assert.Greater(t, store.markStaleCallCount, 0)
	})

	t.Run("can be restarted", func(t *testing.T) {
		store2 := NewMockAgentStoreWithHealth()
		checker2 := agent.NewHealthChecker(store2, logger, config)

		checker2.Start()
		time.Sleep(150 * time.Millisecond)
		checker2.Stop()

		initialCount := store2.markStaleCallCount

		// Start again
		checker2 = agent.NewHealthChecker(store2, logger, config)
		checker2.Start()
		time.Sleep(150 * time.Millisecond)
		checker2.Stop()

		// Verify more checks were performed
		assert.Greater(t, store2.markStaleCallCount, initialCount)
	})
}

func TestHealthChecker_CheckAgentHealth(t *testing.T) {
	store := NewMockAgentStoreWithHealth()
	logger := logging.NewNoopLogger(t)

	// Add some test agents
	store.agents["agent-1"] = &db.AgentController{
		ID:          "agent-1",
		Name:        "agent-1",
		Status:      "active",
		Capacity:    10,
		CurrentJobs: 0,
	}

	store.agents["agent-2"] = &db.AgentController{
		ID:          "agent-2",
		Name:        "agent-2",
		Status:      "active",
		Capacity:    10,
		CurrentJobs: 5,
	}

	config := agent.HealthCheckConfig{
		CheckInterval:  100 * time.Millisecond,
		StaleThreshold: 5 * time.Second,
	}

	checker := agent.NewHealthChecker(store, logger, config)

	t.Run("performs health checks periodically", func(t *testing.T) {
		checker.Start()

		initialCount := store.markStaleCallCount

		// Wait for multiple check cycles
		time.Sleep(250 * time.Millisecond)

		checker.Stop()

		// Should have performed at least 2 checks
		assert.GreaterOrEqual(t, store.markStaleCallCount-initialCount, 2)
	})

	t.Run("uses correct threshold", func(t *testing.T) {
		store2 := NewMockAgentStoreWithHealth()
		config2 := agent.HealthCheckConfig{
			CheckInterval:  100 * time.Millisecond,
			StaleThreshold: 120 * time.Second,
		}

		checker2 := agent.NewHealthChecker(store2, logger, config2)
		checker2.Start()

		time.Sleep(150 * time.Millisecond)

		checker2.Stop()

		// Verify correct threshold was used (120 seconds)
		assert.Equal(t, 120, store2.lastThreshold)
	})
}

func TestHealthChecker_GetHealthStats(t *testing.T) {
	store := NewMockAgentStoreWithHealth()
	logger := logging.NewNoopLogger(t)

	// Add agents with different statuses
	store.agents["agent-active"] = &db.AgentController{
		ID:          "agent-active",
		Name:        "agent-active",
		Status:      "active",
		Capacity:    10,
		CurrentJobs: 0,
	}

	store.agents["agent-draining"] = &db.AgentController{
		ID:          "agent-draining",
		Name:        "agent-draining",
		Status:      "draining",
		Capacity:    10,
		CurrentJobs: 5,
	}

	store.agents["agent-unhealthy"] = &db.AgentController{
		ID:          "agent-unhealthy",
		Name:        "agent-unhealthy",
		Status:      "unhealthy",
		Capacity:    10,
		CurrentJobs: 0,
	}

	config := agent.DefaultHealthCheckConfig()
	checker := agent.NewHealthChecker(store, logger, config)

	t.Run("returns health statistics", func(t *testing.T) {
		stats, err := checker.GetHealthStats(context.Background())
		require.NoError(t, err)

		assert.Equal(t, 3, stats.TotalAgents)
		assert.Equal(t, 1, stats.ActiveAgents)
		assert.Equal(t, 1, stats.DrainingAgents)
		assert.Equal(t, 0, stats.OfflineAgents)
		assert.Equal(t, 1, stats.UnhealthyAgents)
	})
}

func TestHealthCheckConfig_Defaults(t *testing.T) {
	config := agent.DefaultHealthCheckConfig()

	assert.Equal(t, 30*time.Second, config.CheckInterval)
	assert.Equal(t, 2*time.Minute, config.StaleThreshold)
}

func TestHealthChecker_ConcurrentAccess(t *testing.T) {
	store := NewMockAgentStoreWithHealth()
	logger := logging.NewNoopLogger(t)

	// Add multiple agents
	for i := 0; i < 10; i++ {
		store.agents[string(rune(i))] = &db.AgentController{
			ID:          string(rune(i)),
			Name:        string(rune(i)),
			Status:      "active",
			Capacity:    10,
			CurrentJobs: 0,
		}
	}

	config := agent.HealthCheckConfig{
		CheckInterval:  50 * time.Millisecond,
		StaleThreshold: 5 * time.Second,
	}

	checker := agent.NewHealthChecker(store, logger, config)

	t.Run("handles concurrent health checks and stats queries", func(t *testing.T) {
		checker.Start()

		// Query stats concurrently while health checks are running
		done := make(chan bool)
		errors := make(chan error, 5)

		for i := 0; i < 5; i++ {
			go func() {
				for {
					select {
					case <-done:
						return
					default:
						_, err := checker.GetHealthStats(context.Background())
						if err != nil {
							errors <- err
							return
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}

		// Let it run for a bit
		time.Sleep(200 * time.Millisecond)
		close(done)

		checker.Stop()

		// Check for any errors from concurrent access
		select {
		case err := <-errors:
			t.Fatalf("concurrent access error: %v", err)
		default:
			// No errors, test passed
		}
	})
}

func TestHealthChecker_StopWithoutStart(t *testing.T) {
	store := NewMockAgentStoreWithHealth()
	logger := logging.NewNoopLogger(t)
	config := agent.DefaultHealthCheckConfig()

	checker := agent.NewHealthChecker(store, logger, config)

	t.Run("stop without start does not panic", func(t *testing.T) {
		// This should handle gracefully - Stop() will try to close channel
		// but it shouldn't panic since stopChan is initialized
		checker.Stop()
	})
}
