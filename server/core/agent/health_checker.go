package agent

import (
	"context"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
)

// HealthChecker monitors agent health and marks stale agents as unhealthy
type HealthChecker struct {
	agentStore     db.AgentStore
	logger         logging.SimpleLogging
	checkInterval  time.Duration
	staleThreshold time.Duration
	stopChan       chan struct{}
	stoppedChan    chan struct{}
	running        bool
	mu             sync.Mutex
}

// HealthCheckConfig configures the health checker
type HealthCheckConfig struct {
	// CheckInterval is how often to check for stale agents
	CheckInterval time.Duration

	// StaleThreshold is how long without a heartbeat before marking unhealthy
	StaleThreshold time.Duration
}

// DefaultHealthCheckConfig returns sensible defaults
func DefaultHealthCheckConfig() HealthCheckConfig {
	return HealthCheckConfig{
		CheckInterval:  30 * time.Second,
		StaleThreshold: 2 * time.Minute,
	}
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(
	agentStore db.AgentStore,
	logger logging.SimpleLogging,
	config HealthCheckConfig,
) *HealthChecker {
	return &HealthChecker{
		agentStore:     agentStore,
		logger:         logger,
		checkInterval:  config.CheckInterval,
		staleThreshold: config.StaleThreshold,
		stopChan:       make(chan struct{}),
		stoppedChan:    make(chan struct{}),
	}
}

// Start begins the health checking loop
func (hc *HealthChecker) Start() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.running {
		hc.logger.Warn("health checker already running")
		return
	}

	hc.running = true
	hc.logger.Info("starting agent health checker (interval: %s, threshold: %s)",
		hc.checkInterval, hc.staleThreshold)

	go hc.run()
}

// Stop stops the health checker gracefully
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if !hc.running {
		return
	}

	hc.logger.Info("stopping agent health checker")
	close(hc.stopChan)
	<-hc.stoppedChan
	hc.running = false
	hc.logger.Info("agent health checker stopped")
}

// run is the main health checking loop
func (hc *HealthChecker) run() {
	defer close(hc.stoppedChan)

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	// Run initial check immediately
	hc.checkAgentHealth()

	for {
		select {
		case <-ticker.C:
			hc.checkAgentHealth()
		case <-hc.stopChan:
			return
		}
	}
}

// checkAgentHealth checks all agents and marks stale ones as unhealthy
func (hc *HealthChecker) checkAgentHealth() {
	ctx := context.Background()

	thresholdSeconds := int(hc.staleThreshold.Seconds())

	markedCount, err := hc.agentStore.MarkStaleAsUnhealthy(ctx, thresholdSeconds)
	if err != nil {
		hc.logger.Err("failed to mark stale agents as unhealthy: %s", err)
		return
	}

	if markedCount > 0 {
		hc.logger.Warn("marked %d stale agents as unhealthy", markedCount)
	} else {
		hc.logger.Debug("all agents healthy")
	}
}

// GetHealthStats returns statistics about agent health
func (hc *HealthChecker) GetHealthStats(ctx context.Context) (*HealthStats, error) {
	agents, err := hc.agentStore.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	stats := &HealthStats{}

	for _, agent := range agents {
		stats.TotalAgents++

		switch agent.Status {
		case "active":
			stats.ActiveAgents++
		case "draining":
			stats.DrainingAgents++
		case "offline":
			stats.OfflineAgents++
		case "unhealthy":
			stats.UnhealthyAgents++
		}

		// Check if agent is stale
		if time.Since(agent.LastHeartbeat) > hc.staleThreshold {
			stats.StaleAgents++
		}
	}

	return stats, nil
}

// HealthStats contains agent health statistics
type HealthStats struct {
	TotalAgents     int
	ActiveAgents    int
	DrainingAgents  int
	OfflineAgents   int
	UnhealthyAgents int
	StaleAgents     int
}
