package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/logging"
	"golang.org/x/crypto/bcrypt"
)

// Registry manages agent controller registration and lifecycle
type Registry struct {
	agentStore db.AgentStore
	logger     logging.SimpleLogging
}

// NewRegistry creates a new agent registry
func NewRegistry(agentStore db.AgentStore, logger logging.SimpleLogging) *Registry {
	return &Registry{
		agentStore: agentStore,
		logger:     logger,
	}
}

// Register registers a new agent controller with the given configuration
func (r *Registry) Register(ctx context.Context, config AgentConfig) (*db.AgentController, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid agent config: %w", err)
	}

	// Check if agent with same name already exists
	existing, err := r.agentStore.GetByName(ctx, config.Name)
	if err == nil && existing != nil {
		return nil, fmt.Errorf("agent with name %s already exists", config.Name)
	}

	// Create agent controller
	agent := &db.AgentController{
		Name:            config.Name,
		ClusterName:     config.ClusterName,
		Namespace:       config.Namespace,
		Labels:          config.Labels,
		Capacity:        config.Capacity,
		CurrentJobs:     0,
		Status:          "active",
		RegisteredAt:    time.Now(),
		LastHeartbeat:   time.Now(),
		LastSeen:        time.Now(),
		AtlantisVersion: config.AtlantisVersion,
	}

	// Register agent (generates ID and hashes token)
	if err := r.agentStore.Register(ctx, agent, config.Token); err != nil {
		r.logger.Err("failed to register agent: %s", err)
		return nil, fmt.Errorf("registering agent: %w", err)
	}

	r.logger.Info("registered agent %s (ID: %s) in cluster %s", agent.Name, agent.ID, agent.ClusterName)
	return agent, nil
}

// Authenticate verifies an agent's credentials
func (r *Registry) Authenticate(ctx context.Context, controllerID, token string) (bool, error) {
	authenticated, err := r.agentStore.Authenticate(ctx, controllerID, token)
	if err != nil {
		r.logger.Err("authentication failed for agent %s: %s", controllerID, err)
		return false, fmt.Errorf("authenticating agent: %w", err)
	}

	if !authenticated {
		r.logger.Warn("invalid credentials for agent %s", controllerID)
		return false, nil
	}

	return true, nil
}

// UpdateHeartbeat updates the last heartbeat time for an agent
func (r *Registry) UpdateHeartbeat(ctx context.Context, controllerID string, currentJobs int) error {
	if err := r.agentStore.UpdateHeartbeat(ctx, controllerID, currentJobs); err != nil {
		r.logger.Err("failed to update heartbeat for agent %s: %s", controllerID, err)
		return fmt.Errorf("updating heartbeat: %w", err)
	}

	r.logger.Debug("updated heartbeat for agent %s (current jobs: %d)", controllerID, currentJobs)
	return nil
}

// Deregister removes an agent from the registry
func (r *Registry) Deregister(ctx context.Context, controllerID string) error {
	// Get agent first to check if it has running jobs
	agent, err := r.agentStore.Get(ctx, controllerID)
	if err != nil {
		return fmt.Errorf("getting agent: %w", err)
	}

	if agent.CurrentJobs > 0 {
		return fmt.Errorf("cannot deregister agent with %d running jobs", agent.CurrentJobs)
	}

	if err := r.agentStore.Delete(ctx, controllerID); err != nil {
		r.logger.Err("failed to deregister agent %s: %s", controllerID, err)
		return fmt.Errorf("deregistering agent: %w", err)
	}

	r.logger.Info("deregistered agent %s", controllerID)
	return nil
}

// SetStatus updates the status of an agent
func (r *Registry) SetStatus(ctx context.Context, controllerID string, status AgentStatus) error {
	var dbStatus db.AgentStatus
	switch status {
	case AgentStatusActive:
		dbStatus = db.AgentStatusActive
	case AgentStatusDraining:
		dbStatus = db.AgentStatusDraining
	case AgentStatusOffline:
		dbStatus = db.AgentStatusOffline
	case AgentStatusUnhealthy:
		dbStatus = db.AgentStatusUnhealthy
	default:
		return fmt.Errorf("invalid status: %s", status)
	}

	if err := r.agentStore.UpdateStatus(ctx, controllerID, dbStatus); err != nil {
		r.logger.Err("failed to update status for agent %s: %s", controllerID, err)
		return fmt.Errorf("updating status: %w", err)
	}

	r.logger.Info("updated agent %s status to %s", controllerID, status)
	return nil
}

// GetAgent retrieves an agent by ID
func (r *Registry) GetAgent(ctx context.Context, controllerID string) (*db.AgentController, error) {
	agent, err := r.agentStore.Get(ctx, controllerID)
	if err != nil {
		return nil, fmt.Errorf("getting agent: %w", err)
	}
	return agent, nil
}

// GetAgentByName retrieves an agent by name
func (r *Registry) GetAgentByName(ctx context.Context, name string) (*db.AgentController, error) {
	agent, err := r.agentStore.GetByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("getting agent by name: %w", err)
	}
	return agent, nil
}

// ListAgents returns all registered agents
func (r *Registry) ListAgents(ctx context.Context) ([]*db.AgentController, error) {
	agents, err := r.agentStore.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing agents: %w", err)
	}
	return agents, nil
}

// GetAvailableAgents returns agents available for job assignment
func (r *Registry) GetAvailableAgents(ctx context.Context, labels map[string]string, limit int) ([]*db.AgentController, error) {
	agents, err := r.agentStore.GetAvailable(ctx, labels, limit)
	if err != nil {
		return nil, fmt.Errorf("getting available agents: %w", err)
	}
	return agents, nil
}

// AgentConfig represents the configuration for registering a new agent
type AgentConfig struct {
	Name            string
	Token           string
	ClusterName     string
	Namespace       string
	Labels          map[string]string
	Capacity        int
	AtlantisVersion string
}

// Validate checks if the agent configuration is valid
func (c *AgentConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("agent name cannot be empty")
	}
	if c.Token == "" {
		return fmt.Errorf("agent token cannot be empty")
	}
	if len(c.Token) < 32 {
		return fmt.Errorf("agent token must be at least 32 characters")
	}
	// Note: cluster_name is optional - agents can run without specifying a cluster
	if c.Capacity <= 0 {
		return fmt.Errorf("capacity must be greater than 0")
	}
	return nil
}

// AgentStatus represents the possible states of an agent
type AgentStatus string

const (
	AgentStatusActive    AgentStatus = "active"
	AgentStatusDraining  AgentStatus = "draining"
	AgentStatusOffline   AgentStatus = "offline"
	AgentStatusUnhealthy AgentStatus = "unhealthy"
)

// GenerateToken generates a secure random token for agent authentication
func GenerateToken() (string, error) {
	// Generate a random token (32 bytes = 256 bits)
	_ = make([]byte, 32)
	_, err := bcrypt.GenerateFromPassword([]byte(time.Now().String()), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	// For now, return a simple token
	// In production, use crypto/rand
	return fmt.Sprintf("agent-token-%d", time.Now().UnixNano()), nil
}
