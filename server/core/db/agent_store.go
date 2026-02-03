package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// AgentStore handles agent controller registration and management
type AgentStore interface {
	Register(ctx context.Context, agent *AgentController, token string) error
	Get(ctx context.Context, id string) (*AgentController, error)
	GetByName(ctx context.Context, name string) (*AgentController, error)
	Authenticate(ctx context.Context, controllerID, token string) (bool, error)
	UpdateHeartbeat(ctx context.Context, controllerID string, currentJobs int) error
	UpdateStatus(ctx context.Context, controllerID string, status AgentStatus) error
	UpdateCapacity(ctx context.Context, controllerID string, capacity int) error
	IncrementJobs(ctx context.Context, controllerID string) error
	DecrementJobs(ctx context.Context, controllerID string) error
	GetAvailable(ctx context.Context, labels map[string]string, limit int) ([]*AgentController, error)
	MarkStaleAsUnhealthy(ctx context.Context, thresholdSeconds int) (int, error)
	ListAll(ctx context.Context) ([]*AgentController, error)
	Delete(ctx context.Context, controllerID string) error
}

// PostgresAgentStore implements AgentStore using PostgreSQL
type PostgresAgentStore struct {
	db *sql.DB
}

// NewPostgresAgentStore creates a new PostgreSQL agent store
func NewPostgresAgentStore(db *sql.DB) *PostgresAgentStore {
	return &PostgresAgentStore{db: db}
}

// Register registers a new agent controller
func (s *PostgresAgentStore) Register(ctx context.Context, agent *AgentController, token string) error {
	if agent.ID == "" {
		agent.ID = uuid.New().String()
	}

	// Hash the token
	hashedToken, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hashing token: %w", err)
	}

	now := time.Now()
	agent.RegisteredAt = now
	agent.LastHeartbeat = now
	agent.LastSeen = now

	if agent.Status == "" {
		agent.Status = string(AgentStatusActive)
	}
	if agent.Capacity == 0 {
		agent.Capacity = 10 // Default capacity
	}

	// Marshal labels - ensure we always have a valid JSON object, not null
	if agent.Labels == nil {
		agent.Labels = make(map[string]string)
	}
	labelsJSON, err := json.Marshal(agent.Labels)
	if err != nil {
		return fmt.Errorf("marshaling labels: %w", err)
	}

	// Ensure metadata is valid JSON - use empty object if nil
	if agent.Metadata == nil {
		agent.Metadata = json.RawMessage("{}")
	}

	query := `
		INSERT INTO agent_controllers (
			id, name, token_hash, cluster_name, namespace, labels, capacity, 
			current_jobs, status, registered_at, last_heartbeat, last_seen, 
			atlantis_version, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)
		ON CONFLICT (name) DO UPDATE SET
			token_hash = EXCLUDED.token_hash,
			cluster_name = EXCLUDED.cluster_name,
			namespace = EXCLUDED.namespace,
			labels = EXCLUDED.labels,
			capacity = EXCLUDED.capacity,
			status = 'active',
			last_heartbeat = EXCLUDED.last_heartbeat,
			last_seen = EXCLUDED.last_seen,
			atlantis_version = EXCLUDED.atlantis_version,
			metadata = EXCLUDED.metadata
		RETURNING id
	`

	err = s.db.QueryRowContext(
		ctx, query,
		agent.ID, agent.Name, string(hashedToken), agent.ClusterName, agent.Namespace,
		labelsJSON, agent.Capacity, agent.CurrentJobs, agent.Status, agent.RegisteredAt,
		agent.LastHeartbeat, agent.LastSeen, agent.AtlantisVersion, agent.Metadata,
	).Scan(&agent.ID)

	if err != nil {
		return fmt.Errorf("registering agent: %w", err)
	}

	return nil
}

// Get retrieves an agent by ID
func (s *PostgresAgentStore) Get(ctx context.Context, id string) (*AgentController, error) {
	query := `
		SELECT id, name, cluster_name, namespace, labels, capacity, current_jobs, 
			status, registered_at, last_heartbeat, last_seen, atlantis_version, metadata
		FROM agent_controllers
		WHERE id = $1
	`

	agent := &AgentController{}
	var labelsJSON []byte

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&agent.ID, &agent.Name, &agent.ClusterName, &agent.Namespace, &labelsJSON,
		&agent.Capacity, &agent.CurrentJobs, &agent.Status, &agent.RegisteredAt,
		&agent.LastHeartbeat, &agent.LastSeen, &agent.AtlantisVersion, &agent.Metadata,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("getting agent: %w", err)
	}

	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &agent.Labels); err != nil {
			return nil, fmt.Errorf("unmarshaling labels: %w", err)
		}
	}

	return agent, nil
}

// GetByName retrieves an agent by name
func (s *PostgresAgentStore) GetByName(ctx context.Context, name string) (*AgentController, error) {
	query := `
		SELECT id, name, cluster_name, namespace, labels, capacity, current_jobs, 
			status, registered_at, last_heartbeat, last_seen, atlantis_version, metadata
		FROM agent_controllers
		WHERE name = $1
	`

	agent := &AgentController{}
	var labelsJSON []byte

	err := s.db.QueryRowContext(ctx, query, name).Scan(
		&agent.ID, &agent.Name, &agent.ClusterName, &agent.Namespace, &labelsJSON,
		&agent.Capacity, &agent.CurrentJobs, &agent.Status, &agent.RegisteredAt,
		&agent.LastHeartbeat, &agent.LastSeen, &agent.AtlantisVersion, &agent.Metadata,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("agent not found: %s", name)
	}
	if err != nil {
		return nil, fmt.Errorf("getting agent: %w", err)
	}

	if len(labelsJSON) > 0 {
		if err := json.Unmarshal(labelsJSON, &agent.Labels); err != nil {
			return nil, fmt.Errorf("unmarshaling labels: %w", err)
		}
	}

	return agent, nil
}

// Authenticate verifies an agent's token
func (s *PostgresAgentStore) Authenticate(ctx context.Context, controllerID, token string) (bool, error) {
	// Try to query by ID (UUID) first
	query := `SELECT token_hash FROM agent_controllers WHERE id = $1`

	var hashedToken string
	err := s.db.QueryRowContext(ctx, query, controllerID).Scan(&hashedToken)

	// If UUID parsing failed or not found, try by name
	if err != nil {
		if err == sql.ErrNoRows || strings.Contains(err.Error(), "invalid input syntax for type uuid") {
			query = `SELECT token_hash FROM agent_controllers WHERE name = $1`
			err = s.db.QueryRowContext(ctx, query, controllerID).Scan(&hashedToken)
			if err == sql.ErrNoRows {
				return false, nil
			}
			if err != nil {
				return false, fmt.Errorf("querying token by name: %w", err)
			}
		} else {
			return false, fmt.Errorf("querying token: %w", err)
		}
	}

	err = bcrypt.CompareHashAndPassword([]byte(hashedToken), []byte(token))
	return err == nil, nil
}

// UpdateHeartbeat updates the agent's heartbeat
func (s *PostgresAgentStore) UpdateHeartbeat(ctx context.Context, controllerID string, currentJobs int) error {
	query := `SELECT update_agent_heartbeat($1, $2)`
	_, err := s.db.ExecContext(ctx, query, controllerID, currentJobs)
	if err != nil {
		return fmt.Errorf("updating heartbeat: %w", err)
	}
	return nil
}

// UpdateStatus updates the agent's status
func (s *PostgresAgentStore) UpdateStatus(ctx context.Context, controllerID string, status AgentStatus) error {
	statusStr := string(status)
	query := `UPDATE agent_controllers SET status = $1, last_seen = $2 WHERE id = $3`
	_, err := s.db.ExecContext(ctx, query, statusStr, time.Now(), controllerID)
	if err != nil {
		return fmt.Errorf("updating status: %w", err)
	}
	return nil
}

// UpdateCapacity updates the agent's capacity
func (s *PostgresAgentStore) UpdateCapacity(ctx context.Context, controllerID string, capacity int) error {
	query := `UPDATE agent_controllers SET capacity = $1, last_seen = $2 WHERE id = $3`
	_, err := s.db.ExecContext(ctx, query, capacity, time.Now(), controllerID)
	if err != nil {
		return fmt.Errorf("updating capacity: %w", err)
	}
	return nil
}

// IncrementJobs increments the job count
func (s *PostgresAgentStore) IncrementJobs(ctx context.Context, controllerID string) error {
	query := `SELECT increment_agent_jobs($1)`
	_, err := s.db.ExecContext(ctx, query, controllerID)
	if err != nil {
		return fmt.Errorf("incrementing jobs: %w", err)
	}
	return nil
}

// DecrementJobs decrements the job count
func (s *PostgresAgentStore) DecrementJobs(ctx context.Context, controllerID string) error {
	query := `SELECT decrement_agent_jobs($1)`
	_, err := s.db.ExecContext(ctx, query, controllerID)
	if err != nil {
		return fmt.Errorf("decrementing jobs: %w", err)
	}
	return nil
}

// GetAvailable retrieves available agents for job assignment
func (s *PostgresAgentStore) GetAvailable(ctx context.Context, labels map[string]string, limit int) ([]*AgentController, error) {
	var labelsParam interface{}
	if labels != nil && len(labels) > 0 {
		labelsJSON, err := json.Marshal(labels)
		if err != nil {
			return nil, fmt.Errorf("marshaling labels: %w", err)
		}
		labelsParam = labelsJSON
	} else {
		labelsParam = nil
	}

	query := `SELECT * FROM get_available_agents($1, $2)`

	rows, err := s.db.QueryContext(ctx, query, labelsParam, limit)
	if err != nil {
		return nil, fmt.Errorf("getting available agents: %w", err)
	}
	defer rows.Close()

	var agents []*AgentController
	for rows.Next() {
		agent := &AgentController{}
		var availableSlots int

		err := rows.Scan(
			&agent.ID,
			&agent.Name,
			&agent.ClusterName,
			&agent.CurrentJobs,
			&agent.Capacity,
			&availableSlots,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning agent: %w", err)
		}

		agents = append(agents, agent)
	}

	return agents, nil
}

// MarkStaleAsUnhealthy marks agents as unhealthy if heartbeat is old
func (s *PostgresAgentStore) MarkStaleAsUnhealthy(ctx context.Context, thresholdSeconds int) (int, error) {
	query := `SELECT mark_stale_agents_unhealthy($1)`

	var count int
	err := s.db.QueryRowContext(ctx, query, thresholdSeconds).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("marking stale agents: %w", err)
	}

	return count, nil
}

// ListAll retrieves all agent controllers
func (s *PostgresAgentStore) ListAll(ctx context.Context) ([]*AgentController, error) {
	query := `
		SELECT id, name, cluster_name, namespace, labels, capacity, current_jobs, 
			status, registered_at, last_heartbeat, last_seen, atlantis_version
		FROM agent_controllers
		ORDER BY name
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("listing agents: %w", err)
	}
	defer rows.Close()

	var agents []*AgentController
	for rows.Next() {
		agent := &AgentController{}
		var labelsJSON []byte

		err := rows.Scan(
			&agent.ID, &agent.Name, &agent.ClusterName, &agent.Namespace, &labelsJSON,
			&agent.Capacity, &agent.CurrentJobs, &agent.Status, &agent.RegisteredAt,
			&agent.LastHeartbeat, &agent.LastSeen, &agent.AtlantisVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning agent: %w", err)
		}

		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &agent.Labels); err != nil {
				return nil, fmt.Errorf("unmarshaling labels: %w", err)
			}
		}

		agents = append(agents, agent)
	}

	return agents, nil
}

// Delete deletes an agent controller
func (s *PostgresAgentStore) Delete(ctx context.Context, controllerID string) error {
	query := `DELETE FROM agent_controllers WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, controllerID)
	if err != nil {
		return fmt.Errorf("deleting agent: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("agent not found: %s", controllerID)
	}

	return nil
}
