package valid

// ExecutionMode defines how Atlantis executes Terraform operations
type ExecutionMode string

const (
	// LocalExecutionMode runs Terraform on the master server (default, backwards compatible)
	LocalExecutionMode ExecutionMode = "local"
	// DistributedExecutionMode runs Terraform on remote agent workers
	DistributedExecutionMode ExecutionMode = "distributed"
)

// IsValid returns true if the execution mode is valid
func (e ExecutionMode) IsValid() bool {
	return e == LocalExecutionMode || e == DistributedExecutionMode
}

// String returns the string representation
func (e ExecutionMode) String() string {
	return string(e)
}

// AgentPoolSelector defines how jobs are routed to agent pools
type AgentPoolSelector struct {
	// Labels are key-value pairs that must match agent controller labels
	// for the job to be assigned to that agent pool.
	// Example: {"environment": "production", "region": "us-east-1"}
	Labels map[string]string
}

// Matches returns true if all selector labels match the agent labels
func (a AgentPoolSelector) Matches(agentLabels map[string]string) bool {
	if len(a.Labels) == 0 {
		return true // Empty selector matches any agent
	}

	for key, value := range a.Labels {
		if agentLabels[key] != value {
			return false
		}
	}
	return true
}

// IsEmpty returns true if no labels are specified
func (a AgentPoolSelector) IsEmpty() bool {
	return len(a.Labels) == 0
}
