package scheduler

import (
	"fmt"
	"sort"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/jobs"
)

// Router handles label-based routing of jobs to agents
type Router struct {
	config RouterConfig
}

// NewRouter creates a new router with the given configuration
func NewRouter(config RouterConfig) *Router {
	return &Router{
		config: config,
	}
}

// FindMatchingAgents returns agents that match the job's label requirements
func (r *Router) FindMatchingAgents(job *jobs.Job, availableAgents []*db.AgentController) []*db.AgentController {
	if len(job.Labels) == 0 {
		// No label requirements, all agents match
		return availableAgents
	}

	matched := make([]*db.AgentController, 0)
	for _, agent := range availableAgents {
		if r.agentMatchesJob(agent, job) {
			matched = append(matched, agent)
		}
	}

	return matched
}

// SelectBestAgent chooses the best agent from matching candidates based on policy
func (r *Router) SelectBestAgent(job *jobs.Job, candidates []*db.AgentController) *db.AgentController {
	if len(candidates) == 0 {
		return nil
	}

	switch r.config.Policy {
	case PolicyLeastLoaded:
		return r.selectLeastLoaded(candidates)
	case PolicyRoundRobin:
		// Simple selection - first candidate (external round-robin tracking needed)
		return candidates[0]
	case PolicyPriorityFirst:
		// For priority-first, least loaded is still best choice among equal priority
		return r.selectLeastLoaded(candidates)
	default:
		return candidates[0]
	}
}

// SelectBestAgentForDBJob is a convenience wrapper for db.Job
func (r *Router) SelectBestAgentForDBJob(job *db.Job, candidates []*db.AgentController) *db.AgentController {
	if len(candidates) == 0 {
		return nil
	}

	// Filter candidates by label matching
	matched := make([]*db.AgentController, 0)
	for _, agent := range candidates {
		if r.agentMatchesDBJob(agent, job) {
			matched = append(matched, agent)
		}
	}

	if len(matched) == 0 {
		return nil
	}

	// Select based on policy
	switch r.config.Policy {
	case PolicyLeastLoaded:
		return r.selectLeastLoaded(matched)
	case PolicyRoundRobin:
		return matched[0]
	case PolicyPriorityFirst:
		return r.selectLeastLoaded(matched)
	default:
		return matched[0]
	}
}

// agentMatchesJob checks if an agent's labels match the job requirements
func (r *Router) agentMatchesJob(agent *db.AgentController, job *jobs.Job) bool {
	agentLabels := agent.Labels

	if r.config.EnableStrictLabelMatching {
		// All job labels must match exactly
		return r.strictMatch(agentLabels, job.Labels)
	}

	// Flexible matching: agent must have at least the required labels
	return r.flexibleMatch(agentLabels, job.Labels)
}

// agentMatchesDBJob checks if an agent's labels match the db.Job requirements
func (r *Router) agentMatchesDBJob(agent *db.AgentController, job *db.Job) bool {
	agentLabels := agent.Labels

	// Debug logging (will be removed after troubleshooting)
	if len(job.Labels) == 0 {
		// No job labels means any agent matches
		return true
	}

	if r.config.EnableStrictLabelMatching {
		// All job labels must match exactly
		return r.strictMatch(agentLabels, job.Labels)
	}

	// Flexible matching: agent must have at least the required labels
	return r.flexibleMatch(agentLabels, job.Labels)
}

// strictMatch requires all labels to match exactly
func (r *Router) strictMatch(agentLabels, jobLabels map[string]string) bool {
	// Must have same number of labels
	if len(agentLabels) != len(jobLabels) {
		return false
	}

	// All job labels must match
	for key, jobValue := range jobLabels {
		agentValue, exists := agentLabels[key]
		if !exists || agentValue != jobValue {
			return false
		}
	}

	return true
}

// flexibleMatch allows agents to have additional labels
func (r *Router) flexibleMatch(agentLabels, jobLabels map[string]string) bool {
	// All job labels must be present in agent labels
	for key, jobValue := range jobLabels {
		agentValue, exists := agentLabels[key]
		if !exists || agentValue != jobValue {
			return false
		}
	}

	return true
}

// selectLeastLoaded returns the agent with the lowest current job count
func (r *Router) selectLeastLoaded(agents []*db.AgentController) *db.AgentController {
	if len(agents) == 0 {
		return nil
	}

	// Sort by current_jobs ascending
	sort.Slice(agents, func(i, j int) bool {
		return agents[i].CurrentJobs < agents[j].CurrentJobs
	})

	return agents[0]
}

// ValidateLabels validates label format and values
func ValidateLabels(labels map[string]string) error {
	for key, value := range labels {
		if key == "" {
			return fmt.Errorf("label key cannot be empty")
		}
		if value == "" {
			return fmt.Errorf("label value cannot be empty for key %s", key)
		}
	}
	return nil
}

// CalculateMatchScore returns a score indicating how well an agent matches a job
// Higher score means better match
func (r *Router) CalculateMatchScore(agent *db.AgentController, job *jobs.Job) int {
	score := 0

	// Base score for being available
	score += 10

	// Points for each matching label
	for key, jobValue := range job.Labels {
		if agentValue, exists := agent.Labels[key]; exists && agentValue == jobValue {
			score += 5
		}
	}

	// Bonus for low current load
	if agent.CurrentJobs == 0 {
		score += 3
	} else if agent.CurrentJobs < agent.Capacity/2 {
		score += 2
	} else if agent.CurrentJobs < agent.Capacity {
		score += 1
	}

	// Penalty for high load
	if agent.CurrentJobs >= agent.Capacity*90/100 {
		score -= 5
	}

	return score
}
