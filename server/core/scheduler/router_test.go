package scheduler_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/db"
	"github.com/runatlantis/atlantis/server/core/jobs"
	"github.com/runatlantis/atlantis/server/core/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_FindMatchingAgents(t *testing.T) {
	config := scheduler.DefaultRouterConfig()
	router := scheduler.NewRouter(config)

	agents := []*db.AgentController{
		{
			ID: "agent-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
			},
			CurrentJobs: 0,
			Capacity:    10,
		},
		{
			ID: "agent-2",
			Labels: map[string]string{
				"env":    "staging",
				"region": "us-west-2",
			},
			CurrentJobs: 2,
			Capacity:    10,
		},
		{
			ID: "agent-3",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-west-2",
			},
			CurrentJobs: 1,
			Capacity:    10,
		},
	}

	t.Run("matches agents with exact labels", func(t *testing.T) {
		job := &jobs.Job{
			ID: "job-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
			},
		}

		matched := router.FindMatchingAgents(job, agents)
		require.Len(t, matched, 1)
		assert.Equal(t, "agent-1", matched[0].ID)
	})

	t.Run("matches agents with subset of labels", func(t *testing.T) {
		job := &jobs.Job{
			ID: "job-2",
			Labels: map[string]string{
				"env": "production",
			},
		}

		matched := router.FindMatchingAgents(job, agents)
		require.Len(t, matched, 2) // agent-1 and agent-3
		assert.Contains(t, []string{matched[0].ID, matched[1].ID}, "agent-1")
		assert.Contains(t, []string{matched[0].ID, matched[1].ID}, "agent-3")
	})

	t.Run("returns empty when no agents match", func(t *testing.T) {
		job := &jobs.Job{
			ID: "job-3",
			Labels: map[string]string{
				"env": "development",
			},
		}

		matched := router.FindMatchingAgents(job, agents)
		assert.Empty(t, matched)
	})

	t.Run("returns all agents when job has no labels", func(t *testing.T) {
		job := &jobs.Job{
			ID:     "job-4",
			Labels: map[string]string{},
		}

		matched := router.FindMatchingAgents(job, agents)
		assert.Len(t, matched, 3)
	})
}

func TestRouter_SelectBestAgent(t *testing.T) {
	t.Run("selects least loaded agent", func(t *testing.T) {
		config := scheduler.RouterConfig{
			Policy: scheduler.PolicyLeastLoaded,
		}
		router := scheduler.NewRouter(config)

		candidates := []*db.AgentController{
			{ID: "agent-1", CurrentJobs: 5, Capacity: 10},
			{ID: "agent-2", CurrentJobs: 2, Capacity: 10},
			{ID: "agent-3", CurrentJobs: 8, Capacity: 10},
		}

		job := &jobs.Job{ID: "job-1"}
		selected := router.SelectBestAgent(job, candidates)

		require.NotNil(t, selected)
		assert.Equal(t, "agent-2", selected.ID)
	})

	t.Run("selects first agent in round-robin mode", func(t *testing.T) {
		config := scheduler.RouterConfig{
			Policy: scheduler.PolicyRoundRobin,
		}
		router := scheduler.NewRouter(config)

		candidates := []*db.AgentController{
			{ID: "agent-1", CurrentJobs: 5, Capacity: 10},
			{ID: "agent-2", CurrentJobs: 2, Capacity: 10},
		}

		job := &jobs.Job{ID: "job-1"}
		selected := router.SelectBestAgent(job, candidates)

		require.NotNil(t, selected)
		assert.Equal(t, "agent-1", selected.ID)
	})

	t.Run("returns nil when no candidates", func(t *testing.T) {
		config := scheduler.DefaultRouterConfig()
		router := scheduler.NewRouter(config)

		job := &jobs.Job{ID: "job-1"}
		selected := router.SelectBestAgent(job, []*db.AgentController{})

		assert.Nil(t, selected)
	})
}

func TestRouter_StrictVsFlexibleMatching(t *testing.T) {
	t.Run("strict matching requires exact labels", func(t *testing.T) {
		config := scheduler.RouterConfig{
			EnableStrictLabelMatching: true,
		}
		router := scheduler.NewRouter(config)

		agent := &db.AgentController{
			ID: "agent-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
				"extra":  "label",
			},
		}

		job := &jobs.Job{
			ID: "job-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
			},
		}

		matched := router.FindMatchingAgents(job, []*db.AgentController{agent})
		assert.Empty(t, matched)
	})

	t.Run("flexible matching allows extra agent labels", func(t *testing.T) {
		config := scheduler.RouterConfig{
			EnableStrictLabelMatching: false,
		}
		router := scheduler.NewRouter(config)

		agent := &db.AgentController{
			ID: "agent-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
				"extra":  "label",
			},
		}

		job := &jobs.Job{
			ID: "job-1",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
			},
		}

		matched := router.FindMatchingAgents(job, []*db.AgentController{agent})
		require.Len(t, matched, 1)
		assert.Equal(t, "agent-1", matched[0].ID)
	})
}

func TestRouter_CalculateMatchScore(t *testing.T) {
	config := scheduler.DefaultRouterConfig()
	router := scheduler.NewRouter(config)

	job := &jobs.Job{
		ID: "job-1",
		Labels: map[string]string{
			"env":    "production",
			"region": "us-east-1",
		},
	}

	t.Run("higher score for more matching labels", func(t *testing.T) {
		agent1 := &db.AgentController{
			ID: "agent-1",
			Labels: map[string]string{
				"env": "production",
			},
			CurrentJobs: 0,
			Capacity:    10,
		}

		agent2 := &db.AgentController{
			ID: "agent-2",
			Labels: map[string]string{
				"env":    "production",
				"region": "us-east-1",
			},
			CurrentJobs: 0,
			Capacity:    10,
		}

		score1 := router.CalculateMatchScore(agent1, job)
		score2 := router.CalculateMatchScore(agent2, job)

		assert.Greater(t, score2, score1)
	})

	t.Run("bonus for low current load", func(t *testing.T) {
		agentIdle := &db.AgentController{
			ID:          "agent-idle",
			Labels:      job.Labels,
			CurrentJobs: 0,
			Capacity:    10,
		}

		agentBusy := &db.AgentController{
			ID:          "agent-busy",
			Labels:      job.Labels,
			CurrentJobs: 5,
			Capacity:    10,
		}

		scoreIdle := router.CalculateMatchScore(agentIdle, job)
		scoreBusy := router.CalculateMatchScore(agentBusy, job)

		assert.Greater(t, scoreIdle, scoreBusy)
	})

	t.Run("penalty for near-capacity load", func(t *testing.T) {
		agentNearCapacity := &db.AgentController{
			ID:          "agent-full",
			Labels:      job.Labels,
			CurrentJobs: 9,
			Capacity:    10,
		}

		agentLow := &db.AgentController{
			ID:          "agent-low",
			Labels:      job.Labels,
			CurrentJobs: 2,
			Capacity:    10,
		}

		scoreNearCapacity := router.CalculateMatchScore(agentNearCapacity, job)
		scoreLow := router.CalculateMatchScore(agentLow, job)

		assert.Less(t, scoreNearCapacity, scoreLow)
	})
}

func TestValidateLabels(t *testing.T) {
	t.Run("valid labels pass", func(t *testing.T) {
		labels := map[string]string{
			"env":    "production",
			"region": "us-east-1",
		}

		err := scheduler.ValidateLabels(labels)
		assert.NoError(t, err)
	})

	t.Run("empty key fails", func(t *testing.T) {
		labels := map[string]string{
			"":    "value",
			"key": "value",
		}

		err := scheduler.ValidateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "key cannot be empty")
	})

	t.Run("empty value fails", func(t *testing.T) {
		labels := map[string]string{
			"key": "",
		}

		err := scheduler.ValidateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value cannot be empty")
	})

	t.Run("empty labels pass", func(t *testing.T) {
		labels := map[string]string{}

		err := scheduler.ValidateLabels(labels)
		assert.NoError(t, err)
	})
}
