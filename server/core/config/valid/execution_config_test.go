package valid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutionMode_IsValid(t *testing.T) {
	tests := []struct {
		name string
		mode ExecutionMode
		want bool
	}{
		{
			name: "local is valid",
			mode: LocalExecutionMode,
			want: true,
		},
		{
			name: "distributed is valid",
			mode: DistributedExecutionMode,
			want: true,
		},
		{
			name: "invalid mode",
			mode: ExecutionMode("invalid"),
			want: false,
		},
		{
			name: "empty mode",
			mode: ExecutionMode(""),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.mode.IsValid())
		})
	}
}

func TestExecutionMode_String(t *testing.T) {
	assert.Equal(t, "local", LocalExecutionMode.String())
	assert.Equal(t, "distributed", DistributedExecutionMode.String())
}

func TestAgentPoolSelector_Matches(t *testing.T) {
	tests := []struct {
		name        string
		selector    AgentPoolSelector
		agentLabels map[string]string
		want        bool
	}{
		{
			name:        "empty selector matches any agent",
			selector:    AgentPoolSelector{Labels: nil},
			agentLabels: map[string]string{"region": "us-east-1"},
			want:        true,
		},
		{
			name:        "empty selector with empty agent labels",
			selector:    AgentPoolSelector{Labels: map[string]string{}},
			agentLabels: map[string]string{},
			want:        true,
		},
		{
			name: "exact match",
			selector: AgentPoolSelector{
				Labels: map[string]string{"environment": "production", "region": "us-east-1"},
			},
			agentLabels: map[string]string{"environment": "production", "region": "us-east-1"},
			want:        true,
		},
		{
			name: "agent has extra labels",
			selector: AgentPoolSelector{
				Labels: map[string]string{"environment": "production"},
			},
			agentLabels: map[string]string{
				"environment": "production",
				"region":      "us-east-1",
				"tier":        "high",
			},
			want: true,
		},
		{
			name: "missing required label",
			selector: AgentPoolSelector{
				Labels: map[string]string{"environment": "production", "region": "us-east-1"},
			},
			agentLabels: map[string]string{"environment": "production"},
			want:        false,
		},
		{
			name: "label value mismatch",
			selector: AgentPoolSelector{
				Labels: map[string]string{"environment": "production"},
			},
			agentLabels: map[string]string{"environment": "development"},
			want:        false,
		},
		{
			name: "no matching labels",
			selector: AgentPoolSelector{
				Labels: map[string]string{"tier": "critical"},
			},
			agentLabels: map[string]string{"region": "us-west-2"},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.selector.Matches(tt.agentLabels))
		})
	}
}

func TestAgentPoolSelector_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		selector AgentPoolSelector
		want     bool
	}{
		{
			name:     "nil labels",
			selector: AgentPoolSelector{Labels: nil},
			want:     true,
		},
		{
			name:     "empty labels map",
			selector: AgentPoolSelector{Labels: map[string]string{}},
			want:     true,
		},
		{
			name: "has labels",
			selector: AgentPoolSelector{
				Labels: map[string]string{"environment": "production"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.selector.IsEmpty())
		})
	}
}
