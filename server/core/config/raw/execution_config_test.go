package raw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAgentPoolSelector_Validate(t *testing.T) {
	tests := []struct {
		name      string
		selector  AgentPoolSelector
		wantError bool
	}{
		{
			name:      "empty selector",
			selector:  AgentPoolSelector{},
			wantError: false,
		},
		{
			name: "valid labels",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"environment": "production",
					"region":      "us-east-1",
					"tier":        "high",
				},
			},
			wantError: false,
		},
		{
			name: "valid with prefix",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"example.com/environment": "production",
				},
			},
			wantError: false,
		},
		{
			name: "valid with dash and underscore",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"my-app_version": "1.0.0",
				},
			},
			wantError: false,
		},
		{
			name: "invalid - key starts with dash",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"-invalid": "value",
				},
			},
			wantError: true,
		},
		{
			name: "invalid - key ends with dash",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"invalid-": "value",
				},
			},
			wantError: true,
		},
		{
			name: "invalid - value too long",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"key": "this-is-a-very-long-value-that-exceeds-the-maximum-allowed-length-of-sixty-three-characters",
				},
			},
			wantError: true,
		},
		{
			name: "invalid - key with special chars",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"key@invalid": "value",
				},
			},
			wantError: true,
		},
		{
			name: "invalid - multiple slashes",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"example.com/app/name": "value",
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.selector.Validate()
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAgentPoolSelector_ToValid(t *testing.T) {
	tests := []struct {
		name     string
		selector AgentPoolSelector
	}{
		{
			name:     "nil labels",
			selector: AgentPoolSelector{Labels: nil},
		},
		{
			name: "with labels",
			selector: AgentPoolSelector{
				Labels: map[string]string{
					"environment": "production",
					"region":      "us-east-1",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.selector.ToValid()
			assert.NotNil(t, valid.Labels)
			if tt.selector.Labels != nil {
				assert.Equal(t, len(tt.selector.Labels), len(valid.Labels))
			}
		})
	}
}

func TestValidateLabelKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantError bool
	}{
		{"simple key", "environment", false},
		{"with dash", "my-environment", false},
		{"with underscore", "my_environment", false},
		{"with dot", "app.version", false},
		{"with prefix", "example.com/app", false},
		{"empty", "", true},
		{"starts with dash", "-invalid", true},
		{"ends with dash", "invalid-", true},
		{"starts with dot", ".invalid", true},
		{"special char", "app@name", true},
		{"too long", "this-is-a-very-long-key-that-exceeds-sixty-three-characters-limit", true},
		{"multiple slashes", "example.com/app/name", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLabelKey(tt.key)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateLabelValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"simple value", "production", false},
		{"with dash", "us-east-1", false},
		{"with underscore", "v1_0_0", false},
		{"with dot", "1.0.0", false},
		{"empty", "", false},
		{"starts with dash", "-invalid", true},
		{"ends with dash", "invalid-", true},
		{"special char", "value@invalid", true},
		{"too long", "this-is-a-very-long-value-that-exceeds-the-maximum-allowed-length-of-sixty-three-characters", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLabelValue(tt.value)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
