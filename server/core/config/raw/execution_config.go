package raw

import (
	"fmt"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/runatlantis/atlantis/server/core/config/valid"
)

// AgentPoolSelector defines agent pool selection criteria
type AgentPoolSelector struct {
	Labels map[string]string `yaml:"labels,omitempty"`
}

// Validate ensures the agent pool selector is valid
func (a AgentPoolSelector) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.Labels, validation.By(validateLabels)),
	)
}

// ToValid converts raw AgentPoolSelector to valid config
func (a AgentPoolSelector) ToValid() valid.AgentPoolSelector {
	if a.Labels == nil {
		return valid.AgentPoolSelector{Labels: make(map[string]string)}
	}
	return valid.AgentPoolSelector{Labels: a.Labels}
}

// validateLabels ensures labels follow Kubernetes naming conventions
func validateLabels(value interface{}) error {
	labels, ok := value.(map[string]string)
	if !ok || labels == nil {
		return nil // empty is valid
	}

	for key, val := range labels {
		if err := validateLabelKey(key); err != nil {
			return fmt.Errorf("label key %q: %w", key, err)
		}
		if err := validateLabelValue(val); err != nil {
			return fmt.Errorf("label value for key %q: %w", key, err)
		}
	}
	return nil
}

// validateLabelKey validates Kubernetes label key format
// Format: [prefix/]name where prefix is optional
// - name must be 63 chars or less
// - alphanumeric, dash, underscore, dot
// - must start and end with alphanumeric
func validateLabelKey(key string) error {
	if key == "" {
		return fmt.Errorf("cannot be empty")
	}

	// Split prefix and name
	parts := strings.Split(key, "/")
	var name string

	switch len(parts) {
	case 1:
		name = parts[0]
	case 2:
		// Has prefix, validate it
		prefix := parts[0]
		if len(prefix) > 253 {
			return fmt.Errorf("prefix must be 253 chars or less")
		}
		name = parts[1]
	default:
		return fmt.Errorf("can contain at most one '/'")
	}

	if len(name) == 0 || len(name) > 63 {
		return fmt.Errorf("name must be 1-63 chars")
	}

	if !isValidLabelName(name) {
		return fmt.Errorf("name must start/end with alphanumeric and contain only alphanumeric, dash, underscore, or dot")
	}

	return nil
}

// validateLabelValue validates Kubernetes label value format
// - 63 chars or less
// - alphanumeric, dash, underscore, dot
// - must start and end with alphanumeric (or be empty)
func validateLabelValue(value string) error {
	if len(value) > 63 {
		return fmt.Errorf("must be 63 chars or less")
	}

	if value == "" {
		return nil // empty is valid
	}

	if !isValidLabelName(value) {
		return fmt.Errorf("must start/end with alphanumeric and contain only alphanumeric, dash, underscore, or dot")
	}

	return nil
}

// isValidLabelName checks if a string is a valid label name/value
func isValidLabelName(s string) bool {
	if s == "" {
		return false
	}

	// Must start and end with alphanumeric
	first := s[0]
	last := s[len(s)-1]

	if !isAlphanumeric(first) || !isAlphanumeric(last) {
		return false
	}

	// All characters must be alphanumeric, dash, underscore, or dot
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isAlphanumeric(c) && c != '-' && c != '_' && c != '.' {
			return false
		}
	}

	return true
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
