package controller

import (
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		config := Config{
			ControllerID:      "test-controller",
			Token:             "test-token-12345",
			MasterAddress:     "localhost:50051",
			MaxConcurrentJobs: 10,
			ClusterName:       "test-cluster",
			Namespace:         "atlantis",
		}

		err := config.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 30*time.Second, config.HeartbeatInterval)
		assert.Equal(t, "/tmp/atlantis-agent", config.WorkDir)
	})

	t.Run("empty controller ID fails", func(t *testing.T) {
		config := Config{
			Token:             "test-token",
			MasterAddress:     "localhost:50051",
			MaxConcurrentJobs: 10,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "controller ID")
	})

	t.Run("empty token fails", func(t *testing.T) {
		config := Config{
			ControllerID:      "test-controller",
			MasterAddress:     "localhost:50051",
			MaxConcurrentJobs: 10,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "token")
	})

	t.Run("zero max jobs fails", func(t *testing.T) {
		config := Config{
			ControllerID:      "test-controller",
			Token:             "test-token",
			MasterAddress:     "localhost:50051",
			MaxConcurrentJobs: 0,
		}

		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max concurrent jobs")
	})
}

func TestNewAgentController(t *testing.T) {
	logger := logging.NewNoopLogger(t)

	t.Run("creates controller with valid config", func(t *testing.T) {
		config := Config{
			ControllerID:      "test-controller",
			Token:             "test-token-12345",
			MasterAddress:     "localhost:50051",
			MaxConcurrentJobs: 10,
			ClusterName:       "test-cluster",
		}

		controller, err := NewAgentController(config, logger)
		require.NoError(t, err)
		assert.NotNil(t, controller)
		assert.NotNil(t, controller.executor)
		assert.Equal(t, 0, len(controller.currentJobs))
	})

	t.Run("fails with invalid config", func(t *testing.T) {
		config := Config{
			MaxConcurrentJobs: 10,
		}

		controller, err := NewAgentController(config, logger)
		assert.Error(t, err)
		assert.Nil(t, controller)
	})
}

func TestAgentController_GetPodName(t *testing.T) {
	logger := logging.NewNoopLogger(t)

	config := Config{
		ControllerID:      "test-controller-123",
		Token:             "test-token-12345",
		MasterAddress:     "localhost:50051",
		MaxConcurrentJobs: 10,
	}

	controller, err := NewAgentController(config, logger)
	require.NoError(t, err)

	podName := controller.getPodName()
	assert.Equal(t, "test-controller-123", podName)
}
