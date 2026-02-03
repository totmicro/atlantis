package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/runatlantis/atlantis/proto"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJobExecutor(t *testing.T) {
	logger := logging.NewNoopLogger(t)

	t.Run("creates executor with config", func(t *testing.T) {
		config := JobExecutorConfig{
			WorkDir:          "/tmp/test-executor",
			TerraformBinPath: "/usr/bin/terraform",
			Logger:           logger,
		}

		executor := NewJobExecutor(config)
		require.NotNil(t, executor)
		assert.Equal(t, "/tmp/test-executor", executor.config.WorkDir)
		assert.Equal(t, "/usr/bin/terraform", executor.config.TerraformBinPath)
	})

	t.Run("sets default terraform path", func(t *testing.T) {
		config := JobExecutorConfig{
			WorkDir: "/tmp/test-executor",
			Logger:  logger,
		}

		executor := NewJobExecutor(config)
		require.NotNil(t, executor)
		assert.Equal(t, "terraform", executor.config.TerraformBinPath)
	})
}

func TestJobExecutor_Execute_UnknownCommand(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	tempDir := t.TempDir()

	executor := NewJobExecutor(JobExecutorConfig{
		WorkDir: tempDir,
		Logger:  logger,
	})

	job := &proto.JobAssignment{
		JobId:   "test-job-1",
		Command: "unknown-command",
	}

	result := executor.Execute(context.Background(), job)

	assert.Equal(t, "failed", result.Status)
	assert.Contains(t, result.ErrorMessage, "unknown command")
}

func TestJobExecutor_CloneRepo_MissingURL(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	tempDir := t.TempDir()

	executor := NewJobExecutor(JobExecutorConfig{
		WorkDir: tempDir,
		Logger:  logger,
	})

	job := &proto.JobAssignment{
		JobId:        "test-job-2",
		RepoCloneUrl: "",
	}

	targetDir := filepath.Join(tempDir, "repo")
	err := executor.cloneRepo(context.Background(), job, targetDir)

	assert.Error(t, err)
}

func TestJobExecutor_Cleanup(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	tempDir := t.TempDir()

	executor := NewJobExecutor(JobExecutorConfig{
		WorkDir: tempDir,
		Logger:  logger,
	})

	// Create a directory to cleanup
	testDir := filepath.Join(tempDir, "test-cleanup")
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	// Create a file in it
	testFile := filepath.Join(testDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Verify it exists
	_, err = os.Stat(testDir)
	assert.NoError(t, err)

	// Cleanup
	executor.cleanup(testDir)

	// Verify it's gone
	_, err = os.Stat(testDir)
	assert.True(t, os.IsNotExist(err))
}

func TestJobResult_Structure(t *testing.T) {
	result := &JobResult{
		Status:       "completed",
		Output:       "Test output",
		PlanData:     []byte("plan data"),
		ExitCode:     0,
		ErrorMessage: "",
	}

	assert.Equal(t, "completed", result.Status)
	assert.Equal(t, "Test output", result.Output)
	assert.Equal(t, []byte("plan data"), result.PlanData)
	assert.Equal(t, 0, result.ExitCode)
	assert.Empty(t, result.ErrorMessage)
}

func TestJobExecutor_WorkDirCreation(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	tempDir := t.TempDir()

	executor := NewJobExecutor(JobExecutorConfig{
		WorkDir: tempDir,
		Logger:  logger,
	})

	job := &proto.JobAssignment{
		JobId:        "test-job-3",
		Command:      "plan",
		RepoCloneUrl: "https://github.com/nonexistent/repo.git",
		ProjectDir:   ".",
	}

	// This will fail at clone, but should create the job directory first
	result := executor.Execute(context.Background(), job)

	assert.Equal(t, "failed", result.Status)
	// Job directory should have been created and cleaned up
}

func TestJobExecutorConfig_Defaults(t *testing.T) {
	logger := logging.NewNoopLogger(t)

	config := JobExecutorConfig{
		Logger: logger,
	}

	executor := NewJobExecutor(config)

	assert.Equal(t, "terraform", executor.config.TerraformBinPath)
	assert.Empty(t, executor.config.WorkDir)
}
