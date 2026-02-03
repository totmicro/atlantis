package jobs_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/jobs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobBuilder_BuildFromCommand(t *testing.T) {
	builder := jobs.NewJobBuilder()

	ctx := &command.Context{
		User: models.User{
			Username: "testuser",
		},
		Pull: models.PullRequest{
			Num: 123,
			BaseRepo: models.Repo{
				FullName: "owner/repo",
				CloneURL: "https://github.com/owner/repo.git",
				Name:     "repo",
				Owner:    "owner",
				VCSHost: models.VCSHost{
					Hostname: "github.com",
					Type:     models.Github,
				},
			},
			HeadBranch: "feature-branch",
		},
	}

	t.Run("creates job with correct type", func(t *testing.T) {
		job, err := builder.BuildFromCommand(ctx, command.Plan, "myproject", "default", "terraform", false)
		require.NoError(t, err)
		require.NotNil(t, job)

		assert.NotEmpty(t, job.ID)
		assert.Equal(t, jobs.JobTypePlan, job.Type)
		assert.Equal(t, jobs.JobStatusQueued, job.Status)
		assert.Equal(t, jobs.PriorityNormal, job.Priority)
	})

	t.Run("creates apply job with correct type", func(t *testing.T) {
		job, err := builder.BuildFromCommand(ctx, command.Apply, "myproject", "default", "terraform", false)
		require.NoError(t, err)

		assert.Equal(t, jobs.JobTypeApply, job.Type)
	})

	t.Run("populates job context correctly", func(t *testing.T) {
		job, err := builder.BuildFromCommand(ctx, command.Plan, "myproject", "staging", "terraform/prod", true)
		require.NoError(t, err)

		jctx := job.Context
		assert.Equal(t, "owner/repo", jctx.RepoFullName)
		assert.Equal(t, "https://github.com/owner/repo.git", jctx.RepoCloneURL)
		assert.Equal(t, "feature-branch", jctx.RepoBranch)
		assert.Equal(t, "repo", jctx.RepoName)
		assert.Equal(t, "owner", jctx.RepoOwner)
		assert.Equal(t, "github.com", jctx.RepoHostname)
		assert.Equal(t, command.Plan, jctx.Command)
		assert.Equal(t, "staging", jctx.Workspace)
		assert.Equal(t, "myproject", jctx.ProjectName)
		assert.Equal(t, "terraform/prod", jctx.RepoRelDir)
		assert.True(t, jctx.Verbose)
		assert.Equal(t, "testuser", jctx.User.Username)
	})

	t.Run("extracts labels correctly", func(t *testing.T) {
		job, err := builder.BuildFromCommand(ctx, command.Plan, "myproject", "default", "terraform", false)
		require.NoError(t, err)

		assert.Equal(t, "owner/repo", job.Labels["repo"])
		assert.Equal(t, "123", job.Labels["pr"])
		assert.Equal(t, "Github", job.Labels["vcs"])
	})

	t.Run("returns error for nil context", func(t *testing.T) {
		job, err := builder.BuildFromCommand(nil, command.Plan, "myproject", "default", "terraform", false)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.Contains(t, err.Error(), "cannot be nil")
	})
}

func TestJobBuilder_BuildFromProjectCommand(t *testing.T) {
	builder := jobs.NewJobBuilder()

	ctx := &command.Context{
		User: models.User{
			Username: "testuser",
		},
		Pull: models.PullRequest{
			Num: 456,
			BaseRepo: models.Repo{
				FullName: "org/myrepo",
				CloneURL: "https://gitlab.com/org/myrepo.git",
				Name:     "myrepo",
				Owner:    "org",
				VCSHost: models.VCSHost{
					Hostname: "gitlab.com",
					Type:     models.Gitlab,
				},
			},
			HeadBranch: "develop",
		},
	}

	projectCtx := command.ProjectContext{
		CommandName:      command.Apply,
		Workspace:        "production",
		ProjectName:      "backend-api",
		RepoRelDir:       "infrastructure/backend",
		TerraformVersion: nil, // Will be *version.Version, set to nil for testing
		Verbose:          true,
		BaseRepo: models.Repo{
			FullName: "org/myrepo",
		},
	}

	t.Run("creates job from project context", func(t *testing.T) {
		job, err := builder.BuildFromProjectCommand(ctx, projectCtx)
		require.NoError(t, err)
		require.NotNil(t, job)

		assert.NotEmpty(t, job.ID)
		assert.Equal(t, jobs.JobTypeApply, job.Type)
		assert.Equal(t, jobs.JobStatusQueued, job.Status)
	})

	t.Run("sets high priority for apply commands", func(t *testing.T) {
		job, err := builder.BuildFromProjectCommand(ctx, projectCtx)
		require.NoError(t, err)

		assert.Equal(t, jobs.PriorityHigh, job.Priority)
	})

	t.Run("populates project-specific context", func(t *testing.T) {
		job, err := builder.BuildFromProjectCommand(ctx, projectCtx)
		require.NoError(t, err)

		jctx := job.Context
		assert.Equal(t, "backend-api", jctx.ProjectName)
		assert.Equal(t, "infrastructure/backend", jctx.RepoRelDir)
		assert.Equal(t, "", jctx.TerraformVersion) // nil version becomes empty string
		assert.Equal(t, "production", jctx.Workspace)
	})

	t.Run("extracts project labels", func(t *testing.T) {
		job, err := builder.BuildFromProjectCommand(ctx, projectCtx)
		require.NoError(t, err)

		assert.Equal(t, "backend-api", job.Labels["project"])
		assert.Equal(t, "production", job.Labels["workspace"])
		assert.Equal(t, "infrastructure/backend", job.Labels["dir"])
		assert.Equal(t, "org/myrepo", job.Labels["repo"])
	})

	t.Run("adds metadata", func(t *testing.T) {
		job, err := builder.BuildFromProjectCommand(ctx, projectCtx)
		require.NoError(t, err)

		assert.Equal(t, "backend-api", job.Context.Metadata["project_name"])
		// No terraform version in metadata when nil
	})

	t.Run("plan commands get normal priority", func(t *testing.T) {
		planCtx := projectCtx
		planCtx.CommandName = command.Plan

		job, err := builder.BuildFromProjectCommand(ctx, planCtx)
		require.NoError(t, err)

		assert.Equal(t, jobs.PriorityNormal, job.Priority)
	})
}

func TestJobContext_ToMap(t *testing.T) {
	ctx := &jobs.JobContext{
		RepoFullName:     "owner/repo",
		RepoCloneURL:     "https://github.com/owner/repo.git",
		Command:          command.Plan,
		Workspace:        "default",
		ProjectName:      "myproject",
		TerraformVersion: "1.5.0",
		Verbose:          true,
		Metadata: map[string]string{
			"key": "value",
		},
	}

	m := ctx.ToMap()

	assert.Equal(t, "owner/repo", m["repo_full_name"])
	assert.Equal(t, "https://github.com/owner/repo.git", m["repo_clone_url"])
	assert.Equal(t, command.Plan, m["command"])
	assert.Equal(t, "default", m["workspace"])
	assert.Equal(t, "myproject", m["project_name"])
	assert.Equal(t, "1.5.0", m["terraform_version"])
	assert.True(t, m["verbose"].(bool))

	metadata := m["metadata"].(map[string]string)
	assert.Equal(t, "value", metadata["key"])
}
