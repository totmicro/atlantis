package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
)

// PostgresLockAdapter adapts PostgresLockStore to implement the legacy Database interface
// This allows the new PostgreSQL lock store to work with existing Atlantis locking code
type PostgresLockAdapter struct {
	lockStore *PostgresLockStore
	db        *sql.DB
}

// NewPostgresLockAdapter creates a new adapter
func NewPostgresLockAdapter(lockStore *PostgresLockStore, db *sql.DB) *PostgresLockAdapter {
	return &PostgresLockAdapter{
		lockStore: lockStore,
		db:        db,
	}
}

// TryLock attempts to acquire a project lock
func (a *PostgresLockAdapter) TryLock(lock models.ProjectLock) (bool, models.ProjectLock, error) {
	ctx := context.Background()

	// Generate lock ID using the same format as BoltDB and Redis:
	// repo/path/workspace/projectName (no PR number - locks are per project, not per PR)
	lockID := models.GenerateLockKey(lock.Project, lock.Workspace)

	dbLock := &Lock{
		ID:           lockID,
		RepoFullName: lock.Project.RepoFullName,
		PullNum:      lock.Pull.Num,
		Workspace:    lock.Workspace,
		LockedBy:     lock.User.Username,
	}

	if lock.Project.ProjectName != "" {
		dbLock.ProjectName = &lock.Project.ProjectName
	}

	acquired, err := a.lockStore.TryLock(ctx, dbLock)
	if err != nil {
		return false, lock, err
	}

	if !acquired {
		// Lock already exists, retrieve it
		existing, err := a.lockStore.Get(ctx, dbLock.ID)
		if err != nil {
			return false, lock, err
		}

		// Convert back to models.ProjectLock
		existingProjectLock := models.ProjectLock{
			Pull: models.PullRequest{
				Num: existing.PullNum,
			},
			User: models.User{
				Username: existing.LockedBy,
			},
			Workspace: existing.Workspace,
			Project: models.Project{
				RepoFullName: existing.RepoFullName,
			},
			Time: existing.LockedAt,
		}

		return false, existingProjectLock, nil
	}

	return true, lock, nil
}

// Unlock releases a project lock
func (a *PostgresLockAdapter) Unlock(project models.Project, workspace string) (*models.ProjectLock, error) {
	ctx := context.Background()

	// Try to get the lock before deleting it
	// Note: This is a best-effort approach since we don't have the pull number
	locks, err := a.lockStore.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	var existing *Lock
	for _, lock := range locks {
		if lock.RepoFullName == project.RepoFullName && lock.Workspace == workspace {
			existing = lock
			break
		}
	}

	if existing == nil {
		return nil, fmt.Errorf("lock not found")
	}

	if err := a.lockStore.Unlock(ctx, existing.ID); err != nil {
		return nil, err
	}

	projectLock := &models.ProjectLock{
		Pull: models.PullRequest{
			Num: existing.PullNum,
		},
		User: models.User{
			Username: existing.LockedBy,
		},
		Workspace: existing.Workspace,
		Project:   project,
		Time:      existing.LockedAt,
	}

	return projectLock, nil
}

// List returns all current locks
func (a *PostgresLockAdapter) List() ([]models.ProjectLock, error) {
	ctx := context.Background()
	locks, err := a.lockStore.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	projectLocks := make([]models.ProjectLock, 0, len(locks))
	for _, lock := range locks {
		projectLock := models.ProjectLock{
			Pull: models.PullRequest{
				Num: lock.PullNum,
			},
			User: models.User{
				Username: lock.LockedBy,
			},
			Workspace: lock.Workspace,
			Project: models.Project{
				RepoFullName: lock.RepoFullName,
			},
			Time: lock.LockedAt,
		}
		if lock.ProjectName != nil {
			projectLock.Project.ProjectName = *lock.ProjectName
		}
		projectLocks = append(projectLocks, projectLock)
	}

	return projectLocks, nil
}

// GetLock returns a specific lock
func (a *PostgresLockAdapter) GetLock(project models.Project, workspace string) (*models.ProjectLock, error) {
	ctx := context.Background()

	// Search for lock matching repo and workspace
	locks, err := a.lockStore.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	for _, lock := range locks {
		if lock.RepoFullName == project.RepoFullName && lock.Workspace == workspace {
			projectLock := &models.ProjectLock{
				Pull: models.PullRequest{
					Num: lock.PullNum,
				},
				User: models.User{
					Username: lock.LockedBy,
				},
				Workspace: lock.Workspace,
				Project:   project,
				Time:      lock.LockedAt,
			}
			return projectLock, nil
		}
	}

	return nil, fmt.Errorf("lock not found")
}

// UnlockByPull unlocks all locks associated with a pull request
func (a *PostgresLockAdapter) UnlockByPull(repoFullName string, pullNum int) ([]models.ProjectLock, error) {
	ctx := context.Background()

	// Get all locks for this PR before deleting
	locks, err := a.lockStore.List(ctx, repoFullName, pullNum)
	if err != nil {
		return nil, err
	}

	// Convert to project locks
	projectLocks := make([]models.ProjectLock, 0, len(locks))
	for _, lock := range locks {
		projectLock := models.ProjectLock{
			Pull: models.PullRequest{
				Num: lock.PullNum,
			},
			User: models.User{
				Username: lock.LockedBy,
			},
			Workspace: lock.Workspace,
			Project: models.Project{
				RepoFullName: lock.RepoFullName,
			},
			Time: lock.LockedAt,
		}
		if lock.ProjectName != nil {
			projectLock.Project.ProjectName = *lock.ProjectName
		}
		projectLocks = append(projectLocks, projectLock)
	}

	// Unlock them
	if err := a.lockStore.UnlockByPull(ctx, repoFullName, pullNum); err != nil {
		return nil, err
	}

	return projectLocks, nil
}

// UpdateProjectStatus updates the plan status for a project
func (a *PostgresLockAdapter) UpdateProjectStatus(pull models.PullRequest, workspace string, repoRelDir string, newStatus models.ProjectPlanStatus) error {
	// This is a no-op for the PostgreSQL adapter as we don't store project status in locks
	// The legacy BoltDB implementation stores this, but for PostgreSQL we'd store it separately
	return nil
}

// GetPullStatus retrieves the status of a pull request from PostgreSQL
func (a *PostgresLockAdapter) GetPullStatus(pull models.PullRequest) (*models.PullStatus, error) {
	ctx := context.Background()

	query := `
		SELECT workspace, repo_rel_dir, project_name, status
		FROM pull_project_status
		WHERE repo_full_name = $1 AND pull_num = $2
		ORDER BY workspace, repo_rel_dir, project_name
	`

	rows, err := a.db.QueryContext(ctx, query, pull.BaseRepo.FullName, pull.Num)
	if err != nil {
		return nil, fmt.Errorf("querying pull status: %w", err)
	}
	defer rows.Close()

	var projects []models.ProjectStatus
	for rows.Next() {
		var workspace, repoRelDir, projectName string
		var status int

		if err := rows.Scan(&workspace, &repoRelDir, &projectName, &status); err != nil {
			return nil, fmt.Errorf("scanning pull status row: %w", err)
		}

		projects = append(projects, models.ProjectStatus{
			Workspace:   workspace,
			RepoRelDir:  repoRelDir,
			ProjectName: projectName,
			Status:      models.ProjectPlanStatus(status),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pull status rows: %w", err)
	}

	if len(projects) == 0 {
		return nil, nil
	}

	return &models.PullStatus{
		Projects: projects,
	}, nil
}

// DeletePullStatus deletes the status of a pull request from PostgreSQL
func (a *PostgresLockAdapter) DeletePullStatus(pull models.PullRequest) error {
	ctx := context.Background()

	query := `DELETE FROM pull_project_status WHERE repo_full_name = $1 AND pull_num = $2`

	_, err := a.db.ExecContext(ctx, query, pull.BaseRepo.FullName, pull.Num)
	if err != nil {
		return fmt.Errorf("deleting pull status: %w", err)
	}

	return nil
}

// UpdatePullWithResults updates pull status with command results in PostgreSQL
func (a *PostgresLockAdapter) UpdatePullWithResults(pull models.PullRequest, newResults []command.ProjectResult) (models.PullStatus, error) {
	ctx := context.Background()

	// Convert command results to project plan status
	for _, result := range newResults {
		var status models.ProjectPlanStatus

		// Determine status based on command and result
		switch result.Command {
		case command.Plan:
			if result.Error != nil || result.Failure != "" {
				status = models.ErroredPlanStatus
			} else if result.PlanSuccess != nil && result.PlanSuccess.NoChanges() {
				status = models.PlannedNoChangesPlanStatus
			} else {
				status = models.PlannedPlanStatus
			}
		case command.Apply:
			if result.Error != nil || result.Failure != "" {
				status = models.ErroredApplyStatus
			} else {
				status = models.AppliedPlanStatus
			}
		case command.PolicyCheck:
			if result.Error != nil || result.Failure != "" {
				status = models.ErroredPolicyCheckStatus
			} else {
				status = models.PassedPolicyCheckStatus
			}
		default:
			// For other commands, use a generic status
			if result.Error != nil || result.Failure != "" {
				status = models.ErroredPlanStatus
			} else {
				status = models.PlannedPlanStatus
			}
		}

		// Upsert the project status
		query := `
			INSERT INTO pull_project_status 
				(repo_full_name, pull_num, workspace, repo_rel_dir, project_name, status)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (repo_full_name, pull_num, workspace, repo_rel_dir, project_name)
			DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
		`

		_, err := a.db.ExecContext(ctx, query,
			pull.BaseRepo.FullName,
			pull.Num,
			result.Workspace,
			result.RepoRelDir,
			result.ProjectName,
			int(status),
		)

		if err != nil {
			return models.PullStatus{}, fmt.Errorf("updating project status: %w", err)
		}
	}

	// Retrieve and return the complete pull status
	pullStatus, err := a.GetPullStatus(pull)
	if err != nil {
		return models.PullStatus{}, fmt.Errorf("getting updated pull status: %w", err)
	}

	if pullStatus == nil {
		return models.PullStatus{Projects: []models.ProjectStatus{}}, nil
	}

	return *pullStatus, nil
}

// LockCommand locks a command (plan, apply, etc.)
func (a *PostgresLockAdapter) LockCommand(cmdName command.Name, lockTime time.Time) (*command.Lock, error) {
	ctx := context.Background()

	dbLock := &Lock{
		ID:           fmt.Sprintf("command:%s", cmdName.String()),
		RepoFullName: "global",
		PullNum:      0,
		Workspace:    "command",
		LockedBy:     "system",
		LockedAt:     lockTime,
	}

	acquired, err := a.lockStore.TryLock(ctx, dbLock)
	if err != nil {
		return nil, err
	}

	if !acquired {
		existing, err := a.lockStore.Get(ctx, dbLock.ID)
		if err != nil {
			return nil, err
		}

		return &command.Lock{
			CommandName: cmdName,
			LockMetadata: command.LockMetadata{
				UnixTime: existing.LockedAt.Unix(),
			},
		}, nil
	}

	return &command.Lock{
		CommandName: cmdName,
		LockMetadata: command.LockMetadata{
			UnixTime: lockTime.Unix(),
		},
	}, nil
}

// UnlockCommand unlocks a command
func (a *PostgresLockAdapter) UnlockCommand(cmdName command.Name) error {
	ctx := context.Background()
	lockID := fmt.Sprintf("command:%s", cmdName.String())
	return a.lockStore.Unlock(ctx, lockID)
}

// CheckCommandLock checks if a command is locked
func (a *PostgresLockAdapter) CheckCommandLock(cmdName command.Name) (*command.Lock, error) {
	ctx := context.Background()
	lockID := fmt.Sprintf("command:%s", cmdName.String())

	lock, err := a.lockStore.Get(ctx, lockID)
	if err != nil {
		// If lock not found, return nil (no lock) with no error
		// This matches BoltDB and Redis behavior
		if err.Error() == fmt.Sprintf("lock not found: %s", lockID) {
			return nil, nil
		}
		return nil, err
	}

	return &command.Lock{
		CommandName: cmdName,
		LockMetadata: command.LockMetadata{
			UnixTime: lock.LockedAt.Unix(),
		},
	}, nil
}

// Close closes the database connection
func (a *PostgresLockAdapter) Close() error {
	// The underlying PostgresDB connection is managed elsewhere
	return nil
}
