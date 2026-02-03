package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// LockStore handles distributed lock operations
type LockStore interface {
	TryLock(ctx context.Context, lock *Lock) (bool, error)
	Unlock(ctx context.Context, lockID string) error
	UnlockByUser(ctx context.Context, lockID string, username string) error
	Get(ctx context.Context, lockID string) (*Lock, error)
	List(ctx context.Context, repo string, pullNum int) ([]*Lock, error)
	ListAll(ctx context.Context) ([]*Lock, error)
	UnlockByPull(ctx context.Context, repo string, pullNum int) error
}

// PostgresLockStore implements LockStore using PostgreSQL
type PostgresLockStore struct {
	db *sql.DB
}

// NewPostgresLockStore creates a new PostgreSQL lock store
func NewPostgresLockStore(db *sql.DB) *PostgresLockStore {
	return &PostgresLockStore{db: db}
}

// TryLock attempts to acquire a lock
func (s *PostgresLockStore) TryLock(ctx context.Context, lock *Lock) (bool, error) {
	if lock.LockedAt.IsZero() {
		lock.LockedAt = time.Now()
	}

	// Use the PostgreSQL function from migration
	query := `SELECT try_acquire_lock($1, $2, $3, $4, $5, $6, $7)`

	var acquired bool
	err := s.db.QueryRowContext(
		ctx,
		query,
		lock.ID,
		lock.RepoFullName,
		lock.PullNum,
		lock.Workspace,
		nilIfEmpty(lock.ProjectName),
		lock.LockedBy,
		lock.ExpiresAt,
	).Scan(&acquired)

	if err != nil {
		return false, fmt.Errorf("trying to acquire lock: %w", err)
	}

	return acquired, nil
}

// Unlock releases a lock without checking ownership
func (s *PostgresLockStore) Unlock(ctx context.Context, lockID string) error {
	query := `SELECT release_lock($1, NULL)`

	var released bool
	err := s.db.QueryRowContext(ctx, query, lockID).Scan(&released)
	if err != nil {
		return fmt.Errorf("releasing lock: %w", err)
	}

	if !released {
		return fmt.Errorf("lock not found: %s", lockID)
	}

	return nil
}

// UnlockByUser releases a lock with ownership verification
func (s *PostgresLockStore) UnlockByUser(ctx context.Context, lockID string, username string) error {
	query := `SELECT release_lock($1, $2)`

	var released bool
	err := s.db.QueryRowContext(ctx, query, lockID, username).Scan(&released)
	if err != nil {
		return fmt.Errorf("releasing lock: %w", err)
	}

	if !released {
		return fmt.Errorf("lock not found or not owned by user: %s", lockID)
	}

	return nil
}

// Get retrieves a lock by ID
func (s *PostgresLockStore) Get(ctx context.Context, lockID string) (*Lock, error) {
	query := `
		SELECT id, repo_full_name, pull_num, workspace, project_name, locked_by, 
			locked_at, expires_at, lock_metadata
		FROM locks
		WHERE id = $1
	`

	lock := &Lock{}
	var projectName sql.NullString
	var expiresAt sql.NullTime
	var metadata []byte // Use []byte instead of json.RawMessage to handle NULL

	err := s.db.QueryRowContext(ctx, query, lockID).Scan(
		&lock.ID,
		&lock.RepoFullName,
		&lock.PullNum,
		&lock.Workspace,
		&projectName,
		&lock.LockedBy,
		&lock.LockedAt,
		&expiresAt,
		&metadata,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("lock not found: %s", lockID)
	}
	if err != nil {
		return nil, fmt.Errorf("getting lock: %w", err)
	}

	if projectName.Valid {
		lock.ProjectName = &projectName.String
	}

	if expiresAt.Valid {
		lock.ExpiresAt = &expiresAt.Time
	}

	// Only set metadata if not NULL
	if len(metadata) > 0 {
		lock.Metadata = metadata
	}

	return lock, nil
}

// List retrieves all locks for a repo and pull request
func (s *PostgresLockStore) List(ctx context.Context, repo string, pullNum int) ([]*Lock, error) {
	query := `
		SELECT id, repo_full_name, pull_num, workspace, project_name, locked_by, 
			locked_at, expires_at
		FROM locks
		WHERE repo_full_name = $1 AND pull_num = $2
		ORDER BY locked_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, repo, pullNum)
	if err != nil {
		return nil, fmt.Errorf("listing locks: %w", err)
	}
	defer rows.Close()

	var locks []*Lock
	for rows.Next() {
		lock := &Lock{}
		var projectName sql.NullString

		err := rows.Scan(
			&lock.ID,
			&lock.RepoFullName,
			&lock.PullNum,
			&lock.Workspace,
			&projectName,
			&lock.LockedBy,
			&lock.LockedAt,
			&lock.ExpiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning lock: %w", err)
		}

		if projectName.Valid {
			lock.ProjectName = &projectName.String
		}

		locks = append(locks, lock)
	}

	return locks, nil
}

// ListAll retrieves all locks
func (s *PostgresLockStore) ListAll(ctx context.Context) ([]*Lock, error) {
	query := `
		SELECT id, repo_full_name, pull_num, workspace, project_name, locked_by, 
			locked_at, expires_at
		FROM locks
		ORDER BY locked_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("listing all locks: %w", err)
	}
	defer rows.Close()

	var locks []*Lock
	for rows.Next() {
		lock := &Lock{}
		var projectName sql.NullString

		err := rows.Scan(
			&lock.ID,
			&lock.RepoFullName,
			&lock.PullNum,
			&lock.Workspace,
			&projectName,
			&lock.LockedBy,
			&lock.LockedAt,
			&lock.ExpiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning lock: %w", err)
		}

		if projectName.Valid {
			lock.ProjectName = &projectName.String
		}

		locks = append(locks, lock)
	}

	return locks, nil
}

// UnlockByPull unlocks all locks for a pull request
func (s *PostgresLockStore) UnlockByPull(ctx context.Context, repo string, pullNum int) error {
	query := `
		DELETE FROM locks
		WHERE repo_full_name = $1 AND pull_num = $2
	`

	result, err := s.db.ExecContext(ctx, query, repo, pullNum)
	if err != nil {
		return fmt.Errorf("unlocking by pull: %w", err)
	}

	// Note: rows == 0 is not an error - it just means there were no locks to delete
	// This matches behavior of BoltDB, Redis, and NoOpLocker implementations
	rows, _ := result.RowsAffected()
	if rows == 0 {
		// No locks found, but this is not an error
		return nil
	}

	return nil
}

// GenerateLockID generates a lock ID from components
func GenerateLockID(repo string, pullNum int, workspace, project string) string {
	if project == "" {
		return fmt.Sprintf("%s/%d/%s", repo, pullNum, workspace)
	}
	return fmt.Sprintf("%s/%d/%s/%s", repo, pullNum, workspace, project)
}

// Helper to convert empty string pointer to nil
func nilIfEmpty(s *string) interface{} {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
