package db

import (
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrator handles database migrations
type Migrator struct {
	db *sql.DB
}

// NewMigrator creates a new migrator instance
func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

// Up runs all pending migrations with automatic dirty state recovery
func (m *Migrator) Up() error {
	// First check for dirty state without creating full migrator
	var currentVersion uint
	var isDirty bool

	// Query schema_migrations directly to check state
	err := m.db.QueryRow("SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&currentVersion, &isDirty)
	if err != nil && err.Error() != "sql: no rows in result set" {
		// Table might not exist yet, which is fine
		if !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("checking migration state: %w", err)
		}
	}

	// If dirty, we need to determine if the migration actually completed
	if isDirty {
		fmt.Printf("WARNING: Database is in dirty state at version %d\n", currentVersion)

		// Check if key tables from this version exist
		tablesExist := false
		switch currentVersion {
		case 1:
			// Version 1 creates agent_controllers
			tablesExist = m.tableExists("agent_controllers")
		case 2:
			// Version 2 creates locks
			tablesExist = m.tableExists("locks")
		case 3:
			// Version 3 creates jobs
			tablesExist = m.tableExists("jobs")
		case 4:
			// Version 4 creates plan_metadata
			tablesExist = m.tableExists("plan_metadata")
		case 5:
			// Version 5 creates job_audit_log and agent_labels
			tablesExist = m.tableExists("job_audit_log") && m.tableExists("agent_labels")
		case 6:
			// Version 6 adds columns to jobs
			tablesExist = m.columnExists("jobs", "priority")
		default:
			// Unknown version, assume it didn't complete
			tablesExist = false
		}

		if tablesExist {
			// Migration completed but was marked dirty, just clean it
			fmt.Printf("Migration %d appears complete, marking as clean\n", currentVersion)
			_, err := m.db.Exec("UPDATE schema_migrations SET dirty = false WHERE version = $1", currentVersion)
			if err != nil {
				return fmt.Errorf("failed to clean dirty state at version %d: %w", currentVersion, err)
			}
		} else {
			// Migration didn't complete, need to start over
			fmt.Printf("Migration %d did not complete, resetting to start fresh\n", currentVersion)
			// Delete the migration record so we start from scratch
			_, err := m.db.Exec("DELETE FROM schema_migrations WHERE version = $1", currentVersion)
			if err != nil {
				return fmt.Errorf("failed to reset migration state: %w", err)
			}
		}

		fmt.Printf("Successfully recovered from dirty state\n")
	}

	// Now create migrator and run migrations
	migrator, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}
	// Note: Don't close migrator as it closes the underlying *sql.DB connection
	// which we need to keep alive for the application

	// Run migrations
	if err := migrator.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("running migrations: %w", err)
	}

	return nil
}

// tableExists checks if a table exists in the database
func (m *Migrator) tableExists(tableName string) bool {
	var exists bool
	query := `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name = $1
	)`
	err := m.db.QueryRow(query, tableName).Scan(&exists)
	return err == nil && exists
}

// columnExists checks if a column exists in a table
func (m *Migrator) columnExists(tableName, columnName string) bool {
	var exists bool
	query := `SELECT EXISTS (
		SELECT FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1 
		AND column_name = $2
	)`
	err := m.db.QueryRow(query, tableName, columnName).Scan(&exists)
	return err == nil && exists
}

// Down rolls back all migrations
func (m *Migrator) Down() error {
	migrator, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}
	// Note: Don't close migrator as it closes the underlying *sql.DB connection

	if err := migrator.Down(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("rolling back migrations: %w", err)
	}

	return nil
}

// Steps runs n migration steps (positive = up, negative = down)
func (m *Migrator) Steps(n int) error {
	migrator, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}
	// Note: Don't close migrator as it closes the underlying *sql.DB connection

	if err := migrator.Steps(n); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("running %d migration steps: %w", n, err)
	}

	return nil
}

// Version returns the current migration version
func (m *Migrator) Version() (uint, bool, error) {
	migrator, err := m.createMigrator()
	if err != nil {
		return 0, false, fmt.Errorf("creating migrator: %w", err)
	}
	// Note: Don't close migrator as it closes the underlying *sql.DB connection

	version, dirty, err := migrator.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return 0, false, fmt.Errorf("getting migration version: %w", err)
	}

	return version, dirty, nil
}

// Force sets the migration version without running migrations
// Use with caution - only for fixing dirty state
func (m *Migrator) Force(version int) error {
	migrator, err := m.createMigrator()
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}
	// Note: Don't close migrator as it closes the underlying *sql.DB connection

	if err := migrator.Force(version); err != nil {
		return fmt.Errorf("forcing version %d: %w", version, err)
	}

	return nil
}

func (m *Migrator) createMigrator() (*migrate.Migrate, error) {
	// Create source driver from embedded filesystem
	sourceDriver, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("creating source driver: %w", err)
	}

	// Create database driver
	// IMPORTANT: Set MigrationsTable to prevent closing the underlying database connection
	// when migrator.Close() is called. The NoLock option also prevents the driver from
	// closing the connection when it's done.
	dbDriver, err := postgres.WithInstance(m.db, &postgres.Config{
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		return nil, fmt.Errorf("creating database driver: %w", err)
	}

	// Create migrator
	migrator, err := migrate.NewWithInstance(
		"iofs",
		sourceDriver,
		"postgres",
		dbDriver,
	)
	if err != nil {
		return nil, fmt.Errorf("creating migrator: %w", err)
	}

	return migrator, nil
}
