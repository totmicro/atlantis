package db

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPostgresDB(t *testing.T) {
	// Note: This test requires a running PostgreSQL instance
	// Use docker: docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:15-alpine
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := Config{
		Host:            "localhost",
		Port:            5432,
		User:            "postgres",
		Password:        "test",
		Database:        "postgres",
		SSLMode:         "disable",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	db, err := NewPostgresDB(cfg)
	require.NoError(t, err)
	defer db.Close()

	// Test connection
	err = db.Ping()
	assert.NoError(t, err)

	// Test stats
	stats := db.Stats()
	assert.GreaterOrEqual(t, stats.MaxOpenConnections, 10)
}

func TestMigrations(t *testing.T) {
	// TestMigrations tests database migrations
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := Config{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "test",
		Database: "postgres",
		SSLMode:  "disable",
	}

	db, err := NewPostgresDB(cfg)
	require.NoError(t, err)
	defer db.Close()

	// Create a test database
	_, err = db.DB().Exec("DROP DATABASE IF EXISTS atlantis_test")
	require.NoError(t, err)
	_, err = db.DB().Exec("CREATE DATABASE atlantis_test")
	require.NoError(t, err)

	// Connect to test database
	testCfg := cfg
	testCfg.Database = "atlantis_test"
	testDB, err := NewPostgresDB(testCfg)
	require.NoError(t, err)
	defer testDB.Close()

	defer func() {
		testDB.Close()
		_, _ = db.DB().Exec("DROP DATABASE atlantis_test")
	}()

	// Test migrations
	migrator := NewMigrator(testDB.DB())

	// Get initial version
	version, dirty, err := migrator.Version()
	assert.Error(t, err) // No migrations yet
	assert.Equal(t, uint(0), version)
	assert.False(t, dirty)

	// Run migrations up
	err = migrator.Up()
	assert.NoError(t, err)

	// Check version after migrations
	version, dirty, err = migrator.Version()
	assert.NoError(t, err)
	assert.Greater(t, version, uint(0))
	assert.False(t, dirty)

	// Verify tables were created
	tables := []string{"jobs", "locks", "agent_controllers", "plan_metadata"}
	for _, table := range tables {
		var exists bool
		err := testDB.DB().QueryRow(
			"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
			table,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists, "table %s should exist", table)
	}

	// Test down migration
	err = migrator.Down()
	assert.NoError(t, err)

	// Verify tables were dropped
	for _, table := range tables {
		var exists bool
		err := testDB.DB().QueryRow(
			"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
			table,
		).Scan(&exists)
		assert.NoError(t, err)
		assert.False(t, exists, "table %s should not exist after down migration", table)
	}
}

// mockDB is a helper for tests that don't need a real database
func mockDB(t *testing.T) *sql.DB {
	// This would typically use sqlmock or similar
	// For now, we'll skip in tests that need it
	t.Skip("Mock database not implemented yet")
	return nil
}
