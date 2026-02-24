package qwr

import (
	"crypto/rand"
	"fmt"
	"testing"
)

// newTestMgr creates an in-memory manager for testing.
// Uses a unique shared in-memory database so reader and writer see the same data
// without interfering with other concurrent tests.
// Caller is responsible for closing the returned manager.
func newTestMgr(t *testing.T, opts Options) *Manager {
	t.Helper()

	// Use a unique name per test to avoid interference.
	// rand.Text() provides a secure, random string (available from Go 1.24).
	dbName := fmt.Sprintf("test_%s", rand.Text())
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", dbName)

	mgr, err := New(dsn, opts).Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	return mgr
}

// setupTable creates a simple test table.
func setupTable(t *testing.T, mgr *Manager) {
	t.Helper()

	_, err := mgr.Query(
		"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)",
	).Execute()
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}
