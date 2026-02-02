package qwr

import "testing"

// newTestMgr creates an in-memory manager for testing.
// Uses a shared in-memory database so reader/writer see the same data.
// Caller is responsible for closing the returned manager.
func newTestMgr(t *testing.T, opts Options) *Manager {
	t.Helper()

	// Use file::memory:?cache=shared for shared in-memory database
	// This allows multiple connections to access the same in-memory database
	mgr, err := New("file::memory:?cache=shared", opts).Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	return mgr
}

// setupTable creates a simple test table.
func setupTable(t *testing.T, mgr *Manager) {
	t.Helper()

	_, err := mgr.Query(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
	).Execute()
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}
