package qwr

import (
	"database/sql"
	"testing"
)

// TestBasicWrite verifies simple write operations.
func TestBasicWrite(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Insert a row
	_, err := mgr.Query(
		"INSERT INTO users (name) VALUES (?)",
		"Alice",
	).Execute()
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Verify the row exists
	row, err := mgr.Query("SELECT name FROM users WHERE id = ?", 1).ReadRow()
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if name != "Alice" {
		t.Errorf("got name %q, want %q", name, "Alice")
	}
}

// TestBasicRead verifies simple read operations.
func TestBasicRead(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Insert test data
	_, err := mgr.Query(
		"INSERT INTO users (name) VALUES (?), (?)",
		"Alice", "Bob",
	).Execute()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Read via manager
	rows, err := mgr.Query("SELECT name FROM users ORDER BY id").Read()
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		names = append(names, name)
	}

	if len(names) != 2 {
		t.Fatalf("got %d rows, want 2", len(names))
	}
	if names[0] != "Alice" || names[1] != "Bob" {
		t.Errorf("got names %v, want [Alice Bob]", names)
	}
}

// TestReadClose verifies ReadClose automatically handles row cleanup.
func TestReadClose(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Insert test data
	_, err := mgr.Query(
		"INSERT INTO users (name) VALUES (?), (?), (?)",
		"Alice", "Bob", "Charlie",
	).Execute()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Test ReadClose with automatic cleanup
	var names []string
	err = mgr.Query("SELECT name FROM users ORDER BY id").ReadClose(func(rows *sql.Rows) error {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReadClose failed: %v", err)
	}

	if len(names) != 3 {
		t.Fatalf("got %d rows, want 3", len(names))
	}
	if names[0] != "Alice" || names[1] != "Bob" || names[2] != "Charlie" {
		t.Errorf("got names %v, want [Alice Bob Charlie]", names)
	}
}

// TestUsePreparedStatements verifies the UsePreparedStatements option.
func TestUsePreparedStatements(t *testing.T) {
	opts := DefaultOptions
	opts.UsePreparedStatements = true

	mgr := newTestMgr(t, opts)
	defer mgr.Close()

	setupTable(t, mgr)

	// Query should automatically use prepared statements
	qb := mgr.Query("INSERT INTO users (name) VALUES (?)", "Alice")

	// Check that prepared flag is set from options
	if !qb.query.prepared {
		t.Error("expected query to use prepared statements when UsePreparedStatements=true")
	}

	// Execute the query
	_, err := qb.Execute()
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Verify data was written
	row, _ := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
	var count int
	row.Scan(&count)
	if count != 1 {
		t.Errorf("got %d rows, want 1", count)
	}
}
