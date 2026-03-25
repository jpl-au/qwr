package qwr

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/jpl-au/qwr/profile"
)

func TestAttachBuilder(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "analytics.db")

	// Create the analytics database with a table
	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Attach("analytics", attachDB, profile.Attached().
			WithJournalMode(profile.JournalWal)).
		Open()
	if err != nil {
		t.Fatalf("Open with Attach failed: %v", err)
	}
	defer mgr.Close()

	// Create a table in main
	_, err = mgr.Query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").Execute()
	if err != nil {
		t.Fatalf("create main table: %v", err)
	}

	// Insert into main
	_, err = mgr.Query("INSERT INTO users (name) VALUES (?)", "Alice").Execute()
	if err != nil {
		t.Fatalf("insert main: %v", err)
	}

	// Insert into attached database
	_, err = mgr.Query("INSERT INTO analytics.events (action) VALUES (?)", "login").Execute()
	if err != nil {
		t.Fatalf("insert attached: %v", err)
	}

	// Cross-database read
	var name, action string
	row, err := mgr.Query(
		"SELECT u.name, e.action FROM users u, analytics.events e WHERE u.name = 'Alice'",
	).ReadRow()
	if err != nil {
		t.Fatalf("cross-db query setup: %v", err)
	}
	if err := row.Scan(&name, &action); err != nil {
		t.Fatalf("cross-db scan: %v", err)
	}
	if name != "Alice" || action != "login" {
		t.Errorf("got name=%q action=%q, want Alice/login", name, action)
	}
}

func TestAttachSecondFileDB(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	cacheDB := filepath.Join(dir, "cache.db")

	mgr, err := New(mainDB).
		Attach("cache", cacheDB).
		Open()
	if err != nil {
		t.Fatalf("Open with file attach failed: %v", err)
	}
	defer mgr.Close()

	// Create a table in the attached database
	_, err = mgr.Query("CREATE TABLE cache.entries (key TEXT, val TEXT)").Execute()
	if err != nil {
		t.Fatalf("create cache table: %v", err)
	}

	_, err = mgr.Query("INSERT INTO cache.entries (key, val) VALUES (?, ?)", "k1", "v1").Execute()
	if err != nil {
		t.Fatalf("insert cache: %v", err)
	}

	var val string
	row, err := mgr.Query("SELECT val FROM cache.entries WHERE key = ?", "k1").ReadRow()
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if err := row.Scan(&val); err != nil {
		t.Fatalf("scan cache: %v", err)
	}
	if val != "v1" {
		t.Errorf("got val=%q, want v1", val)
	}
}

func TestAttachReaderPool(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "extra.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Attach("extra", attachDB).
		Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer mgr.Close()

	// Read from attached database via the reader pool
	var count int
	row, err := mgr.Query("SELECT COUNT(*) FROM extra.events").ReadRow()
	if err != nil {
		t.Fatalf("reader query: %v", err)
	}
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan: %v", err)
	}
	// Table exists but is empty
	if count != 0 {
		t.Errorf("got count=%d, want 0", count)
	}
}

func TestAttachTransaction(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "other.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Attach("other", attachDB).
		Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer mgr.Close()

	// Setup main table
	_, err = mgr.Query("CREATE TABLE log (msg TEXT)").Execute()
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Cross-database transaction
	_, err = mgr.Transaction(2).
		Add("INSERT INTO log (msg) VALUES (?)", "cross-db").
		Add("INSERT INTO other.events (action) VALUES (?)", "tx-action").
		Write()
	if err != nil {
		t.Fatalf("cross-db transaction: %v", err)
	}

	// Verify both inserts
	var msg string
	row, err := mgr.Query("SELECT msg FROM log").ReadRow()
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if err := row.Scan(&msg); err != nil {
		t.Fatalf("scan log: %v", err)
	}
	if msg != "cross-db" {
		t.Errorf("got msg=%q, want cross-db", msg)
	}

	var action string
	row, err = mgr.Query("SELECT action FROM other.events").ReadRow()
	if err != nil {
		t.Fatalf("read other: %v", err)
	}
	if err := row.Scan(&action); err != nil {
		t.Fatalf("scan other: %v", err)
	}
	if action != "tx-action" {
		t.Errorf("got action=%q, want tx-action", action)
	}
}

func TestAttachRuntimeAttach(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "runtime.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer mgr.Close()

	// Attach at runtime
	if err := mgr.Attach("runtime", attachDB); err != nil {
		t.Fatalf("runtime Attach: %v", err)
	}

	// Writer should have immediate access
	_, err = mgr.Query("INSERT INTO runtime.events (action) VALUES (?)", "dynamic").Execute()
	if err != nil {
		t.Fatalf("write to runtime-attached db: %v", err)
	}

	// Reset reader pool for immediate reader access
	if err := mgr.ResetReaderPool(); err != nil {
		t.Fatalf("ResetReaderPool: %v", err)
	}

	var action string
	row, err := mgr.Query("SELECT action FROM runtime.events WHERE action = ?", "dynamic").ReadRow()
	if err != nil {
		t.Fatalf("read runtime-attached: %v", err)
	}
	if err := row.Scan(&action); err != nil {
		t.Fatalf("scan runtime: %v", err)
	}
	if action != "dynamic" {
		t.Errorf("got action=%q, want dynamic", action)
	}
}

func TestAttachDuplicateAlias(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "dup.db")

	setupAttachDB(t, attachDB)

	_, err := New(mainDB).
		Attach("dup", attachDB).
		Attach("dup", attachDB).
		Open()
	if err == nil {
		t.Fatal("expected error for duplicate alias, got nil")
	}
}

func TestAttachReservedAlias(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	_, err := New(mainDB).
		Attach("main", "other.db").
		Open()
	if err == nil {
		t.Fatal("expected error for reserved alias 'main', got nil")
	}

	_, err = New(mainDB).
		Attach("temp", "other.db").
		Open()
	if err == nil {
		t.Fatal("expected error for reserved alias 'temp', got nil")
	}
}

func TestAttachNewSQLRejected(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = NewSQL(db, db).
		Attach("extra", "extra.db").
		Open()
	if err != ErrAttachNotSupported {
		t.Fatalf("expected ErrAttachNotSupported, got %v", err)
	}
}

func TestAttachRuntimeDuplicateRejected(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "extra.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Attach("extra", attachDB).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	// Should fail because "extra" is already attached
	err = mgr.Attach("extra", attachDB)
	if err == nil {
		t.Fatal("expected error for duplicate runtime attach")
	}
}

func TestAttachPerSchemaCheckpoint(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "ckpt.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Attach("ckpt", attachDB, profile.Attached().WithJournalMode(profile.JournalWal)).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	// Write some data to trigger WAL
	_, err = mgr.Query("INSERT INTO ckpt.events (action) VALUES (?)", "data").Execute()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Per-schema checkpoint should not error
	if err := mgr.RunCheckpoint("PASSIVE", "ckpt"); err != nil {
		t.Fatalf("per-schema checkpoint: %v", err)
	}
}

// setupAttachDB creates a database file with an events table.
func setupAttachDB(t *testing.T, path string) {
	t.Helper()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open attach db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE events (id INTEGER PRIMARY KEY, action TEXT)"); err != nil {
		t.Fatalf("create events table: %v", err)
	}
}
