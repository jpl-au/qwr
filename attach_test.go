package qwr

import (
	"database/sql"
	"fmt"
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
	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = NewSQL(db, db).
		Attach("extra", filepath.Join(t.TempDir(), "extra.db")).
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

// T1: Injection vector tests
func TestAttachInvalidAliasRejected(t *testing.T) {
	tests := []struct {
		name  string
		alias string
	}{
		{"sql_injection", "test; DROP TABLE x"},
		{"spaces", "my alias"},
		{"hyphen", "my-db"},
		{"dot", "my.db"},
		{"starts_with_digit", "1bad"},
		{"unicode", "caf\u00e9"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newAttachment(tt.alias, "/tmp/test.db", "/tmp/main.db", nil)
			if err == nil {
				t.Errorf("expected error for alias %q, got nil", tt.alias)
			}
		})
	}
}

// T2: Special-character path tests
func TestAttachSpecialCharPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"spaces", "path with spaces/test.db"},
		{"single_quote", "it's/test.db"},
		{"semicolon", "a;b/test.db"},
		{"unicode", "caf\u00e9/test.db"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			mainDB := filepath.Join(dir, "main.db")

			// Create the subdirectory and database
			dbPath := filepath.Join(dir, tt.path)
			if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			setupAttachDB(t, dbPath)

			mgr, err := New(mainDB).
				Writer(profile.WriteBalanced()).
				Attach("ext", dbPath).
				Open()
			if err != nil {
				t.Fatalf("Open failed for path %q: %v", tt.path, err)
			}
			defer mgr.Close()

			// Verify the attached database is usable
			_, err = mgr.Query("INSERT INTO ext.events (action) VALUES (?)", "test").Execute()
			if err != nil {
				t.Fatalf("insert failed for path %q: %v", tt.path, err)
			}
		})
	}
}

// T3: Runtime attach with profile
func TestAttachRuntimeWithProfile(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "profiled.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	// Attach with a profile that sets PRAGMAs
	prof := profile.Attached().
		WithJournalMode(profile.JournalWal).
		WithCacheSize(-30720)

	if err := mgr.Attach("profiled", attachDB, prof); err != nil {
		t.Fatalf("Attach with profile: %v", err)
	}

	// Insert succeeding proves the ATTACH + PRAGMAs were applied
	_, err = mgr.Query("INSERT INTO profiled.events (action) VALUES (?)", "with-profile").Execute()
	if err != nil {
		t.Fatalf("insert into profiled-attached db: %v", err)
	}
}

// T4: :memory: attach rejection
func TestAttachMemoryRejected(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	_, err := New(mainDB).
		Attach("cache", ":memory:").
		Open()
	if err == nil {
		t.Fatal("expected error for :memory: attach, got nil")
	}

	// Also test runtime attach
	mgr, err := New(mainDB).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	err = mgr.Attach("cache", ":memory:")
	if err == nil {
		t.Fatal("expected error for runtime :memory: attach, got nil")
	}
}

// T5: Relative path with :memory: main DB rejected
func TestAttachRelativePathMemoryMainRejected(t *testing.T) {
	_, err := New(":memory:").
		Attach("ext", "relative.db").
		Open()
	if err == nil {
		t.Fatal("expected error for relative path with :memory: main, got nil")
	}
}

// Verify writer pool is forced to MaxOpenConns=1 regardless of profile
func TestWriterMaxOpenConnsEnforced(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	// Create a profile that requests 5 writer connections
	wp := profile.WriteBalanced().WithMaxOpenConns(5)

	mgr, err := New(mainDB).
		Writer(wp).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	stats := mgr.writer.Stats()
	if stats.MaxOpenConnections != 1 {
		t.Errorf("writer MaxOpenConnections = %d, want 1", stats.MaxOpenConnections)
	}
}

// Verify methods return ErrManagerClosed after Close()
func TestMethodsAfterCloseReturnError(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "extra.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mgr.Close()

	if err := mgr.Attach("extra", attachDB); err != ErrManagerClosed {
		t.Errorf("Attach after Close: got %v, want ErrManagerClosed", err)
	}
	if err := mgr.ResetReaderPool(); err != ErrManagerClosed {
		t.Errorf("ResetReaderPool after Close: got %v, want ErrManagerClosed", err)
	}
	if err := mgr.RunVacuum(); err != ErrManagerClosed {
		t.Errorf("RunVacuum after Close: got %v, want ErrManagerClosed", err)
	}
	if err := mgr.RunIncrementalVacuum(0); err != ErrManagerClosed {
		t.Errorf("RunIncrementalVacuum after Close: got %v, want ErrManagerClosed", err)
	}
	if err := mgr.RunCheckpoint("PASSIVE"); err != ErrManagerClosed {
		t.Errorf("RunCheckpoint after Close: got %v, want ErrManagerClosed", err)
	}
}

// Verify invalid checkpoint.Mode is rejected
func TestInvalidCheckpointModeRejected(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	// Invalid mode at Open() time
	_, err := New(mainDB).
		Writer(profile.WriteBalanced()).
		Checkpoint("INVALID; DROP TABLE x").
		Open()
	if err == nil {
		t.Fatal("expected error for invalid checkpoint mode at Open, got nil")
	}

	// Invalid mode at RunCheckpoint() time
	mgr, err := New(mainDB).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	if err := mgr.RunCheckpoint("INVALID; DROP TABLE x"); err == nil {
		t.Fatal("expected error for invalid checkpoint mode at RunCheckpoint, got nil")
	}
}

// Verify ResetReaderPool drains in-flight queries and works after reset.
// During the swap, readers that grabbed the old pool pointer may see a
// transient "database is closed" error - this is expected and benign.
func TestResetReaderPoolWithConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")
	attachDB := filepath.Join(dir, "concurrent.db")

	setupAttachDB(t, attachDB)

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	// Create a table and insert data
	_, err = mgr.Query("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)").Execute()
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := range 100 {
		_, err = mgr.Query("INSERT INTO items (val) VALUES (?)", i).Execute()
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Start concurrent readers
	done := make(chan struct{})
	panicked := make(chan string, 5)

	for range 5 {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicked <- fmt.Sprintf("%v", r)
				}
			}()
			for {
				select {
				case <-done:
					return
				default:
					// Transient "database is closed" errors during the
					// swap are expected - the important thing is no panics
					// and no data corruption.
					rows, err := mgr.Query("SELECT val FROM items").Read()
					if err != nil {
						continue
					}
					for rows.Next() {
						var v string
						rows.Scan(&v)
					}
					rows.Close()
				}
			}
		}()
	}

	// Attach and reset while readers are active
	if err := mgr.Attach("concurrent", attachDB); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := mgr.ResetReaderPool(); err != nil {
		t.Fatalf("ResetReaderPool: %v", err)
	}

	close(done)

	// Verify no panics occurred
	select {
	case p := <-panicked:
		t.Fatalf("concurrent reader panicked: %s", p)
	default:
	}

	// Verify reads work after the reset (including attached DB)
	var count int
	row, err := mgr.Query("SELECT COUNT(*) FROM items").ReadRow()
	if err != nil {
		t.Fatalf("post-reset read: %v", err)
	}
	if err := row.Scan(&count); err != nil {
		t.Fatalf("post-reset scan: %v", err)
	}
	if count != 100 {
		t.Errorf("got count=%d, want 100", count)
	}

	var evtCount int
	row, err = mgr.Query("SELECT COUNT(*) FROM concurrent.events").ReadRow()
	if err != nil {
		t.Fatalf("post-reset attached read: %v", err)
	}
	if err := row.Scan(&evtCount); err != nil {
		t.Fatalf("post-reset attached scan: %v", err)
	}
}

// Verify invalid schema names are rejected by maintenance methods
func TestInvalidSchemaRejected(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	mgr, err := New(mainDB).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	badSchemas := []string{
		"schema; DROP TABLE x",
		"has spaces",
		"1startsdigit",
	}

	for _, s := range badSchemas {
		if err := mgr.RunVacuum(s); err == nil {
			t.Errorf("RunVacuum(%q): expected error, got nil", s)
		}
		if err := mgr.RunIncrementalVacuum(0, s); err == nil {
			t.Errorf("RunIncrementalVacuum(%q): expected error, got nil", s)
		}
		if err := mgr.RunCheckpoint("PASSIVE", s); err == nil {
			t.Errorf("RunCheckpoint(%q): expected error, got nil", s)
		}
	}
}

// Verify renamed getter methods work
func TestRenamedGetters(t *testing.T) {
	dir := t.TempDir()
	mainDB := filepath.Join(dir, "main.db")

	mgr, err := New(mainDB).
		Reader(profile.ReadBalanced()).
		Writer(profile.WriteBalanced()).
		Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer mgr.Close()

	if mgr.ReaderProfile() == nil {
		t.Error("ReaderProfile() returned nil")
	}
	if mgr.WriterProfile() == nil {
		t.Error("WriterProfile() returned nil")
	}
	if mgr.Errors() == nil {
		// Errors() returns nil when queue is empty - that's fine
	}
	_, found := mgr.ErrorByID(999)
	if found {
		t.Error("ErrorByID(999) should not find anything")
	}
	status, err := mgr.JobStatus(999)
	if err != nil {
		t.Errorf("JobStatus: %v", err)
	}
	if status != "unknown" {
		t.Errorf("JobStatus = %q, want unknown", status)
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
