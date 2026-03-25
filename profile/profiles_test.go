package profile

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestNew(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
	if p.pragmas == nil {
		t.Error("Pragmas map not initialized")
	}
	if len(p.pragmas) != 0 {
		t.Errorf("Pragmas map should be empty, got %d entries", len(p.pragmas))
	}
}

func TestProfileBuilder(t *testing.T) {
	p := New().
		WithMaxOpenConns(10).
		WithMaxIdleConns(5).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-102400).
		WithMMapSize(268435456).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumIncremental).
		WithPageSize(8192).
		WithBusyTimeout(5000).
		WithLockingMode(LockingNormal).
		WithRecursiveTriggers(true).
		WithSecureDelete(false).
		WithQueryOnly(false)

	if p.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns = %d, want 10", p.MaxOpenConns)
	}
	if p.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns = %d, want 5", p.MaxIdleConns)
	}
	if p.ConnMaxLifetime != time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want %v", p.ConnMaxLifetime, time.Hour)
	}
	if p.pragmas[PragmaJournalMode] != "WAL" {
		t.Errorf("journal_mode = %v, want WAL", p.pragmas[PragmaJournalMode])
	}
	if p.pragmas[PragmaSynchronous] != "1" {
		t.Errorf("synchronous = %v, want 1", p.pragmas[PragmaSynchronous])
	}
	if p.pragmas[PragmaForeignKeys] != "ON" {
		t.Errorf("foreign_keys = %v, want ON", p.pragmas[PragmaForeignKeys])
	}
	if p.pragmas[PragmaCacheSize] != -102400 {
		t.Errorf("cache_size = %v, want -102400", p.pragmas[PragmaCacheSize])
	}
	if p.pragmas[PragmaPageSize] != 8192 {
		t.Errorf("page_size = %v, want 8192", p.pragmas[PragmaPageSize])
	}
}

func TestForeignKeysDisabled(t *testing.T) {
	p := New().WithForeignKeys(false)
	if p.pragmas[PragmaForeignKeys] != "OFF" {
		t.Errorf("foreign_keys = %v, want OFF", p.pragmas[PragmaForeignKeys])
	}
}

func TestRecursiveTriggersDisabled(t *testing.T) {
	p := New().WithRecursiveTriggers(false)
	if p.pragmas[PragmaRecursiveTriggers] != "OFF" {
		t.Errorf("recursive_triggers = %v, want OFF", p.pragmas[PragmaRecursiveTriggers])
	}
}

func TestSecureDeleteEnabled(t *testing.T) {
	p := New().WithSecureDelete(true)
	if p.pragmas[PragmaSecureDelete] != "ON" {
		t.Errorf("secure_delete = %v, want ON", p.pragmas[PragmaSecureDelete])
	}
}

func TestQueryOnlyEnabled(t *testing.T) {
	p := New().WithQueryOnly(true)
	if p.pragmas[PragmaQueryOnly] != "ON" {
		t.Errorf("query_only = %v, want ON", p.pragmas[PragmaQueryOnly])
	}
}

func TestClone(t *testing.T) {
	original := New().
		WithMaxOpenConns(10).
		WithMaxIdleConns(5).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithCacheSize(-102400)

	clone := original.Clone()

	// Verify values are copied
	if clone.MaxOpenConns != original.MaxOpenConns {
		t.Errorf("Clone MaxOpenConns = %d, want %d", clone.MaxOpenConns, original.MaxOpenConns)
	}
	if clone.MaxIdleConns != original.MaxIdleConns {
		t.Errorf("Clone MaxIdleConns = %d, want %d", clone.MaxIdleConns, original.MaxIdleConns)
	}
	if clone.pragmas[PragmaJournalMode] != original.pragmas[PragmaJournalMode] {
		t.Errorf("Clone journal_mode = %v, want %v", clone.pragmas[PragmaJournalMode], original.pragmas[PragmaJournalMode])
	}

	// Verify deep copy - modifying clone shouldn't affect original
	clone.MaxOpenConns = 20
	clone.pragmas[PragmaJournalMode] = "DELETE"

	if original.MaxOpenConns == 20 {
		t.Error("Modifying clone affected original MaxOpenConns")
	}
	if original.pragmas[PragmaJournalMode] == "DELETE" {
		t.Error("Modifying clone affected original Pragmas")
	}
}

func TestString(t *testing.T) {
	p := New().
		WithMaxOpenConns(10).
		WithMaxIdleConns(5).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal)

	s := p.String()

	if !strings.Contains(s, "MaxOpenConns: 10") {
		t.Errorf("String() missing MaxOpenConns, got: %s", s)
	}
	if !strings.Contains(s, "MaxIdleConns: 5") {
		t.Errorf("String() missing MaxIdleConns, got: %s", s)
	}
	if !strings.Contains(s, "ConnMaxLifetime:") {
		t.Errorf("String() missing ConnMaxLifetime, got: %s", s)
	}
	if !strings.Contains(s, "journal_mode: WAL") {
		t.Errorf("String() missing journal_mode, got: %s", s)
	}
}

func TestStringWithoutLifetime(t *testing.T) {
	p := New().WithMaxOpenConns(10).WithMaxIdleConns(5)
	s := p.String()

	if strings.Contains(s, "ConnMaxLifetime") {
		t.Errorf("String() should not contain ConnMaxLifetime when zero, got: %s", s)
	}
}

func TestApply(t *testing.T) {
	// Use temp file since :memory: doesn't support WAL mode
	tmpFile := t.TempDir() + "/test.db"
	db, err := sql.Open("sqlite", tmpFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	p := New().
		WithMaxOpenConns(5).
		WithMaxIdleConns(2).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-10240).
		WithBusyTimeout(5000)

	if err := p.Apply(db); err != nil {
		t.Fatalf("Apply() error: %v", err)
	}

	// Verify pragmas were applied
	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("Failed to query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("journal_mode = %s, want wal", journalMode)
	}

	var foreignKeys int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("Failed to query foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("foreign_keys = %d, want 1", foreignKeys)
	}
}

func TestApplyRejectsUnknownPragma(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	p := New()
	p.pragmas[Pragma("invalid; name")] = "value"

	err = p.Apply(db)
	if err == nil {
		t.Error("Apply() should reject invalid pragma name")
	}
	if !strings.Contains(err.Error(), "not a valid identifier") {
		t.Errorf("Error should mention invalid identifier, got: %v", err)
	}
}

func TestJournalModeConstants(t *testing.T) {
	tests := []struct {
		mode JournalMode
		want string
	}{
		{JournalDelete, "DELETE"},
		{JournalTruncate, "TRUNCATE"},
		{JournalPersist, "PERSIST"},
		{JournalMemory, "MEMORY"},
		{JournalWal, "WAL"},
		{JournalOff, "OFF"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("JournalMode %v = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}

func TestSynchronousModeConstants(t *testing.T) {
	tests := []struct {
		mode SynchronousMode
		want string
	}{
		{SyncOff, "0"},
		{SyncNormal, "1"},
		{SyncFull, "2"},
		{SyncExtra, "3"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("SynchronousMode %v = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}

func TestTempStoreConstants(t *testing.T) {
	tests := []struct {
		store TempStore
		want  string
	}{
		{TempStoreDefault, "DEFAULT"},
		{TempStoreFile, "FILE"},
		{TempStoreMemory, "MEMORY"},
	}

	for _, tt := range tests {
		if string(tt.store) != tt.want {
			t.Errorf("TempStore %v = %q, want %q", tt.store, string(tt.store), tt.want)
		}
	}
}

func TestAutoVacuumConstants(t *testing.T) {
	tests := []struct {
		mode AutoVacuum
		want string
	}{
		{AutoVacuumNone, "NONE"},
		{AutoVacuumFull, "FULL"},
		{AutoVacuumIncremental, "INCREMENTAL"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("AutoVacuum %v = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}

func TestLockingModeConstants(t *testing.T) {
	tests := []struct {
		mode LockingMode
		want string
	}{
		{LockingNormal, "NORMAL"},
		{LockingExclusive, "EXCLUSIVE"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("LockingMode %v = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}

func TestReadProfiles(t *testing.T) {
	profiles := []struct {
		name    string
		profile *Profile
		conns   int
	}{
		{"ReadLight", ReadLight(), 5},
		{"ReadBalanced", ReadBalanced(), 10},
		{"ReadHeavy", ReadHeavy(), 25},
	}

	for _, tt := range profiles {
		t.Run(tt.name, func(t *testing.T) {
			if tt.profile.MaxOpenConns != tt.conns {
				t.Errorf("%s MaxOpenConns = %d, want %d", tt.name, tt.profile.MaxOpenConns, tt.conns)
			}
			if tt.profile.pragmas[PragmaJournalMode] != "WAL" {
				t.Errorf("%s journal_mode = %v, want WAL", tt.name, tt.profile.pragmas[PragmaJournalMode])
			}
			if tt.profile.pragmas[PragmaForeignKeys] != "ON" {
				t.Errorf("%s foreign_keys = %v, want ON", tt.name, tt.profile.pragmas[PragmaForeignKeys])
			}
		})
	}
}

func TestWriteProfiles(t *testing.T) {
	profiles := []struct {
		name    string
		profile *Profile
	}{
		{"WriteLight", WriteLight()},
		{"WriteBalanced", WriteBalanced()},
		{"WriteHeavy", WriteHeavy()},
	}

	for _, tt := range profiles {
		t.Run(tt.name, func(t *testing.T) {
			// All write profiles should have single connection due to SQLite constraint
			if tt.profile.MaxOpenConns != 1 {
				t.Errorf("%s MaxOpenConns = %d, want 1 (SQLite single writer)", tt.name, tt.profile.MaxOpenConns)
			}
			if tt.profile.MaxIdleConns != 1 {
				t.Errorf("%s MaxIdleConns = %d, want 1", tt.name, tt.profile.MaxIdleConns)
			}
			if tt.profile.pragmas[PragmaJournalMode] != "WAL" {
				t.Errorf("%s journal_mode = %v, want WAL", tt.name, tt.profile.pragmas[PragmaJournalMode])
			}
		})
	}
}

func TestProfileApplyToDatabase(t *testing.T) {
	profiles := []*Profile{
		ReadLight(),
		ReadBalanced(),
		ReadHeavy(),
		WriteLight(),
		WriteBalanced(),
		WriteHeavy(),
	}

	for i, p := range profiles {
		db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
		if err != nil {
			t.Fatalf("Profile %d: Failed to open database: %v", i, err)
		}

		if err := p.Apply(db); err != nil {
			t.Errorf("Profile %d: Apply() error: %v", i, err)
		}

		db.Close()
	}
}

func TestCustomPragma(t *testing.T) {
	p := New().Custom(Pragma("wal_autocheckpoint"), 1000)

	if p.pragmas[Pragma("wal_autocheckpoint")] != 1000 {
		t.Errorf("Custom pragma not set, got %v", p.pragmas[Pragma("wal_autocheckpoint")])
	}

	// Verify it flows through DSNPragmas
	params := p.DSNPragmas()
	if len(params) != 1 {
		t.Fatalf("DSNPragmas() returned %d params, want 1", len(params))
	}
	if !strings.Contains(params[0], "wal_autocheckpoint") {
		t.Errorf("DSNPragmas missing custom pragma, got %q", params[0])
	}

	// Verify it flows through SchemaStatements
	stmts := p.SchemaStatements("analytics")
	if len(stmts) != 1 {
		t.Fatalf("SchemaStatements() returned %d stmts, want 1", len(stmts))
	}
	if !strings.Contains(stmts[0], "analytics.wal_autocheckpoint") {
		t.Errorf("SchemaStatements missing custom pragma, got %q", stmts[0])
	}
}

func TestCustomPragmaInvalidNameSkipped(t *testing.T) {
	// An invalid pragma name set via Custom should be skipped by
	// DSNPragmas and SchemaStatements, and rejected by Apply
	p := New().Custom(Pragma("invalid; name"), "value")

	if len(p.DSNPragmas()) != 0 {
		t.Error("DSNPragmas should skip invalid pragma name")
	}
	if len(p.SchemaStatements("s")) != 0 {
		t.Error("SchemaStatements should skip invalid pragma name")
	}

	db, err := sql.Open("sqlite", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := p.Apply(db); err == nil {
		t.Error("Apply should reject invalid pragma name")
	}
}
