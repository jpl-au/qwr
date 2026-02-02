package qwr

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jpl-au/qwr/backup"
)

// TestBackup verifies database backup functionality.
func TestBackup(t *testing.T) {
	tests := []struct {
		name   string
		method backup.Method
	}{
		{"Default", backup.Default},
		{"API", backup.API},
		{"Vacuum", backup.Vacuum},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "qwr-backup-test-*")
			if err != nil {
				t.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			src := filepath.Join(tmpDir, "source.db")
			dest := filepath.Join(tmpDir, "backup.db")

			// Create source database with data
			mgr, err := New(src).Open()
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			// Create table and insert data
			_, err = mgr.Query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").Execute()
			if err != nil {
				t.Fatalf("failed to create table: %v", err)
			}

			for i := range 100 {
				_, err = mgr.Query("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("User%d", i)).Execute()
				if err != nil {
					t.Fatalf("insert %d failed: %v", i, err)
				}
			}

			// Close source before backup to ensure all data is flushed
			mgr.Close()

			// Reopen for backup
			mgr, err = New(src).Open()
			if err != nil {
				t.Fatalf("failed to reopen manager: %v", err)
			}

			// Create backup
			err = mgr.Backup(dest, tt.method)
			if err != nil {
				t.Fatalf("backup failed: %v", err)
			}

			// Close source
			mgr.Close()

			// Verify backup file exists
			info, err := os.Stat(dest)
			if err != nil {
				t.Fatalf("backup file not found: %v", err)
			}
			if info.Size() == 0 {
				t.Fatal("backup file is empty")
			}
			t.Attr("backup_size_bytes", strconv.FormatInt(info.Size(), 10))

			// Validate using sqlite3 CLI before opening with qwr
			validateBackupWithCLI(t, dest, 100)

			// Open backup and verify data with qwr
			backupMgr, err := New(dest).Open()
			if err != nil {
				t.Fatalf("failed to open backup: %v", err)
			}
			defer backupMgr.Close()

			row, err := backupMgr.Query("SELECT COUNT(*) FROM users").ReadRow()
			if err != nil {
				t.Fatalf("count query failed: %v", err)
			}

			var count int
			if err := row.Scan(&count); err != nil {
				t.Fatalf("scan failed: %v", err)
			}

			if count != 100 {
				t.Errorf("backup has %d rows, want 100", count)
			}
		})
	}
}

// validateBackupWithCLI uses sqlite3 CLI to validate backup contents.
func validateBackupWithCLI(t *testing.T, dbPath string, expectedRows int) {
	t.Helper()

	// Check if sqlite3 is available
	_, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Log("sqlite3 CLI not found, skipping CLI validation")
		return
	}

	// Get row count using sqlite3 CLI
	cmd := exec.Command("sqlite3", dbPath, "SELECT COUNT(*) FROM users;")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("sqlite3 CLI failed: %v", err)
	}

	countStr := strings.TrimSpace(string(output))
	count, err := strconv.Atoi(countStr)
	if err != nil {
		t.Fatalf("failed to parse count from sqlite3: %v (output: %q)", err, countStr)
	}

	if count != expectedRows {
		t.Errorf("sqlite3 CLI reports %d rows, want %d", count, expectedRows)
	}
	t.Attr("validated_rows", strconv.Itoa(count))

	// Also get first and last row to verify data integrity
	cmd = exec.Command("sqlite3", "-json", dbPath, "SELECT * FROM users ORDER BY id LIMIT 2;")
	output, err = cmd.Output()
	if err != nil {
		t.Logf("sqlite3 JSON output failed: %v", err)
		return
	}
	t.Logf("First 2 rows (JSON): %s", strings.TrimSpace(string(output)))

	cmd = exec.Command("sqlite3", "-json", dbPath, "SELECT * FROM users ORDER BY id DESC LIMIT 2;")
	output, err = cmd.Output()
	if err != nil {
		t.Logf("sqlite3 JSON output failed: %v", err)
		return
	}
	t.Logf("Last 2 rows (JSON): %s", strings.TrimSpace(string(output)))
}

// TestBackupDestinationExists verifies backup fails if destination exists.
func TestBackupDestinationExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwr-backup-exists-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	src := filepath.Join(tmpDir, "source.db")
	dest := filepath.Join(tmpDir, "backup.db")

	// Create source database
	mgr, err := New(src).Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Create existing non-empty backup file
	if err := os.WriteFile(dest, []byte("existing content"), 0644); err != nil {
		t.Fatalf("failed to create existing file: %v", err)
	}

	// Backup should fail
	err = mgr.Backup(dest, backup.Default)
	if err == nil {
		t.Error("expected error when backup destination exists, got nil")
	}
}
