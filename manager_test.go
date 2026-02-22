package qwr

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/jpl-au/qwr/checkpoint"
)

// TestContextCancellation verifies context cancellation works.
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := DefaultOptions
	opts.UseContexts = true

	mgr, err := New(":memory:", opts).WithContext(ctx).Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	setupTable(t, mgr)

	// Cancel context before query
	cancel()

	// Query should fail due to cancelled context
	_, err = mgr.Query("SELECT * FROM users").Read()
	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if err != nil && err != context.Canceled {
		t.Logf("got error: %v", err)
	}
}

// TestEventsEmitted verifies events are emitted during manager lifecycle.
func TestEventsEmitted(t *testing.T) {
	var mu sync.Mutex
	var received []EventType

	handler := func(e Event) {
		mu.Lock()
		received = append(received, e.Type)
		mu.Unlock()
	}

	mgr, err := New("file::memory:?cache=shared", DefaultOptions).
		WithObserver(handler).
		Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Close to trigger closing/closed events
	if err := mgr.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Should have at least: WorkerStarted, ManagerOpened, ManagerClosing, WorkerStopped, ManagerClosed
	hasOpened := false
	hasClosing := false
	hasClosed := false
	for _, et := range received {
		switch et {
		case EventManagerOpened:
			hasOpened = true
		case EventManagerClosing:
			hasClosing = true
		case EventManagerClosed:
			hasClosed = true
		}
	}

	if !hasOpened {
		t.Error("expected EventManagerOpened to be emitted")
	}
	if !hasClosing {
		t.Error("expected EventManagerClosing to be emitted")
	}
	if !hasClosed {
		t.Error("expected EventManagerClosed to be emitted")
	}
}

// TestCheckpointOnClose verifies WAL checkpoint runs on close.
func TestCheckpointOnClose(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "qwr-checkpoint-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	walPath := dbPath + "-wal"

	// Create manager with checkpoint.Truncate
	mgr, err := New(dbPath).
		Checkpoint(checkpoint.Truncate).
		Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Create table and insert data to generate WAL activity
	_, err = mgr.Query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").Execute()
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for i := range 100 {
		_, err = mgr.Query("INSERT INTO users (name) VALUES (?)", "User").Execute()
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Check WAL file exists and has content before close
	walInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist before close: %v", err)
	}
	if walInfo.Size() == 0 {
		t.Fatal("WAL file should have content before close")
	}
	t.Attr("wal_size_bytes", strconv.FormatInt(walInfo.Size(), 10))

	// Close manager (should run checkpoint)
	if err := mgr.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Check WAL file is truncated to zero
	walInfo, err = os.Stat(walPath)
	if err != nil {
		// WAL file might be completely removed, which is also acceptable
		if os.IsNotExist(err) {
			t.Log("WAL file removed after truncate checkpoint")
			return
		}
		t.Fatalf("failed to stat WAL after close: %v", err)
	}

	if walInfo.Size() != 0 {
		t.Errorf("WAL file should be truncated to 0 bytes, got %d", walInfo.Size())
	} else {
		t.Log("WAL file truncated to 0 bytes")
	}
}

// TestCheckpointMode verifies checkpoint.Mode values.
func TestCheckpointMode(t *testing.T) {
	tests := []struct {
		mode checkpoint.Mode
		want string
	}{
		{checkpoint.None, ""},
		{checkpoint.Passive, "PASSIVE"},
		{checkpoint.Full, "FULL"},
		{checkpoint.Restart, "RESTART"},
		{checkpoint.Truncate, "TRUNCATE"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("checkpoint.Mode %q != %q", tt.mode, tt.want)
		}
	}
}
