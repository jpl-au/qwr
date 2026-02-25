// event_integration_test.go verifies that each qwr subsystem (worker queue,
// direct writes, batching, caching, backups, vacuums, lifecycle) emits the
// correct events. Each test creates a real Manager, performs an operation, and
// checks that the expected EventTypes appeared.
//
// For EventBus mechanics (subscribe, unsubscribe, filtering, concurrency),
// see event_test.go. For observer usage patterns (metrics, logging, alerting),
// see observer_test.go and metrics_test.go.
package qwr

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jpl-au/qwr/backup"
)

// collectEvents is a test helper that subscribes to all events and returns
// a function to retrieve collected events.
func collectEvents(mgr *Manager) func() []Event {
	var mu sync.Mutex
	var events []Event
	mgr.Subscribe(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	return func() []Event {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(events)
	}
}

// hasEventType checks if the event list contains the given type.
func hasEventType(events []Event, t EventType) bool {
	return slices.ContainsFunc(events, func(e Event) bool {
		return e.Type == t
	})
}

func TestObserverExecuteEvents(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	setupTable(t, mgr)

	_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "Alice").Execute()
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventJobQueued) {
		t.Error("missing EventJobQueued")
	}
	if !hasEventType(events, EventJobStarted) {
		t.Error("missing EventJobStarted")
	}
	if !hasEventType(events, EventJobCompleted) {
		t.Error("missing EventJobCompleted")
	}
}

func TestObserverDirectWriteEvents(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	setupTable(t, mgr)

	_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "Bob").Write()
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventDirectWriteCompleted) {
		t.Error("missing EventDirectWriteCompleted")
	}
}

func TestObserverDirectWriteFailedEvents(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	// Write to a non-existent table should fail
	_, _ = mgr.Query("INSERT INTO nonexistent (name) VALUES (?)", "Bob").Write()

	events := getEvents()
	if !hasEventType(events, EventDirectWriteFailed) {
		t.Error("missing EventDirectWriteFailed")
	}
}

func TestObserverBatchEvents(t *testing.T) {
	opts := DefaultOptions
	opts.BatchSize = 2
	opts.BatchTimeout = 100 * time.Millisecond

	mgr := newTestMgr(t, opts)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	setupTable(t, mgr)

	// Add enough queries to trigger a batch flush
	for i := range 3 {
		_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", i).Batch()
		if err != nil {
			t.Fatalf("batch add failed: %v", err)
		}
	}

	// Wait for batch to be processed
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mgr.WaitForIdle(ctx); err != nil {
		t.Fatalf("wait for idle failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventBatchQueryAdded) {
		t.Error("missing EventBatchQueryAdded")
	}
	if !hasEventType(events, EventBatchFlushed) {
		t.Error("missing EventBatchFlushed")
	}
}

func TestObserverCacheEvents(t *testing.T) {
	opts := DefaultOptions
	opts.UsePreparedStatements = true

	mgr := newTestMgr(t, opts)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	setupTable(t, mgr)

	// First call: cache miss (prepare)
	_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "Alice").Execute()
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Second call: cache hit
	_, err = mgr.Query("INSERT INTO users (name) VALUES (?)", "Bob").Execute()
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventCacheMiss) {
		t.Error("missing EventCacheMiss")
	}
	if !hasEventType(events, EventCacheHit) {
		t.Error("missing EventCacheHit")
	}
}

func TestObserverBackupEvents(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwr-observer-backup-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	mgr, err := New(dbPath).Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	setupTable(t, mgr)

	destPath := filepath.Join(tmpDir, "backup.db")
	if err := mgr.Backup(destPath, backup.Default); err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventBackupStarted) {
		t.Error("missing EventBackupStarted")
	}
	if !hasEventType(events, EventBackupCompleted) {
		t.Error("missing EventBackupCompleted")
	}
}

func TestObserverManagerLifecycle(t *testing.T) {
	var mu sync.Mutex
	var events []Event

	mgr, err := New("file::memory:?cache=shared", DefaultOptions).
		WithObserver(func(e Event) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		}).
		Open()
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	var types []EventType
	for _, e := range events {
		types = append(types, e.Type)
	}

	// Verify order: WorkerStarted before ManagerOpened, ManagerClosing before ManagerClosed
	openedIdx := -1
	closingIdx := -1
	closedIdx := -1
	for i, et := range types {
		switch et {
		case EventManagerOpened:
			openedIdx = i
		case EventManagerClosing:
			closingIdx = i
		case EventManagerClosed:
			closedIdx = i
		}
	}

	if openedIdx == -1 {
		t.Fatal("missing EventManagerOpened")
	}
	if closingIdx == -1 {
		t.Fatal("missing EventManagerClosing")
	}
	if closedIdx == -1 {
		t.Fatal("missing EventManagerClosed")
	}
	if closingIdx >= closedIdx {
		t.Errorf("EventManagerClosing (idx=%d) should come before EventManagerClosed (idx=%d)", closingIdx, closedIdx)
	}
	if openedIdx >= closingIdx {
		t.Errorf("EventManagerOpened (idx=%d) should come before EventManagerClosing (idx=%d)", openedIdx, closingIdx)
	}
}

func TestObserverVacuumEvents(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	getEvents := collectEvents(mgr)
	defer mgr.Close()

	if err := mgr.RunVacuum(); err != nil {
		t.Fatalf("vacuum failed: %v", err)
	}

	events := getEvents()
	if !hasEventType(events, EventVacuumStarted) {
		t.Error("missing EventVacuumStarted")
	}
	if !hasEventType(events, EventVacuumCompleted) {
		t.Error("missing EventVacuumCompleted")
	}
}
