package qwr

import (
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestWatchdogDetection verifies that slow handlers trigger a warning log.
func TestWatchdogDetection(t *testing.T) {
	// Custom slog handler to capture output
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	slog.SetDefault(logger)

	eb := NewEventBus()
	defer eb.Close()

	// Register a slow handler (150ms)
	eb.Subscribe(func(e Event) {
		time.Sleep(150 * time.Millisecond)
	})

	eb.Emit(Event{Type: EventJobCompleted})

	// Check if warning was logged
	output := buf.String()
	if !strings.Contains(output, "slow event handler detected") {
		t.Errorf("expected watchdog warning log, got: %q", output)
	}
	if !strings.Contains(output, "EventJobCompleted") {
		t.Errorf("expected event type in log, got: %q", output)
	}
}

// TestReaderEvents verifies that Read and ReadRow emit events.
func TestReaderEvents(t *testing.T) {
	mgr := newTestMgr(t, Options{EnableReader: true, EnableWriter: true})
	defer mgr.Close()
	setupTable(t, mgr)

	var readCount atomic.Int64
	mgr.SubscribeFiltered(func(e Event) {
		readCount.Add(1)
	}, ReadEvents())

	// Trigger Read()
	rows, err := mgr.Query("SELECT * FROM users").Read()
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	// Trigger ReadRow()
	_, _ = mgr.Query("SELECT COUNT(*) FROM users").ReadRow()

	if readCount.Load() != 2 {
		t.Errorf("expected 2 read events, got %d", readCount.Load())
	}
}

// TestReaderFailureEvent verifies that reader errors emit failure events.
func TestReaderFailureEvent(t *testing.T) {
	mgr := newTestMgr(t, Options{EnableReader: true})
	defer mgr.Close()

	var failCount atomic.Int64
	mgr.SubscribeFiltered(func(e Event) {
		if e.Type == EventReaderQueryFailed {
			failCount.Add(1)
		}
	}, ErrorEvents())

	// Trigger a failed read (table doesn't exist)
	_, _ = mgr.Query("SELECT * FROM nonexistent").Read()

	if failCount.Load() != 1 {
		t.Errorf("expected 1 reader failure event, got %d", failCount.Load())
	}
}
