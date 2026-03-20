// metrics_test.go demonstrates how to build your own metrics on top of qwr's
// event system. qwr does not ship a metrics library or impose one on you.
// Instead, every internal operation emits an Event that you can observe.
//
// Each test below is a self-contained pattern you can copy into your own code.
// They are also real tests: they execute queries and verify that the metrics
// are correct afterward.
//
// Patterns covered:
//   - WriteMetrics:     count writes, track latency, record failures
//   - ErrorAlerting:    capture error context for alerting or dashboards
//   - StructuredLogger: wire events into log/slog (or any structured logger)
package qwr

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Pattern 1: Write metrics (counters + latency)
// ---------------------------------------------------------------------------

// writeMetrics tracks write throughput and latency. Safe for concurrent use
// because handlers are called synchronously on the write worker goroutine,
// but the metrics may be read from other goroutines (e.g., a /metrics endpoint).
type writeMetrics struct {
	completed atomic.Int64
	failed    atomic.Int64
	totalExec atomic.Int64 // nanoseconds, use time.Duration(m.totalExec.Load())
	maxExec   atomic.Int64 // nanoseconds
}

func (m *writeMetrics) handler(e Event) {
	switch e.Type {
	case EventJobCompleted:
		m.completed.Add(1)
		ns := int64(e.ExecTime)
		m.totalExec.Add(ns)
		// Update max using compare-and-swap loop
		for {
			cur := m.maxExec.Load()
			if ns <= cur || m.maxExec.CompareAndSwap(cur, ns) {
				break
			}
		}
	case EventJobFailed:
		m.failed.Add(1)
	}
}

func TestWriteMetrics(t *testing.T) {
	mgr := newTestMgr(t, Options{})
	defer mgr.Close()
	setupTable(t, mgr)

	var metrics writeMetrics
	mgr.SubscribeFiltered(metrics.handler, JobEvents())

	// Run several successful writes
	for i := range 5 {
		_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("user_%d", i)).Execute()
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	mgr.WaitForIdle(context.Background())

	if got := metrics.completed.Load(); got != 5 {
		t.Errorf("completed: got %d, want 5", got)
	}
	if got := metrics.failed.Load(); got != 0 {
		t.Errorf("failed: got %d, want 0", got)
	}
	if got := time.Duration(metrics.totalExec.Load()); got <= 0 {
		t.Error("totalExec should be positive after 5 writes")
	}
	if got := time.Duration(metrics.maxExec.Load()); got <= 0 {
		t.Error("maxExec should be positive after 5 writes")
	}

	// Run a write that will fail (table does not exist)
	_, _ = mgr.Query("INSERT INTO nonexistent (x) VALUES (1)").Execute()
	mgr.WaitForIdle(context.Background())

	if got := metrics.failed.Load(); got != 1 {
		t.Errorf("failed: got %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Pattern 2: Error alerting
// ---------------------------------------------------------------------------

// errorRecord captures enough context from a failure to feed an alerting
// system, a dashboard, or an error log. Only error-related events are
// delivered because the handler is registered with ErrorEvents().
type errorRecord struct {
	jobID    int64
	sql      string
	err      error
	execTime time.Duration
}

func TestErrorAlerting(t *testing.T) {
	mgr := newTestMgr(t, Options{})
	defer mgr.Close()
	setupTable(t, mgr)

	var mu sync.Mutex
	var errors []errorRecord

	// ErrorEvents() matches failures across categories: EventJobFailed,
	// EventDirectWriteFailed, EventErrorStored, EventErrorPersisted,
	// EventErrorQueueOverflow, and EventRetryExhausted.
	mgr.SubscribeFiltered(func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		errors = append(errors, errorRecord{
			jobID:    e.JobID,
			sql:      e.SQL,
			err:      e.Err,
			execTime: e.ExecTime,
		})
	}, ErrorEvents())

	// A successful write should not trigger the error handler
	_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "good").Execute()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	mgr.WaitForIdle(context.Background())

	mu.Lock()
	if len(errors) != 0 {
		t.Errorf("got %d error records after successful write, want 0", len(errors))
	}
	mu.Unlock()

	// A failing write should be captured with full context
	badSQL := "INSERT INTO nonexistent_table (x) VALUES (1)"
	_, _ = mgr.Query(badSQL).Execute()
	mgr.WaitForIdle(context.Background())

	mu.Lock()
	defer mu.Unlock()

	if len(errors) == 0 {
		t.Fatal("no error records captured after failed write")
	}

	rec := errors[0]
	if rec.sql != badSQL {
		t.Errorf("sql: got %q, want %q", rec.sql, badSQL)
	}
	if rec.err == nil {
		t.Error("err should not be nil for a failed write")
	}
	if rec.jobID == 0 {
		t.Error("jobID should be set so the error can be correlated")
	}
}

// ---------------------------------------------------------------------------
// Pattern 3: Structured logging via slog
// ---------------------------------------------------------------------------

// TestStructuredLogger shows how to wire qwr events into log/slog.
// This pattern replaces the need for qwr to depend on any logging library.
// You control the log level, format, and destination.
func TestStructuredLogger(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		// Use LevelDebug so all events are logged in this test.
		// In production you might use LevelInfo and only log completions/failures.
		Level: slog.LevelDebug,
	}))

	handler := func(e Event) {
		switch e.Type {
		case EventJobCompleted:
			logger.Debug("query completed",
				"job_id", e.JobID,
				"sql", e.SQL,
				"exec_time", e.ExecTime,
				"queue_wait", e.QueueWait,
			)
		case EventJobFailed:
			logger.Error("query failed",
				"job_id", e.JobID,
				"sql", e.SQL,
				"error", e.Err,
				"exec_time", e.ExecTime,
			)
		}
	}

	mgr := newTestMgr(t, Options{})
	defer mgr.Close()
	setupTable(t, mgr)

	mgr.SubscribeFiltered(handler, JobEvents())

	_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "slog_test").Execute()
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	mgr.WaitForIdle(context.Background())

	output := buf.String()
	if !strings.Contains(output, "query completed") {
		t.Errorf("expected 'query completed' in log output, got:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO users") {
		t.Errorf("expected SQL in log output, got:\n%s", output)
	}
	if !strings.Contains(output, "exec_time") {
		t.Errorf("expected exec_time in log output, got:\n%s", output)
	}
}

// ---------------------------------------------------------------------------
// Pattern 4: Queue depth monitoring
// ---------------------------------------------------------------------------

// TestQueueDepthMonitoring shows how to track queue depth over time using
// EventJobQueued and EventJobCompleted. This is useful for detecting
// backpressure - if queued grows faster than completed, the worker can't
// keep up with the write rate.
func TestQueueDepthMonitoring(t *testing.T) {
	mgr := newTestMgr(t, Options{})
	defer mgr.Close()
	setupTable(t, mgr)

	var queued, dequeued atomic.Int64

	mgr.SubscribeFiltered(func(e Event) {
		switch e.Type {
		case EventJobQueued:
			queued.Add(1)
		case EventJobCompleted, EventJobFailed:
			dequeued.Add(1)
		}
	}, JobEvents())

	for i := range 10 {
		_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", fmt.Sprintf("user_%d", i)).Execute()
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	mgr.WaitForIdle(context.Background())

	q := queued.Load()
	d := dequeued.Load()

	if q != 10 {
		t.Errorf("queued: got %d, want 10", q)
	}
	// Every queued job should eventually complete or fail
	if q != d {
		t.Errorf("queued (%d) != dequeued (%d): jobs are stuck in the queue", q, d)
	}
}
