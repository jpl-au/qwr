// observer_test.go demonstrates and verifies the observer pattern from a
// user's perspective: subscribing, filtering, extracting event data, and
// capturing lifecycle transitions. Each subtest of TestObserverPattern is a
// self-contained usage example.
//
// For EventBus mechanics (concurrency, panic recovery, close semantics),
// see event_test.go. For per-subsystem event emission, see
// event_integration_test.go. For metrics/alerting patterns, see metrics_test.go.
package qwr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestObserverPattern demonstrates and verifies the use of the Observer pattern in qwr.
// This test file serves as documentation for implementing event-driven logic.
func TestObserverPattern(t *testing.T) {
	// 1. Basic Subscription
	// Subscribing to all events is useful for global logging or metrics.
	t.Run("BasicSubscription", func(t *testing.T) {
		mgr := newTestMgr(t, Options{})
		defer mgr.Close()

		var eventCount atomic.Int64
		handler := func(e Event) {
			eventCount.Add(1)
		}

		// Register the handler
		id := mgr.Subscribe(handler)
		if id == 0 {
			t.Fatal("expected non-zero subscription ID")
		}

		// Trigger some events
		_, _ = mgr.Query("CREATE TABLE test (id INT)").Execute()
		mgr.WaitForIdle(context.Background())

		if eventCount.Load() == 0 {
			t.Error("expected to receive events, got none")
		}

		// Unsubscribe and verify no more events are received
		mgr.Unsubscribe(id)
		lastCount := eventCount.Load()
		_, _ = mgr.Query("INSERT INTO test (id) VALUES (1)").Execute()
		mgr.WaitForIdle(context.Background())

		if eventCount.Load() != lastCount {
			t.Errorf("received events after Unsubscribe: got %d, expected %d", eventCount.Load(), lastCount)
		}
	})

	// 2. Filtered Subscription
	// Filters allow you to subscribe only to specific types of events, reducing overhead.
	t.Run("FilteredSubscription", func(t *testing.T) {
		mgr := newTestMgr(t, Options{})
		defer mgr.Close()
		setupTable(t, mgr)

		var jobEvents []Event
		var mu sync.Mutex

		// Subscribe only to Job lifecycle events (Queued, Started, Completed, Failed)
		mgr.SubscribeFiltered(func(e Event) {
			mu.Lock()
			jobEvents = append(jobEvents, e)
			mu.Unlock()
		}, JobEvents())

		// Execute a query which should trigger Job events
		_, err := mgr.Query("INSERT INTO users (name) VALUES (?)", "observer_test").Execute()
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		mgr.WaitForIdle(context.Background())

		mu.Lock()
		defer mu.Unlock()

		if len(jobEvents) == 0 {
			t.Fatal("no job events received")
		}

		// Verify we only got Job-related events
		for _, e := range jobEvents {
			switch e.Type {
			case EventJobQueued, EventJobStarted, EventJobCompleted, EventJobFailed:
				// Expected
			default:
				t.Errorf("received unexpected event type: %v", e.Type)
			}
		}
	})

	// 3. Extracting Data from Events
	// The Event struct is a "flat" struct. This example shows how to correctly 
	// inspect fields based on the EventType.
	t.Run("EventDataExtraction", func(t *testing.T) {
		mgr := newTestMgr(t, Options{})
		defer mgr.Close()
		setupTable(t, mgr)

		var capturedEvent Event
		done := make(chan struct{})

		mgr.SubscribeFiltered(func(e Event) {
			if e.Type == EventJobCompleted {
				capturedEvent = e
				close(done)
			}
		}, JobEvents())

		sql := "INSERT INTO users (name) VALUES (?)"
		_, _ = mgr.Query(sql, "data_extraction").Execute()

		select {
		case <-done:
			// Success
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for EventJobCompleted")
		}

		// Verify specific fields populated for Job events
		if capturedEvent.SQL != sql {
			t.Errorf("expected SQL %q, got %q", sql, capturedEvent.SQL)
		}
		if capturedEvent.ExecTime <= 0 {
			t.Error("expected positive ExecTime for completed job")
		}
		if capturedEvent.JobID == 0 {
			t.Error("expected non-zero JobID")
		}
	})

	// 4. Batching Events
	// This demonstrates how the observer pattern captures internal batching behaviour.
	t.Run("BatchingEvents", func(t *testing.T) {
		opts := Options{
			BatchSize:      2,
			BatchTimeout:   100 * time.Millisecond,
			InlineInserts:  true,
		}
		mgr := newTestMgr(t, opts)
		defer mgr.Close()
		setupTable(t, mgr)

		var batchFlushed, batchOptimised bool
		var mu sync.Mutex
		
		mgr.SubscribeFiltered(func(e Event) {
			mu.Lock()
			if e.Type == EventBatchFlushed {
				batchFlushed = true
				if e.BatchSize != 2 {
					t.Errorf("expected batch size 2, got %d", e.BatchSize)
				}
				if e.BatchReason != "size_limit" {
					t.Errorf("expected reason size_limit, got %s", e.BatchReason)
				}
			}
			if e.Type == EventBatchInlineOptimised {
				batchOptimised = true
			}
			mu.Unlock()
		}, BatchEvents())

		// Add two items with identical SQL to trigger inline optimisation
		sql := "INSERT INTO users (name) VALUES (?)"
		mgr.Query(sql, "batch1").Batch()
		mgr.Query(sql, "batch2").Batch()
		mgr.WaitForIdle(context.Background())

		mu.Lock()
		if !batchFlushed {
			t.Error("EventBatchFlushed not received")
		}
		if !batchOptimised {
			t.Error("EventBatchInlineOptimised not received")
		}
		mu.Unlock()
	})

	// 5. Manager Lifecycle
	// Shows how to capture the full lifecycle of the manager.
	t.Run("LifecycleEvents", func(t *testing.T) {
		var opened, closing, closed bool
		var mu sync.Mutex

		handler := func(e Event) {
			mu.Lock()
			defer mu.Unlock()
			switch e.Type {
			case EventManagerOpened:
				opened = true
			case EventManagerClosing:
				closing = true
			case EventManagerClosed:
				closed = true
			}
		}

		// To capture EventManagerOpened, we must use WithObserver during construction
		mgr, err := New("file::memory:?cache=shared", Options{}).
			WithObserver(handler).
			Open()
		if err != nil {
			t.Fatalf("failed to open manager: %v", err)
		}

		// Close the manager to trigger closing/closed events
		mgr.Close()

		mu.Lock()
		if !opened {
			t.Error("EventManagerOpened not received")
		}
		if !closing {
			t.Error("EventManagerClosing not received")
		}
		if !closed {
			t.Error("EventManagerClosed not received")
		}
		mu.Unlock()
	})

	// 6. Multiple Subscribers
	// Verify that multiple independent subscribers all receive the events.
	t.Run("MultipleSubscribers", func(t *testing.T) {
		mgr := newTestMgr(t, Options{})
		defer mgr.Close()

		var count1, count2 atomic.Int64
		mgr.Subscribe(func(e Event) { count1.Add(1) })
		mgr.Subscribe(func(e Event) { count2.Add(1) })

		_, _ = mgr.Query("SELECT 1").Execute()
		mgr.WaitForIdle(context.Background())

		if count1.Load() == 0 || count1.Load() != count2.Load() {
			t.Errorf("subscriber mismatch: count1=%d, count2=%d", count1.Load(), count2.Load())
		}
	})
}

