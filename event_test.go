// event_test.go tests EventBus mechanics: subscribe/unsubscribe, filtering,
// timestamp handling, concurrent emit, panic recovery, close semantics, and
// re-entrant subscribe/unsubscribe from within handlers.
//
// For per-subsystem event emission (does each operation emit the right events),
// see event_integration_test.go. For observer usage patterns, see
// observer_test.go and metrics_test.go.
package qwr

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventBusSubscribeEmit(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var received []Event
	eb.Subscribe(func(e Event) {
		received = append(received, e)
	})

	eb.Emit(Event{Type: EventManagerOpened})
	eb.Emit(Event{Type: EventManagerClosed})

	if len(received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(received))
	}
	if received[0].Type != EventManagerOpened {
		t.Errorf("expected EventManagerOpened, got %v", received[0].Type)
	}
	if received[1].Type != EventManagerClosed {
		t.Errorf("expected EventManagerClosed, got %v", received[1].Type)
	}
}

func TestEventBusFiltered(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var received []Event
	eb.SubscribeFiltered(func(e Event) {
		received = append(received, e)
	}, func(t EventType) bool {
		return t == EventJobCompleted
	})

	eb.Emit(Event{Type: EventJobQueued})
	eb.Emit(Event{Type: EventJobStarted})
	eb.Emit(Event{Type: EventJobCompleted})
	eb.Emit(Event{Type: EventJobFailed})

	if len(received) != 1 {
		t.Fatalf("expected 1 filtered event, got %d", len(received))
	}
	if received[0].Type != EventJobCompleted {
		t.Errorf("expected EventJobCompleted, got %v", received[0].Type)
	}
}

func TestEventBusUnsubscribe(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var count int
	id := eb.Subscribe(func(e Event) {
		count++
	})

	eb.Emit(Event{Type: EventJobQueued})
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}

	eb.Unsubscribe(id)
	eb.Emit(Event{Type: EventJobQueued})
	if count != 1 {
		t.Errorf("expected count still 1 after unsubscribe, got %d", count)
	}
}

func TestEventBusTimestamp(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var received Event
	eb.Subscribe(func(e Event) {
		received = e
	})

	before := time.Now()
	eb.Emit(Event{Type: EventManagerOpened})
	after := time.Now()

	if received.Timestamp.Before(before) || received.Timestamp.After(after) {
		t.Errorf("timestamp %v not between %v and %v", received.Timestamp, before, after)
	}
}

func TestEventBusPreserveExplicitTimestamp(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var received Event
	eb.Subscribe(func(e Event) {
		received = e
	})

	explicit := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	eb.Emit(Event{Type: EventManagerOpened, Timestamp: explicit})

	if !received.Timestamp.Equal(explicit) {
		t.Errorf("expected explicit timestamp preserved, got %v", received.Timestamp)
	}
}

func TestEventBusConcurrentEmit(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var count atomic.Int64
	eb.Subscribe(func(e Event) {
		count.Add(1)
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				eb.Emit(Event{Type: EventJobQueued})
			}
		}()
	}
	wg.Wait()

	if count.Load() != 10000 {
		t.Errorf("expected 10000 events, got %d", count.Load())
	}
}

func TestEventBusZeroSubscribers(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	// Should not panic with no subscribers
	eb.Emit(Event{Type: EventJobQueued})
	eb.Emit(Event{Type: EventJobCompleted})
}

func TestEventBusClose(t *testing.T) {
	eb := NewEventBus()

	var count int
	eb.Subscribe(func(e Event) {
		count++
	})

	eb.Emit(Event{Type: EventJobQueued})
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}

	eb.Close()

	// After close, no events should be delivered
	eb.Emit(Event{Type: EventJobQueued})
	if count != 1 {
		t.Errorf("expected count still 1 after close, got %d", count)
	}
}

func TestEventBusMultipleSubscribers(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var count1, count2 int
	eb.Subscribe(func(e Event) { count1++ })
	eb.Subscribe(func(e Event) { count2++ })

	eb.Emit(Event{Type: EventJobQueued})

	if count1 != 1 || count2 != 1 {
		t.Errorf("expected both subscribers called, got %d and %d", count1, count2)
	}
}

func TestEventBusPanicRecovery(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	// First handler panics
	eb.Subscribe(func(e Event) {
		panic("handler exploded")
	})

	// Second handler should still be called
	var called bool
	eb.Subscribe(func(e Event) {
		called = true
	})

	// Emit should not panic — the panicking handler is recovered
	eb.Emit(Event{Type: EventJobQueued})

	if !called {
		t.Error("second handler was not called after first handler panicked")
	}
}

func TestEventBusSubscribeFromHandler(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var dynamicCount int
	eb.Subscribe(func(e Event) {
		if e.Type == EventManagerOpened {
			// Subscribe from within a handler — should not deadlock or corrupt
			eb.Subscribe(func(e Event) {
				dynamicCount++
			})
		}
	})

	// This emit triggers the inner Subscribe
	eb.Emit(Event{Type: EventManagerOpened})

	// The dynamically added subscriber should receive this event
	eb.Emit(Event{Type: EventJobQueued})

	if dynamicCount != 1 {
		t.Errorf("expected dynamic subscriber to receive 1 event, got %d", dynamicCount)
	}
}

func TestEventBusUnsubscribeFromHandler(t *testing.T) {
	eb := NewEventBus()
	defer eb.Close()

	var count int
	var subID uint64
	subID = eb.Subscribe(func(e Event) {
		count++
		// Unsubscribe self after first event — should not deadlock or corrupt
		eb.Unsubscribe(subID)
	})

	// Another subscriber to verify iteration isn't corrupted
	var otherCount int
	eb.Subscribe(func(e Event) {
		otherCount++
	})

	eb.Emit(Event{Type: EventJobQueued}) // first handler runs, unsubscribes self
	eb.Emit(Event{Type: EventJobQueued}) // first handler should NOT run

	if count != 1 {
		t.Errorf("expected self-unsubscribing handler to run once, got %d", count)
	}
	if otherCount != 2 {
		t.Errorf("expected other handler to run twice, got %d", otherCount)
	}
}

func TestEventBusConcurrentEmitAndClose(t *testing.T) {
	eb := NewEventBus()

	var count atomic.Int64
	eb.Subscribe(func(e Event) {
		count.Add(1)
	})

	var wg sync.WaitGroup
	// Hammer Emit from multiple goroutines
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				eb.Emit(Event{Type: EventJobQueued})
			}
		}()
	}

	// Close concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		eb.Close()
	}()

	wg.Wait()

	// After Close, Emit must be a no-op
	before := count.Load()
	eb.Emit(Event{Type: EventJobQueued})
	if count.Load() != before {
		t.Error("Emit delivered an event after Close")
	}
}

func BenchmarkEventBusEmitZeroSubscribers(b *testing.B) {
	eb := NewEventBus()
	defer eb.Close()
	event := Event{Type: EventJobQueued}

	b.ResetTimer()
	for range b.N {
		eb.Emit(event)
	}
}

func BenchmarkEventBusEmitOneSubscriber(b *testing.B) {
	eb := NewEventBus()
	defer eb.Close()
	eb.Subscribe(func(e Event) {})
	event := Event{Type: EventJobQueued}

	b.ResetTimer()
	for range b.N {
		eb.Emit(event)
	}
}

func BenchmarkEventBusEmitFilteredSubscriber(b *testing.B) {
	eb := NewEventBus()
	defer eb.Close()
	eb.SubscribeFiltered(func(e Event) {}, JobEvents())
	event := Event{Type: EventCacheHit} // won't match filter

	b.ResetTimer()
	for range b.N {
		eb.Emit(event)
	}
}
