package qwr

import (
	"log/slog"
	"slices"
	"sync"
	"time"
)

// EventHandler is a callback that receives events from the EventBus.
//
// Handlers are called synchronously on the goroutine that called Emit. In
// practice this means handlers run on the write worker goroutine for job
// events, or on the caller's goroutine for direct operations like RunVacuum.
//
// When multiple handlers are registered, they are called sequentially in
// subscription order for each event. A slow handler delays all handlers
// after it for that event AND blocks the caller (e.g., the write worker).
//
// Handlers MUST NOT block. A slow handler stalls the write worker, which
// prevents all other queued jobs from executing. If you need to do expensive
// work (network I/O, disk writes, etc.), send the event to a channel and
// process it in a separate goroutine.
//
// Panics in handlers are recovered by the EventBus and do not propagate.
type EventHandler func(Event)

// EventFilter returns true if the handler should receive this event type.
// Use the predefined filters (JobEvents, ErrorEvents, etc.) or provide your own.
type EventFilter func(EventType) bool

// subscription pairs a handler with an optional filter.
type subscription struct {
	id      uint64
	handler EventHandler
	filter  EventFilter
}

// EventBus provides synchronous, in-process event dispatch.
//
// Delivery guarantee: each event is delivered exactly once to every subscriber
// whose filter matches at the time of the Emit call. Events are never queued,
// retried, or persisted — if no subscribers are registered, the event is
// silently discarded. After Close, all Emit calls are no-ops.
//
// Concurrency model:
//   - Multiple goroutines may call Emit concurrently.
//   - Each handler is called on the emitting goroutine (not a background worker).
//   - Subscribe, Unsubscribe, and Emit may all be called concurrently, including
//     from within a handler (see Emit for details).
//   - After Close, Emit is a no-op. Close is safe to call concurrently with Emit.
type EventBus struct {
	mu          sync.RWMutex
	subscribers []subscription
	nextID      uint64
	// closed is checked inside the RLock in Emit. Nilling subscribers alone is
	// not sufficient because a concurrent SubscribeFiltered could re-populate
	// the list between Close and the next Emit.
	closed bool
}

// NewEventBus creates a ready-to-use EventBus.
func NewEventBus() *EventBus {
	return &EventBus{}
}

// Subscribe registers a handler that receives all events.
// Returns a subscription ID that can be passed to Unsubscribe.
func (eb *EventBus) Subscribe(handler EventHandler) uint64 {
	return eb.SubscribeFiltered(handler, nil)
}

// SubscribeFiltered registers a handler that only receives events accepted by filter.
// If filter is nil the handler receives all events.
func (eb *EventBus) SubscribeFiltered(handler EventHandler, filter EventFilter) uint64 {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.nextID++
	id := eb.nextID
	eb.subscribers = append(eb.subscribers, subscription{
		id:      id,
		handler: handler,
		filter:  filter,
	})
	return id
}

// Unsubscribe removes the subscription with the given ID.
func (eb *EventBus) Unsubscribe(id uint64) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.subscribers = slices.DeleteFunc(eb.subscribers, func(s subscription) bool {
		return s.id == id
	})
}

// Emit dispatches an event to all matching subscribers synchronously.
// Safe to call concurrently. The Timestamp field is set automatically if zero.
//
// After Close, Emit is a no-op. Panics in handlers are recovered and do not
// propagate to the caller.
//
// Handlers are called outside the lock, so it is safe to call Subscribe or
// Unsubscribe from within a handler without deadlocking. Changes to the
// subscriber list take effect on the next Emit call, not the current one.
func (eb *EventBus) Emit(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	eb.mu.RLock()
	if eb.closed {
		eb.mu.RUnlock()
		return
	}
	// Clone creates an independent slice with its own backing array.
	// Concurrent Subscribe/Unsubscribe calls modify eb.subscribers but
	// cannot affect this snapshot, so iteration below is safe without a lock.
	// Releasing the lock before calling handlers also prevents deadlock if
	// a handler calls Subscribe or Unsubscribe.
	subs := slices.Clone(eb.subscribers)
	eb.mu.RUnlock()

	for _, sub := range subs {
		if sub.filter == nil || sub.filter(event.Type) {
			eb.invokeHandler(sub.handler, event)
		}
	}
}

// invokeHandler calls the handler and recovers from any panics so that a
// faulty handler cannot crash the write worker or other internal goroutines.
func (eb *EventBus) invokeHandler(handler EventHandler, event Event) {
	start := time.Now()
	defer func() {
		recover()
		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			slog.Warn("slow event handler detected",
				"event_type", event.Type.String(),
				"duration", duration,
				"recommendation", "handlers must not block; use background goroutines for I/O")
		}
	}()
	handler(event)
}

// Close stops all future event delivery. After Close returns, Emit is a no-op.
// Any Emit calls already in progress will finish delivering to their snapshot
// of subscribers, but no new Emit calls will proceed.
func (eb *EventBus) Close() {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.closed = true
	eb.subscribers = nil
}
