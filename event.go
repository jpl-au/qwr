package qwr

import (
	"database/sql"
	"fmt"
	"time"
)

// EventType identifies the kind of event emitted by the qwr system.
//
// Event uses a single flat struct rather than per-type structs. This avoids
// type assertions and interface hierarchies. The trade-off is that most fields
// are zero-valued for any given event. Each constant below documents exactly
// which Event fields are populated, so you never need to read the emitting
// code to know what data is available.
type EventType int

const (
	// --- Manager lifecycle ---
	// These events carry no extra fields beyond Type and Timestamp.
	// Filter: LifecycleEvents()

	// EventManagerOpened is emitted after Open() completes successfully.
	EventManagerOpened EventType = iota
	// EventManagerClosing is emitted at the start of Close().
	EventManagerClosing
	// EventManagerClosed is emitted at the end of Close(), just before
	// the EventBus itself is closed. This is the last event emitted by
	// a Manager; no events will follow it.
	EventManagerClosed

	// --- Job lifecycle (worker queue) ---
	// Filters: JobEvents(), WriteEvents(). EventJobFailed is also in ErrorEvents().

	// EventJobQueued is emitted when a job enters the worker queue.
	// Fields: JobID, JobType.
	EventJobQueued
	// EventJobStarted is emitted when the worker begins executing a job.
	// Fields: JobID, JobType, QueueWait.
	EventJobStarted
	// EventJobCompleted is emitted when a job finishes successfully.
	// Fields: JobID, JobType, QueueWait, ExecTime.
	// Query jobs also set: SQL, Attempt.
	EventJobCompleted
	// EventJobFailed is emitted when a job finishes with an error.
	// Fields: JobID, JobType, QueueWait, ExecTime, Err.
	// Query jobs also set: SQL, Attempt.
	EventJobFailed

	// --- Direct writes (bypass queue) ---
	// Filter: WriteEvents(). EventDirectWriteFailed is also in ErrorEvents().

	// EventDirectWriteCompleted is emitted after a direct Write() or
	// Transaction.Write() succeeds.
	// Fields: JobID, ExecTime. Query writes also set SQL and Result.
	EventDirectWriteCompleted
	// EventDirectWriteFailed is emitted after a direct Write() or
	// Transaction.Write() fails.
	// Fields: JobID, ExecTime, Err. Query writes also set SQL.
	EventDirectWriteFailed

	// --- Batch lifecycle ---
	// Filter: BatchEvents()

	// EventBatchQueryAdded is emitted when a query is added to the batch.
	// Fields: BatchSize (current count after adding).
	EventBatchQueryAdded
	// EventBatchFlushed is emitted when a batch is flushed for execution.
	// Fields: BatchID, BatchSize, BatchReason ("size_limit"|"timeout"|"close").
	EventBatchFlushed
	// EventBatchInlineOptimized is emitted when inline INSERT combining
	// reduces the number of queries in a batch.
	// Fields: BatchID, OriginalCount, CombinedCount.
	EventBatchInlineOptimized
	// EventBatchSubmitted is emitted after a batch is placed on the worker queue.
	// Fields: BatchID, BatchSize.
	EventBatchSubmitted
	// EventBatchSubmitFailed is emitted when a batch cannot be queued.
	// Fields: BatchID, BatchReason ("worker_not_running"|"queue_full").
	EventBatchSubmitFailed

	// --- Error lifecycle ---
	// Filter: ErrorEvents() (which also includes EventJobFailed,
	// EventDirectWriteFailed, and EventRetryExhausted from other categories).

	// EventErrorStored is emitted when a retriable error is added to the queue.
	// Fields: JobID, Err.
	EventErrorStored
	// EventErrorRemoved is emitted when an error is removed from the queue.
	// Fields: JobID.
	EventErrorRemoved
	// EventErrorQueueCleared is emitted when ClearErrors() is called.
	// No extra fields.
	EventErrorQueueCleared
	// EventErrorQueueOverflow is emitted when the queue exceeds max size
	// and oldest entries are evicted.
	// Fields: EvictedCount.
	EventErrorQueueOverflow
	// EventErrorPersisted is emitted when an error is written to the
	// persistent error log database.
	// Fields: JobID. May also set Err for non-retriable errors.
	EventErrorPersisted
	// EventErrorPersistFailed is emitted when writing to the error log
	// database fails.
	// Fields: JobID, Err.
	EventErrorPersistFailed

	// --- Retry lifecycle ---
	// Filter: RetryEvents(). EventRetryExhausted is also in ErrorEvents().

	// EventRetryScheduled is emitted when a retry is scheduled after a
	// retriable failure.
	// Fields: JobID, Attempt, NextRetry.
	EventRetryScheduled
	// EventRetryStarted is emitted when a scheduled retry begins execution.
	// Fields: JobID, Attempt.
	EventRetryStarted
	// EventRetryExhausted is emitted when all retry attempts are used up.
	// Fields: JobID, Err, Attempt.
	EventRetryExhausted

	// --- Statement cache ---
	// Filter: CacheEvents()

	// EventCacheHit is emitted when a prepared statement is found in cache.
	// Fields: CacheQuery.
	EventCacheHit
	// EventCacheMiss is emitted when a statement is not cached and must be
	// prepared. Emitted after successful preparation.
	// Fields: CacheQuery, CachePrepTime.
	EventCacheMiss
	// EventCacheEvicted is emitted when a statement is evicted from cache.
	// No extra fields.
	EventCacheEvicted
	// EventCachePrepError is emitted when statement preparation fails.
	// Fields: CacheQuery, Err.
	EventCachePrepError

	// --- Backup ---
	// Filter: BackupEvents()

	// EventBackupStarted is emitted when a backup operation begins.
	// Fields: BackupMethod ("api"|"vacuum"), BackupDest.
	EventBackupStarted
	// EventBackupCompleted is emitted when a backup finishes successfully.
	// Fields: BackupMethod, BackupDest.
	EventBackupCompleted
	// EventBackupFailed is emitted when a backup fails.
	// Fields: BackupMethod, BackupDest, Err.
	EventBackupFailed
	// EventBackupFallback is emitted when the backup API is unsupported
	// and qwr falls back to VACUUM INTO.
	// Fields: BackupDest.
	EventBackupFallback

	// --- Maintenance ---
	// No predefined filter; use a custom EventFilter if needed.

	// EventVacuumStarted is emitted before a VACUUM or incremental VACUUM.
	// No extra fields.
	EventVacuumStarted
	// EventVacuumCompleted is emitted after a successful VACUUM.
	// No extra fields.
	EventVacuumCompleted
	// EventVacuumFailed is emitted when a VACUUM fails.
	// Fields: Err.
	EventVacuumFailed
	// EventCheckpointStarted is emitted before a WAL checkpoint.
	// Fields: CheckpointMode.
	EventCheckpointStarted
	// EventCheckpointCompleted is emitted after a successful checkpoint.
	// Fields: CheckpointMode.
	EventCheckpointCompleted
	// EventCheckpointFailed is emitted when a WAL checkpoint fails.
	// Fields: CheckpointMode, Err.
	EventCheckpointFailed

	// --- Worker ---
	// Filter: LifecycleEvents()

	// EventWorkerStarted is emitted when the write worker goroutine starts.
	// No extra fields.
	EventWorkerStarted
	// EventWorkerStopped is emitted when the write worker goroutine exits.
	// No extra fields.
	EventWorkerStopped
)

// String returns the name of the event type (e.g. "EventJobCompleted").
// This makes test failures and log output readable instead of printing raw ints.
func (t EventType) String() string {
	if int(t) < len(eventTypeNames) {
		return eventTypeNames[t]
	}
	return fmt.Sprintf("EventType(%d)", t)
}

// eventTypeNames maps each EventType to its constant name. The order must
// match the iota declaration above.
var eventTypeNames = [...]string{
	EventManagerOpened:         "EventManagerOpened",
	EventManagerClosing:        "EventManagerClosing",
	EventManagerClosed:         "EventManagerClosed",
	EventJobQueued:             "EventJobQueued",
	EventJobStarted:            "EventJobStarted",
	EventJobCompleted:          "EventJobCompleted",
	EventJobFailed:             "EventJobFailed",
	EventDirectWriteCompleted:  "EventDirectWriteCompleted",
	EventDirectWriteFailed:     "EventDirectWriteFailed",
	EventBatchQueryAdded:       "EventBatchQueryAdded",
	EventBatchFlushed:          "EventBatchFlushed",
	EventBatchInlineOptimized:  "EventBatchInlineOptimized",
	EventBatchSubmitted:        "EventBatchSubmitted",
	EventBatchSubmitFailed:     "EventBatchSubmitFailed",
	EventErrorStored:           "EventErrorStored",
	EventErrorRemoved:          "EventErrorRemoved",
	EventErrorQueueCleared:     "EventErrorQueueCleared",
	EventErrorQueueOverflow:    "EventErrorQueueOverflow",
	EventErrorPersisted:        "EventErrorPersisted",
	EventErrorPersistFailed:    "EventErrorPersistFailed",
	EventRetryScheduled:        "EventRetryScheduled",
	EventRetryStarted:          "EventRetryStarted",
	EventRetryExhausted:        "EventRetryExhausted",
	EventCacheHit:              "EventCacheHit",
	EventCacheMiss:             "EventCacheMiss",
	EventCacheEvicted:          "EventCacheEvicted",
	EventCachePrepError:        "EventCachePrepError",
	EventBackupStarted:         "EventBackupStarted",
	EventBackupCompleted:       "EventBackupCompleted",
	EventBackupFailed:          "EventBackupFailed",
	EventBackupFallback:        "EventBackupFallback",
	EventVacuumStarted:         "EventVacuumStarted",
	EventVacuumCompleted:       "EventVacuumCompleted",
	EventVacuumFailed:          "EventVacuumFailed",
	EventCheckpointStarted:     "EventCheckpointStarted",
	EventCheckpointCompleted:   "EventCheckpointCompleted",
	EventCheckpointFailed:      "EventCheckpointFailed",
	EventWorkerStarted:         "EventWorkerStarted",
	EventWorkerStopped:         "EventWorkerStopped",
}

// Event carries data for a single occurrence in the qwr system.
//
// This is a flat struct: all event types share the same fields, and each event
// type only populates the fields listed in its EventType documentation. Fields
// not listed for a given event type will be zero-valued. Always check the Type
// field first to know which other fields are meaningful.
//
// Example handler:
//
//	func(e qwr.Event) {
//	    switch e.Type {
//	    case qwr.EventJobCompleted:
//	        log.Printf("job %d completed in %v: %s", e.JobID, e.ExecTime, e.SQL)
//	    case qwr.EventJobFailed:
//	        log.Printf("job %d failed (attempt %d): %v", e.JobID, e.Attempt, e.Err)
//	    }
//	}
type Event struct {
	// Type identifies the kind of event. All events have a Type and Timestamp.
	Type      EventType
	Timestamp time.Time

	// --- Job context ---
	// Set by: Job lifecycle, Direct write, and Retry events.
	JobID   int64
	JobType JobType
	SQL     string

	// --- Timing ---
	// Set by: EventJobStarted (QueueWait only), EventJobCompleted, EventJobFailed,
	// EventDirectWriteCompleted, EventDirectWriteFailed.
	QueueWait time.Duration
	ExecTime  time.Duration

	// --- Error context ---
	// Err is set by all "Failed" and error-related events.
	Err error
	// EvictedCount is set by EventErrorQueueOverflow: the number of oldest
	// entries removed to bring the queue back within its max size.
	EvictedCount int

	// --- Retry context ---
	// Set by: EventJobCompleted, EventJobFailed (query jobs only),
	// EventRetryScheduled, EventRetryStarted, EventRetryExhausted.
	// Attempt is the zero-based retry count: 0 means the first execution,
	// 1 means the first retry, etc.
	Attempt   int
	NextRetry time.Time

	// --- Batch context ---
	// Set by: Batch lifecycle events.
	BatchID       int64
	BatchSize     int
	BatchReason   string // "size_limit", "timeout", "close"
	OriginalCount int
	CombinedCount int

	// --- Cache context ---
	// Set by: EventCacheHit, EventCacheMiss, EventCachePrepError.
	CacheQuery    string
	CachePrepTime time.Duration

	// --- Backup/maintenance context ---
	// Set by: Backup and Checkpoint events.
	BackupMethod   string
	BackupDest     string
	CheckpointMode string // "PASSIVE", "FULL", "RESTART", "TRUNCATE"

	// --- Result ---
	// Set by: EventDirectWriteCompleted.
	Result sql.Result
}
