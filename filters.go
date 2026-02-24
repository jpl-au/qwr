package qwr

// JobEvents returns a filter matching job lifecycle events:
// EventJobQueued, EventJobStarted, EventJobCompleted, EventJobFailed.
func JobEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventJobQueued, EventJobStarted, EventJobCompleted, EventJobFailed:
			return true
		}
		return false
	}
}

// ErrorEvents returns a filter matching all events that indicate something
// went wrong, across multiple categories. This includes job failures
// (EventJobFailed, EventDirectWriteFailed) in addition to error queue and
// retry exhaustion events.
//
// Matches: EventJobFailed, EventDirectWriteFailed, EventReaderQueryFailed,
// EventRetrySubmitFailed, EventErrorStored, EventErrorPersisted,
// EventErrorQueueOverflow, EventRetryExhausted.
func ErrorEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventJobFailed, EventDirectWriteFailed, EventReaderQueryFailed,
			EventRetrySubmitFailed, EventErrorStored, EventErrorPersisted,
			EventErrorQueueOverflow, EventRetryExhausted:
			return true
		}
		return false
	}
}

// ReadEvents returns a filter matching reader-side events:
// EventReaderQueryCompleted, EventReaderQueryFailed.
func ReadEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventReaderQueryCompleted, EventReaderQueryFailed:
			return true
		}
		return false
	}
}

// CacheEvents returns a filter matching statement cache events:
// EventCacheHit, EventCacheMiss, EventCacheEvicted, EventCachePrepError.
func CacheEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventCacheHit, EventCacheMiss, EventCacheEvicted, EventCachePrepError:
			return true
		}
		return false
	}
}

// RetryEvents returns a filter matching retry lifecycle events:
// EventRetryScheduled, EventRetryStarted, EventRetryExhausted, EventRetrySubmitFailed.
func RetryEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventRetryScheduled, EventRetryStarted, EventRetryExhausted, EventRetrySubmitFailed:
			return true
		}
		return false
	}
}

// WriteEvents returns a filter matching all write-related events, both
// queued (job lifecycle) and direct:
// EventJobQueued, EventJobStarted, EventJobCompleted, EventJobFailed,
// EventDirectWriteCompleted, EventDirectWriteFailed.
func WriteEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventJobQueued, EventJobStarted, EventJobCompleted, EventJobFailed,
			EventDirectWriteCompleted, EventDirectWriteFailed:
			return true
		}
		return false
	}
}

// BatchEvents returns a filter matching batch lifecycle events:
// EventBatchQueryAdded, EventBatchFlushed, EventBatchInlineOptimised,
// EventBatchSubmitted, EventBatchSubmitFailed.
func BatchEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventBatchQueryAdded, EventBatchFlushed, EventBatchInlineOptimised,
			EventBatchSubmitted, EventBatchSubmitFailed:
			return true
		}
		return false
	}
}

// BackupEvents returns a filter matching backup events:
// EventBackupStarted, EventBackupCompleted, EventBackupFailed, EventBackupFallback.
func BackupEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventBackupStarted, EventBackupCompleted, EventBackupFailed, EventBackupFallback:
			return true
		}
		return false
	}
}

// LifecycleEvents returns a filter matching manager and worker lifecycle events:
// EventManagerOpened, EventManagerClosing, EventManagerClosed,
// EventWorkerStarted, EventWorkerStopped.
func LifecycleEvents() EventFilter {
	return func(t EventType) bool {
		switch t {
		case EventManagerOpened, EventManagerClosing, EventManagerClosed,
			EventWorkerStarted, EventWorkerStopped:
			return true
		}
		return false
	}
}
