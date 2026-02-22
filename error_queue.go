package qwr

import (
	"container/list"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/jpl-au/qwr/profile"
)

// ErrorQueue maintains a simple registry of failed async operations
type ErrorQueue struct {
	mu        sync.RWMutex
	errors    map[int64]*list.Element
	errorList *list.List
	db        *sql.DB

	events  *EventBus
	options Options // Store options by value since they're immutable
	dbPath  string  // Database path for logging context
}

// NewErrorQueue creates a new error queue
func NewErrorQueue(events *EventBus, opts Options, dbPath string) *ErrorQueue {
	eq := &ErrorQueue{
		errors:    make(map[int64]*list.Element),
		errorList: list.New(),
		events:    events,
		options:   opts, // Store by value
		dbPath:    dbPath,
	}

	// Only initialize error log DB if ErrorLogPath is set and is not :memory:
	// We reject :memory: to prevent unbounded memory growth in long-running apps
	if opts.ErrorLogPath != "" && opts.ErrorLogPath != ":memory:" {
		eq.initErrorLogDb(opts.ErrorLogPath)
	}

	return eq
}

// Close shuts down the error queue
func (eq *ErrorQueue) Close() {
	if eq.db != nil {
		eq.db.Close()
	}
}

// Store adds or overwrites an error in the queue, or immediately persists non-retriable errors
func (eq *ErrorQueue) Store(jobErr JobError) {
	// Non-retriable errors are immediately persisted to DB and not queued for retry
	if jobErr.errType == nil || !jobErr.errType.IsRetriable() {
		eq.PersistError(jobErr, "non_retriable")
		eq.events.Emit(Event{Type: EventErrorPersisted, JobID: jobErr.Query.ID(), Err: jobErr.err})
		return
	}

	eq.mu.Lock()
	defer eq.mu.Unlock()

	if elem, exists := eq.errors[jobErr.Query.ID()]; exists {
		elem.Value = jobErr
		eq.errorList.MoveToBack(elem)
		return
	}

	elem := eq.errorList.PushBack(jobErr)
	eq.errors[jobErr.Query.ID()] = elem

	eq.events.Emit(Event{Type: EventErrorStored, JobID: jobErr.Query.ID(), Err: jobErr.err})

	eq.enforceMaxSize()
}

// Get retrieves a specific error by job ID
func (eq *ErrorQueue) Get(jobID int64) (JobError, bool) {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	if elem, exists := eq.errors[jobID]; exists {
		jobErr, ok := elem.Value.(JobError)
		if !ok {
			return JobError{}, false
		}
		return jobErr, true
	}
	return JobError{}, false
}

// Remove removes an error from the queue
func (eq *ErrorQueue) Remove(jobID int64) bool {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if elem, exists := eq.errors[jobID]; exists {
		eq.errorList.Remove(elem)
		delete(eq.errors, jobID)
		eq.events.Emit(Event{Type: EventErrorRemoved, JobID: jobID})
		return true
	}
	return false
}

// GetAll returns all errors in the queue in chronological order
func (eq *ErrorQueue) GetAll() []JobError {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	errors := make([]JobError, 0, eq.errorList.Len())
	for e := eq.errorList.Front(); e != nil; e = e.Next() {
		jobErr, ok := e.Value.(JobError)
		if ok {
			errors = append(errors, jobErr)
		}
	}
	return errors
}

// Count returns the number of errors in the queue
func (eq *ErrorQueue) Count() int {
	eq.mu.RLock()
	defer eq.mu.RUnlock()
	return eq.errorList.Len()
}

// Clear removes all errors from the queue
func (eq *ErrorQueue) Clear() {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	eq.events.Emit(Event{Type: EventErrorQueueCleared})
	eq.errors = make(map[int64]*list.Element)
	eq.errorList = list.New()
}

// enforceMaxSize removes oldest entries when queue exceeds max size
func (eq *ErrorQueue) enforceMaxSize() {
	if eq.options.ErrorQueueMaxSize <= 0 {
		return
	}

	excess := eq.errorList.Len() - eq.options.ErrorQueueMaxSize
	if excess <= 0 {
		return
	}

	eq.events.Emit(Event{Type: EventErrorQueueOverflow, EvictedCount: excess})

	for i := 0; i < excess && eq.errorList.Front() != nil; i++ {
		oldest := eq.errorList.Front()
		jobErr, ok := oldest.Value.(JobError)
		if !ok {
			// Skip corrupted entry
			eq.errorList.Remove(oldest)
			continue
		}

		eq.PersistError(jobErr, "queue_overflow")
		eq.errorList.Remove(oldest)
		delete(eq.errors, jobErr.Query.ID())
	}
}

// GetReadyForRetry returns errors that are ready for retry
func (eq *ErrorQueue) GetReadyForRetry(now time.Time, maxRetries int) []JobError {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	readyErrors := make([]JobError, 0)
	for e := eq.errorList.Front(); e != nil; e = e.Next() {
		jobErr, ok := e.Value.(JobError)
		if !ok {
			continue
		}

		if jobErr.ShouldRetry(maxRetries) &&
			!jobErr.nextRetryAt.IsZero() &&
			now.After(jobErr.nextRetryAt) {
			readyErrors = append(readyErrors, jobErr)
		}
	}
	return readyErrors
}

// PersistError logs an error to the database
func (eq *ErrorQueue) PersistError(jobErr JobError, reason string) error {
	if eq.db == nil {
		return nil
	}

	var argsCbor []byte

	if len(jobErr.Query.Args) > 0 {
		if encoded, err := cbor.Marshal(jobErr.Query.Args); err == nil {
			argsCbor = encoded
		}
	}

	insertSQL := `
		INSERT INTO error_log (
			job_id, error_message, error_type, sql_statement,
			args_cbor, attempts, first_error_at, last_error_at, reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := eq.db.Exec(insertSQL,
		jobErr.Query.ID(),
		jobErr.err.Error(),
		func() string {
			if jobErr.errType != nil {
				return jobErr.errType.Strategy.String()
			}
			return "unknown"
		}(),
		jobErr.Query.SQL,
		argsCbor,
		jobErr.Query.retries,
		jobErr.timestamp,
		time.Now(),
		reason)

	if err != nil {
		eq.events.Emit(Event{Type: EventErrorPersistFailed, Err: err, JobID: jobErr.Query.ID()})
	} else {
		eq.events.Emit(Event{Type: EventErrorPersisted, JobID: jobErr.Query.ID()})
	}

	return err
}

// initErrorLogDb creates and initialises the error log database
func (eq *ErrorQueue) initErrorLogDb(errorLogPath string) {
	var err error

	eq.db, err = sql.Open("sqlite", errorLogPath)
	if err != nil {
		slog.Error("Failed to open error log database", "path", errorLogPath, "error", err)
		return
	}

	p := profile.WriteBalanced()

	if err := p.Apply(eq.db); err != nil {
		slog.Error("Failed to apply profile to error log database", "error", err)
		eq.db.Close()
		return
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS error_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			error_message TEXT NOT NULL,
			error_type TEXT NOT NULL,
			sql_statement TEXT,
			args_cbor BLOB,
			attempts INTEGER DEFAULT 1,
			first_error_at DATETIME,
			last_error_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			reason TEXT NOT NULL
		)
	`

	if _, err := eq.db.Exec(createTableSQL); err != nil {
		slog.Error("Failed to create error log table", "error", err)
		eq.db.Close()
		return
	}
}
