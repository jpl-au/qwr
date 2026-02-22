package qwr

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/jpl-au/qwr/checkpoint"
	"github.com/jpl-au/qwr/profile"
	_ "modernc.org/sqlite"
)

// Manager handles serialised database operations
type Manager struct {
	reader         *sql.DB
	writer         *sql.DB
	serialiser     *WriteSerialiser
	batcher        *BatchCollector
	errorQueue     *ErrorQueue
	events         *EventBus
	options        Options
	ctx            context.Context // User-facing context (can be nil)
	internalCtx    context.Context // Internal context for batching (never nil)
	readerProfile  *profile.Profile
	writerProfile  *profile.Profile
	readStmtCache  *StmtCache
	writeStmtCache *StmtCache
	path           string          // Database path for logging context
	checkpoint     checkpoint.Mode // WAL checkpoint mode to run on Close()
}

// Database extracts the database filename for logging context
func (m *Manager) Database() string {
	if m.path == "" {
		return ""
	}
	return filepath.Base(m.path)
}

// Subscribe registers an event handler that receives all events.
// Returns a subscription ID for later removal via Unsubscribe.
func (m *Manager) Subscribe(handler EventHandler) uint64 {
	return m.events.Subscribe(handler)
}

// SubscribeFiltered registers an event handler with a filter.
// The handler only receives events for which filter returns true.
func (m *Manager) SubscribeFiltered(handler EventHandler, filter EventFilter) uint64 {
	return m.events.SubscribeFiltered(handler, filter)
}

// Unsubscribe removes a previously registered event handler.
func (m *Manager) Unsubscribe(id uint64) {
	m.events.Unsubscribe(id)
}

// Query creates a new query with the given SQL and arguments
func (m *Manager) Query(sql string, args ...any) *QueryBuilder {
	qb := GetQueryBuilder()
	qb.manager = m
	qb.query.SQL = sql
	qb.query.prepared = m.options.UsePreparedStatements
	qb.useContexts = m.options.UseContexts

	// Ensure Args slice has enough capacity
	if cap(qb.query.Args) < len(args) {
		qb.query.Args = make([]any, len(args))
	} else {
		qb.query.Args = qb.query.Args[:len(args)]
	}
	copy(qb.query.Args, args)

	return qb
}

// Batch adds a job to be executed as part of a batch
func (m *Manager) Batch(job Job) error {
	if m.writer == nil || m.serialiser == nil || m.batcher == nil {
		return ErrWriterDisabled
	}

	m.batcher.Add(job)
	return nil
}

// handleRetryEvent processes failed job events for automatic retry.
// Registered as an internal subscriber when EnableAutoRetry is true.
func (m *Manager) handleRetryEvent(e Event) {
	if m.errorQueue == nil {
		return
	}

	// Only handle async query failures
	if e.JobType != JobTypeQuery {
		return
	}

	qwrErr := ClassifyError(e.Err, "async_query")
	jobErr := JobError{
		Query:     Query{SQL: e.SQL, id: e.JobID, async: true, retries: e.Attempt},
		err:       e.Err,
		timestamp: time.Now(),
		duration:  e.ExecTime,
		errType:   qwrErr,
	}

	// Calculate next retry time if it's retriable
	if qwrErr.IsRetriable() {
		jobErr.CalculateNextRetry(m.options.BaseRetryDelay)
	}

	m.errorQueue.Store(jobErr)

	if !qwrErr.IsRetriable() || jobErr.Query.retries >= m.options.MaxRetries {
		if qwrErr.IsRetriable() {
			m.events.Emit(Event{Type: EventRetryExhausted, JobID: e.JobID, Err: e.Err, Attempt: jobErr.Query.retries})
		}
		return
	}

	m.events.Emit(Event{Type: EventRetryScheduled, JobID: e.JobID, Attempt: jobErr.Query.retries + 1, NextRetry: jobErr.nextRetryAt})

	// Schedule retry at the exact calculated time
	delay := time.Until(jobErr.nextRetryAt)
	if delay < 0 {
		delay = 0
	}

	time.AfterFunc(delay, func() {
		m.events.Emit(Event{Type: EventRetryStarted, JobID: e.JobID, Attempt: jobErr.Query.retries + 1})

		retryQuery := jobErr.CreateRetryQuery()
		retryQuery.async = true

		ctx, cancel := context.WithTimeout(context.Background(), m.options.RetrySubmitTimeout)
		_, err := m.serialiser.SubmitNoWait(ctx, NewQueryJob(retryQuery))
		cancel()

		if err != nil {
			if retryQuery.retries >= m.options.MaxRetries {
				m.errorQueue.Remove(jobErr.Query.ID())
				m.errorQueue.PersistError(jobErr, "max_retries_exceeded")
				m.events.Emit(Event{Type: EventRetryExhausted, JobID: e.JobID, Err: e.Err, Attempt: retryQuery.retries})
			}
		} else {
			m.errorQueue.Remove(jobErr.Query.ID())
		}
	})
}

// Close closes all database connections and stops the worker pool
func (m *Manager) Close() error {
	var errs []error

	m.events.Emit(Event{Type: EventManagerClosing})

	// Stop batch collector
	if m.batcher != nil {
		m.batcher.Close()
	}

	// Stop worker pool
	if m.serialiser != nil {
		if err := m.serialiser.Stop(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close error queue
	if m.errorQueue != nil {
		m.errorQueue.Close()
	}

	// Close statement caches
	if m.readStmtCache != nil {
		m.readStmtCache.Close()
	}

	if m.writeStmtCache != nil {
		m.writeStmtCache.Close()
	}

	// Run WAL checkpoint if configured (before closing writer)
	if m.checkpoint != checkpoint.None && m.writer != nil {
		m.events.Emit(Event{Type: EventCheckpointStarted, CheckpointMode: string(m.checkpoint)})
		if _, err := m.writer.Exec(fmt.Sprintf("PRAGMA wal_checkpoint(%s)", m.checkpoint)); err != nil {
			m.events.Emit(Event{Type: EventCheckpointFailed, CheckpointMode: string(m.checkpoint), Err: err})
			errs = append(errs, err)
		} else {
			m.events.Emit(Event{Type: EventCheckpointCompleted, CheckpointMode: string(m.checkpoint)})
		}
	}

	// Close writer
	if m.writer != nil {
		if err := m.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close reader
	if m.reader != nil {
		if err := m.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	m.events.Emit(Event{Type: EventManagerClosed})
	m.events.Close()

	if len(errs) > 0 {
		return errs[0] // Return first error for now
	}
	return nil
}

// WaitForIdle waits until all operations are processed
func (m *Manager) WaitForIdle(ctx context.Context) error {
	// Start with shorter intervals, back off as we wait longer
	intervals := []time.Duration{
		50 * time.Millisecond,  // First few checks
		100 * time.Millisecond, // Short-term waiting
		250 * time.Millisecond, // Medium-term waiting
		500 * time.Millisecond, // Long-term waiting
	}

	intervalIndex := 0
	checkCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Check if queue is empty and batch collector is idle
			queueLen := 0
			if m.serialiser != nil {
				queueLen = m.serialiser.QueueLen()
			}
			batchSize := 0
			if m.batcher != nil {
				m.batcher.mutex.Lock()
				batchSize = len(m.batcher.queries)
				m.batcher.mutex.Unlock()
			}

			if queueLen == 0 && batchSize == 0 {
				return nil
			}

			// Progressive backoff
			currentInterval := intervals[intervalIndex]
			if checkCount > 10 && intervalIndex < len(intervals)-1 {
				intervalIndex++
				checkCount = 0
			}

			time.Sleep(currentInterval)
			checkCount++
		}
	}
}

// GetReaderProfile returns the current reader profile
func (m *Manager) GetReaderProfile() *profile.Profile {
	return m.readerProfile
}

// GetWriterProfile returns the current writer profile
func (m *Manager) GetWriterProfile() *profile.Profile {
	return m.writerProfile
}

// GetErrors returns all errors in the error queue
func (m *Manager) GetErrors() []JobError {
	if m.errorQueue == nil {
		return nil
	}
	return m.errorQueue.GetAll()
}

// GetErrorByID retrieves a specific error from the queue
func (m *Manager) GetErrorByID(jobID int64) (JobError, bool) {
	if m.errorQueue == nil {
		return JobError{}, false
	}
	return m.errorQueue.Get(jobID)
}

// RemoveError removes a specific error from the queue
func (m *Manager) RemoveError(jobID int64) bool {
	if m.errorQueue == nil {
		return false
	}
	return m.errorQueue.Remove(jobID)
}

// ClearErrors removes all errors from the queue
func (m *Manager) ClearErrors() {
	if m.errorQueue != nil {
		m.errorQueue.Clear()
	}
}

// RetryJob manually retries a failed job by its ID
func (m *Manager) RetryJob(jobID int64) error {
	if m.errorQueue == nil {
		return ErrErrorQueueDisabled
	}

	jobError, found := m.errorQueue.Get(jobID)
	if !found {
		return ErrJobNotFound
	}

	retryQuery := jobError.CreateRetryQuery()
	retryQuery.async = true

	ctx, cancel := context.WithTimeout(context.Background(), m.options.RetrySubmitTimeout)
	defer cancel()

	_, err := m.serialiser.SubmitNoWait(ctx, NewQueryJob(retryQuery))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRetrySubmissionFailed, err)
	}

	// Remove from error queue since we successfully resubmitted
	m.errorQueue.Remove(jobID)
	return nil
}

// ResetCaches clears all cached prepared statements, freeing memory.
// The cache remains usable - new statements will be prepared on demand.
func (m *Manager) ResetCaches() {
	if m.readStmtCache != nil {
		m.readStmtCache.Clear()
	}
	if m.writeStmtCache != nil {
		m.writeStmtCache.Clear()
	}
}

// RunVacuum performs a VACUUM operation (full database rebuild)
func (m *Manager) RunVacuum() error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	m.events.Emit(Event{Type: EventVacuumStarted})
	_, err := m.writer.Exec("VACUUM")
	if err != nil {
		m.events.Emit(Event{Type: EventVacuumFailed, Err: err})
	} else {
		m.events.Emit(Event{Type: EventVacuumCompleted})
	}
	return err
}

// RunIncrementalVacuum performs incremental vacuum if enabled
func (m *Manager) RunIncrementalVacuum(pages int) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	if pages < 0 {
		return fmt.Errorf("invalid pages value: must be non-negative, got %d", pages)
	}
	m.events.Emit(Event{Type: EventVacuumStarted})
	_, err := m.writer.Exec("PRAGMA incremental_vacuum(?)", pages)
	if err != nil {
		m.events.Emit(Event{Type: EventVacuumFailed, Err: err})
	} else {
		m.events.Emit(Event{Type: EventVacuumCompleted})
	}
	return err
}

// RunCheckpoint triggers a WAL checkpoint
func (m *Manager) RunCheckpoint(mode checkpoint.Mode) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	if mode == checkpoint.None {
		return nil
	}
	m.events.Emit(Event{Type: EventCheckpointStarted, CheckpointMode: string(mode)})
	_, err := m.writer.Exec(fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode))
	if err != nil {
		m.events.Emit(Event{Type: EventCheckpointFailed, CheckpointMode: string(mode), Err: err})
	} else {
		m.events.Emit(Event{Type: EventCheckpointCompleted, CheckpointMode: string(mode)})
	}
	return err
}

// SetSecureDelete enables or disables secure_delete
func (m *Manager) SetSecureDelete(enabled bool) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	val := "OFF"
	if enabled {
		val = "ON"
	}
	_, err := m.writer.Exec(fmt.Sprintf("PRAGMA secure_delete = %s", val))
	return err
}

// GetJobStatus checks if a job failed by looking in the error queue
func (m *Manager) GetJobStatus(jobID int64) (string, error) {
	if m.errorQueue == nil {
		return "", ErrErrorQueueDisabled
	}

	if _, found := m.errorQueue.Get(jobID); found {
		return "failed", nil
	}

	return "unknown", nil
}
