package qwr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
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

	// ATTACH support
	attachMu        sync.Mutex   // Protects attachments slice
	attachments     []attachment // Attached databases for checkpoint on close
	readerConnector *connInit    // Connector for reader pool (nil for NewSQL)
	writerConnector *connInit    // Connector for writer pool (nil for NewSQL)
	isSQL           bool         // true when created via NewSQL

	// Shutdown coordination
	closed      atomic.Bool    // fast check for retry callbacks and submissions
	closeOnce   sync.Once      // ensures Close() runs exactly once
	closeErr    error          // cached result from first Close()
	retryMu     sync.Mutex     // protects retryTimers slice
	retryTimers []*time.Timer  // pending retry timers for cancellation on Close()
	retryWg     sync.WaitGroup // tracks in-flight retry callbacks
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

	return m.batcher.Add(job)
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
	delay := max(time.Until(jobErr.nextRetryAt), 0)

	// Don't schedule if shutdown has begun
	if m.closed.Load() {
		return
	}

	m.retryWg.Add(1)
	timer := time.AfterFunc(delay, func() {
		defer m.retryWg.Done()

		// Bail out if shutdown has begun
		if m.closed.Load() {
			return
		}

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
			} else {
				m.events.Emit(Event{Type: EventRetrySubmitFailed, JobID: e.JobID, Err: err})
				slog.Warn("failed to submit background retry",
					"job_id", e.JobID,
					"error", err,
					"attempt", retryQuery.retries)
			}
		} else {
			m.errorQueue.Remove(jobErr.Query.ID())
		}
	})

	m.retryMu.Lock()
	m.retryTimers = append(m.retryTimers, timer)
	m.retryMu.Unlock()
}

// cancelRetryTimers stops all pending retry timers and waits for any
// in-flight retry callbacks to complete. Must be called before closing
// the serialiser, error queue, or event bus.
func (m *Manager) cancelRetryTimers() {
	m.closed.Store(true)

	m.retryMu.Lock()
	for _, t := range m.retryTimers {
		t.Stop()
	}
	m.retryTimers = nil
	m.retryMu.Unlock()

	m.retryWg.Wait()
}

// Close closes all database connections and stops the worker pool.
// Safe to call multiple times - subsequent calls return the same result.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.closeErr = m.doClose()
	})
	return m.closeErr
}

func (m *Manager) doClose() error {
	var errs []error

	m.events.Emit(Event{Type: EventManagerClosing})

	// Cancel pending retries before stopping dependent components
	m.cancelRetryTimers()

	// Stop batch collector
	if m.batcher != nil {
		if err := m.batcher.Close(); err != nil {
			errs = append(errs, err)
		}
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
		// Checkpoint main database
		m.events.Emit(Event{Type: EventCheckpointStarted, CheckpointMode: string(m.checkpoint)})
		if _, err := m.writer.Exec(fmt.Sprintf("PRAGMA wal_checkpoint(%s)", m.checkpoint)); err != nil {
			m.events.Emit(Event{Type: EventCheckpointFailed, CheckpointMode: string(m.checkpoint), Err: err})
			errs = append(errs, err)
		} else {
			m.events.Emit(Event{Type: EventCheckpointCompleted, CheckpointMode: string(m.checkpoint)})
		}

		// Checkpoint each attached database
		m.attachMu.Lock()
		atts := m.attachments
		m.attachMu.Unlock()
		for _, att := range atts {
			stmt := fmt.Sprintf("PRAGMA %s.wal_checkpoint(%s)", att.alias, m.checkpoint)
			if _, err := m.writer.Exec(stmt); err != nil {
				errs = append(errs, fmt.Errorf("checkpoint %s: %w", att.alias, err))
			}
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

	return errors.Join(errs...)
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

// ReaderProfile returns the current reader profile
func (m *Manager) ReaderProfile() *profile.Profile {
	return m.readerProfile
}

// WriterProfile returns the current writer profile
func (m *Manager) WriterProfile() *profile.Profile {
	return m.writerProfile
}

// Errors returns all errors in the error queue
func (m *Manager) Errors() []JobError {
	if m.errorQueue == nil {
		return nil
	}
	return m.errorQueue.GetAll()
}

// ErrorByID retrieves a specific error from the queue
func (m *Manager) ErrorByID(jobID int64) (JobError, bool) {
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

// RunVacuum performs a VACUUM operation on the main database or an
// attached database if a schema name is provided.
func (m *Manager) RunVacuum(schema ...string) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	if m.writer == nil {
		return ErrWriterDisabled
	}
	stmt := "VACUUM"
	if len(schema) > 0 && schema[0] != "" {
		if err := validSchema(schema[0]); err != nil {
			return err
		}
		stmt = fmt.Sprintf("VACUUM %s", schema[0])
	}
	m.events.Emit(Event{Type: EventVacuumStarted})
	_, err := m.writer.Exec(stmt)
	if err != nil {
		m.events.Emit(Event{Type: EventVacuumFailed, Err: err})
	} else {
		m.events.Emit(Event{Type: EventVacuumCompleted})
	}
	return err
}

// RunIncrementalVacuum performs incremental vacuum on the main database
// or an attached database if a schema name is provided.
func (m *Manager) RunIncrementalVacuum(pages int, schema ...string) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	if m.writer == nil {
		return ErrWriterDisabled
	}
	if pages < 0 {
		return fmt.Errorf("invalid pages value: must be non-negative, got %d", pages)
	}
	prefix := "PRAGMA"
	if len(schema) > 0 && schema[0] != "" {
		if err := validSchema(schema[0]); err != nil {
			return err
		}
		prefix = fmt.Sprintf("PRAGMA %s.", schema[0])
	}
	m.events.Emit(Event{Type: EventVacuumStarted})
	_, err := m.writer.Exec(fmt.Sprintf("%sincremental_vacuum(%d)", prefix, pages))
	if err != nil {
		m.events.Emit(Event{Type: EventVacuumFailed, Err: err})
	} else {
		m.events.Emit(Event{Type: EventVacuumCompleted})
	}
	return err
}

// RunCheckpoint triggers a WAL checkpoint on the main database or an
// attached database if a schema name is provided.
func (m *Manager) RunCheckpoint(mode checkpoint.Mode, schema ...string) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	if m.writer == nil {
		return ErrWriterDisabled
	}
	if mode == checkpoint.None {
		return nil
	}
	if !mode.Valid() {
		return fmt.Errorf("%w: %q", checkpoint.ErrInvalidMode, mode)
	}
	prefix := "PRAGMA"
	if len(schema) > 0 && schema[0] != "" {
		if err := validSchema(schema[0]); err != nil {
			return err
		}
		prefix = fmt.Sprintf("PRAGMA %s.", schema[0])
	}
	m.events.Emit(Event{Type: EventCheckpointStarted, CheckpointMode: string(mode)})
	_, err := m.writer.Exec(fmt.Sprintf("%swal_checkpoint(%s)", prefix, mode))
	if err != nil {
		m.events.Emit(Event{Type: EventCheckpointFailed, CheckpointMode: string(mode), Err: err})
	} else {
		m.events.Emit(Event{Type: EventCheckpointCompleted, CheckpointMode: string(mode)})
	}
	return err
}

// Attach attaches a database at runtime. The ATTACH statement runs immediately
// on the writer connection. The reader connector is updated so new reader
// connections get the attachment automatically as the pool recycles them.
// Call ResetReaderPool to force immediate reader access to the attached database.
//
// An optional profile configures per-schema PRAGMAs for the attached database.
// Only PRAGMA settings are used - pool parameters are ignored.
//
// Not supported for managers created with NewSQL.
func (m *Manager) Attach(alias, path string, p ...*profile.Profile) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	if m.isSQL {
		return ErrAttachNotSupported
	}

	var prof *profile.Profile
	if len(p) > 0 {
		prof = p[0]
	}

	att, err := newAttachment(alias, path, m.path, prof)
	if err != nil {
		return err
	}

	m.attachMu.Lock()
	defer m.attachMu.Unlock()

	// Check for duplicate alias across existing attachments
	for _, existing := range m.attachments {
		if existing.alias == alias {
			return fmt.Errorf("%w: %q", ErrAttachDuplicateAlias, alias)
		}
	}
	if m.readerConnector != nil && m.readerConnector.hasAlias(alias) {
		return fmt.Errorf("%w: %q", ErrAttachDuplicateAlias, alias)
	}

	// Run ATTACH + PRAGMAs on the writer using a single raw connection.
	// This prevents the write worker from interleaving between the ATTACH
	// and its per-schema PRAGMAs.
	if m.writer != nil {
		if err := m.attachOnWriter(att); err != nil {
			return err
		}
	}

	// Update connectors so new connections get the attachment
	if m.readerConnector != nil {
		m.readerConnector.addAttachment(att)
	}
	if m.writerConnector != nil {
		m.writerConnector.addAttachment(att)
	}

	// Store for close-time checkpoint
	m.attachments = append(m.attachments, att)
	return nil
}

// attachOnWriter runs ATTACH + per-schema PRAGMAs on a single raw connection,
// preventing the write worker from interleaving between statements.
func (m *Manager) attachOnWriter(att attachment) error {
	conn, err := m.writer.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire writer connection for attach: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(context.Background(), att.attachSQL, att.path); err != nil {
		return fmt.Errorf("failed to attach %q on writer: %w", att.alias, err)
	}

	for _, stmt := range att.schemaStatements {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			// Rollback the ATTACH to avoid a phantom attachment
			conn.ExecContext(context.Background(), fmt.Sprintf("DETACH DATABASE %s", att.alias)) //nolint:errcheck
			return fmt.Errorf("failed to apply pragma for %q on writer: %w", att.alias, err)
		}
	}

	return nil
}

// ResetReaderPool closes all existing reader connections and creates a
// new reader pool using the same connector. New connections will include
// any attachments added via Manager.Attach since the pool was last created.
// In-flight read operations on the old pool may fail.
func (m *Manager) ResetReaderPool() error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	if m.reader == nil {
		return ErrReaderDisabled
	}
	if m.readerConnector == nil {
		return errors.New("reader pool reset requires a qwr-managed connector (not available with NewSQL)")
	}

	// Build new resources before touching the old ones. This ensures we
	// don't leave the manager in a broken state if creation fails.
	newReader := sql.OpenDB(m.readerConnector)
	m.readerProfile.ApplyPool(newReader)

	if err := newReader.Ping(); err != nil {
		newReader.Close()
		return fmt.Errorf("failed to verify new reader pool: %w", err)
	}

	newCache, err := NewStmtCache(m.events, m.options)
	if err != nil {
		newReader.Close()
		return fmt.Errorf("failed to create reader statement cache: %w", err)
	}

	// Swap: store old references, install new ones
	oldReader := m.reader
	oldCache := m.readStmtCache
	m.reader = newReader
	m.readStmtCache = newCache

	// Close old resources after the swap
	if oldCache != nil {
		oldCache.Close()
	}
	oldReader.Close()

	return nil
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

// JobStatus checks if a job failed by looking in the error queue
func (m *Manager) JobStatus(jobID int64) (string, error) {
	if m.errorQueue == nil {
		return "", ErrErrorQueueDisabled
	}

	if _, found := m.errorQueue.Get(jobID); found {
		return "failed", nil
	}

	return "unknown", nil
}
