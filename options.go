package qwr

import (
	"time"
)

// Options holds configuration for the qwr manager's internal behaviour.
//
// Options are immutable after manager startup. They can only be set
// during manager construction via New() and cannot be modified at runtime.
// To change options, stop the application, create a new manager with
// different options, and restart.
type Options struct {
	// WorkerQueueDepth sets the buffer size for the serialised write queue.
	// A deeper queue absorbs bursts without blocking callers but uses more
	// memory. Too shallow and Execute/Async calls block waiting for space.
	// Default: 1000.
	WorkerQueueDepth int

	// EnableReader creates the reader connection pool. Disable for
	// write-only applications to avoid opening unused connections.
	// Default: true.
	EnableReader bool

	// EnableWriter creates the writer connection and serialised worker.
	// Disable for read-only applications. Without a writer, all write
	// methods (Write, Execute, Async, Batch) return ErrWriterDisabled.
	// Default: true.
	EnableWriter bool

	// BatchSize is the number of queries collected before the batch
	// collector flushes automatically. Larger batches amortise the
	// per-transaction cost across more rows but increase latency for
	// individual writes and memory held by the collector.
	// Default: 200.
	BatchSize int

	// BatchTimeout is the maximum time the collector waits before
	// flushing a partial batch. Prevents stale writes sitting in the
	// buffer when write volume is too low to trigger a size-based flush.
	// Default: 1s.
	BatchTimeout time.Duration

	// InlineInserts enables combining of identically-structured INSERT
	// statements within a batch into a single multi-value INSERT. This
	// reduces round-trips but relies on simple string parsing - it will
	// produce incorrect results for INSERT...SELECT or VALUES clauses
	// containing parentheses in string literals.
	//
	// Only enable when you control the SQL structure and all batched
	// inserts follow the pattern INSERT INTO t (...) VALUES (?,...).
	// Default: false.
	InlineInserts bool

	// UseContexts makes all operations use context-aware database methods
	// by default. When false, context methods are only used when the caller
	// explicitly calls WithContext(). The non-context path avoids the
	// overhead of context propagation for applications that don't need
	// cancellation or deadlines.
	// Default: false.
	UseContexts bool

	// StmtCacheMaxSize caps the number of prepared statements held in
	// cache. Eviction is LRU. Raise this for applications with many
	// distinct queries; lower it to reduce memory under constrained
	// environments. Each cached statement holds a database-side resource.
	// Default: 1000.
	StmtCacheMaxSize int

	// ErrorQueueMaxSize caps the number of errors retained in memory
	// for inspection via GetErrorByID. When full, oldest errors are
	// evicted. If ErrorLogPath is set, evicted errors are persisted
	// to disk first.
	// Default: 1000.
	ErrorQueueMaxSize int

	// EnableAutoRetry automatically resubmits failed async jobs that
	// hit retriable errors (SQLITE_BUSY, SQLITE_LOCKED, timeouts).
	// Retries use exponential backoff governed by BaseRetryDelay and
	// MaxRetries. Disable for applications that need manual control
	// over retry logic.
	// Default: false.
	EnableAutoRetry bool

	// MaxRetries is the maximum retry attempts for a failed async job
	// before it is marked permanently failed and added to the error
	// queue. Only applies when EnableAutoRetry is true.
	// Default: 3.
	MaxRetries int

	// BaseRetryDelay is the base for exponential backoff between retries.
	// Actual delay is BaseRetryDelay * 2^(attempt-1) with jitter. Longer
	// delays reduce pressure on a contended database but increase the
	// time before a transient failure resolves.
	// Default: 30s.
	BaseRetryDelay time.Duration

	// JobTimeout is the deadline applied to individual query execution
	// when context is available. Does not apply to transactions (see
	// TransactionTimeout).
	// Default: 30s.
	JobTimeout time.Duration

	// TransactionTimeout is the deadline applied to the entire transaction
	// lifecycle (begin through commit) when context is available. Should
	// be at least as long as JobTimeout since transactions typically
	// contain multiple operations.
	// Default: 30s.
	TransactionTimeout time.Duration

	// RetrySubmitTimeout caps how long the retry handler waits to
	// resubmit a job to the worker queue. Prevents the retry goroutine
	// from blocking indefinitely when the queue is full.
	// Default: 5s.
	RetrySubmitTimeout time.Duration

	// QueueSubmitTimeout caps how long context-free submissions
	// (SubmitWaitNoContext, SubmitNoWaitNoContext) wait for queue space.
	// Prevents deadlock when the queue is saturated and no context
	// cancellation is available.
	// Default: 5m.
	QueueSubmitTimeout time.Duration

	// UsePreparedStatements makes all queries use cached prepared
	// statements by default, avoiding repeated SQL parsing. Individual
	// queries can still override via Prepared(). Most beneficial when
	// the same queries execute repeatedly with different parameters.
	// Default: false.
	UsePreparedStatements bool

	// ErrorLogPath is the file path for persistent error logging. When
	// set, errors evicted from the in-memory queue are written to a
	// SQLite database at this path for post-mortem inspection. Leave
	// empty to keep error logging in-memory only.
	// Default: "" (disabled).
	ErrorLogPath string
}

// Validate validates and sets defaults for all options.
func (o *Options) Validate() error {
	o.SetDefaults()

	if o.WorkerQueueDepth < 0 {
		o.WorkerQueueDepth = 1000
	}

	if o.BatchSize < 0 {
		o.BatchSize = 200
	}

	if o.ErrorQueueMaxSize < 0 {
		o.ErrorQueueMaxSize = 1000
	}

	if o.MaxRetries < 0 {
		o.MaxRetries = 3
	}

	return nil
}

// SetDefaults applies default values for any option left at its zero value.
func (o *Options) SetDefaults() {
	if o.WorkerQueueDepth <= 0 {
		o.WorkerQueueDepth = 50000
	}

	if !o.EnableReader && !o.EnableWriter {
		o.EnableReader = true
		o.EnableWriter = true
	}

	if o.BatchSize <= 0 {
		o.BatchSize = 200
	}
	if o.BatchTimeout <= 0 {
		o.BatchTimeout = 1 * time.Second
	}

	if o.ErrorQueueMaxSize <= 0 {
		o.ErrorQueueMaxSize = o.WorkerQueueDepth
	}

	if o.MaxRetries <= 0 {
		o.MaxRetries = 3
	}
	if o.BaseRetryDelay <= 0 {
		o.BaseRetryDelay = 30 * time.Second
	}

	if o.JobTimeout <= 0 {
		o.JobTimeout = 30 * time.Second
	}
	if o.TransactionTimeout <= 0 {
		o.TransactionTimeout = 30 * time.Second
	}
	if o.RetrySubmitTimeout <= 0 {
		o.RetrySubmitTimeout = 5 * time.Second
	}
	if o.QueueSubmitTimeout <= 0 {
		o.QueueSubmitTimeout = 5 * time.Minute
	}

	// Boolean defaults (InlineInserts, UseContexts, etc.) cannot be
	// distinguished from explicitly-set false, so they are handled by
	// DefaultOptions at construction time rather than here.
}

// DefaultOptions provides sensible defaults suitable for most applications.
// WorkerQueueDepth is set conservatively at 1000 (not 50000) to avoid
// excessive memory use by default - raise it for high-throughput workloads.
var DefaultOptions = Options{
	WorkerQueueDepth:   1000,
	EnableReader:       true,
	EnableWriter:       true,
	BatchSize:          200,
	BatchTimeout:       1 * time.Second,
	InlineInserts:      false,
	UseContexts:        false,
	StmtCacheMaxSize:   1000,
	ErrorQueueMaxSize:  1000,
	EnableAutoRetry:    false,
	MaxRetries:         3,
	BaseRetryDelay:     30 * time.Second,
	JobTimeout:         30 * time.Second,
	TransactionTimeout: 30 * time.Second,
	RetrySubmitTimeout: 5 * time.Second,
	QueueSubmitTimeout: 5 * time.Minute,
}
