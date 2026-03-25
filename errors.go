package qwr

import (
	"errors"
	"strings"
	"time"
)

// Error constants
var (
	ErrManagerClosed             = errors.New("manager is closed")
	ErrReaderDisabled            = errors.New("reader is disabled")
	ErrWriterDisabled            = errors.New("writer is disabled")
	ErrResultNotFound            = errors.New("result not found")
	ErrInvalidResult             = errors.New("invalid result type")
	ErrQueryTooLarge             = errors.New("query exceeds maximum allowed size")
	ErrStatementCacheFull        = errors.New("statement cache is full")
	ErrHashCollision             = errors.New("statement hash collision")
	ErrErrorQueueDisabled        = errors.New("error queue is disabled")
	ErrJobNotFound               = errors.New("job not found in error queue")
	ErrWorkerNotRunning          = errors.New("worker pool is not running")
	ErrQueueTimeout              = errors.New("timeout waiting for queue to accept submission")
	ErrRetrySubmissionFailed     = errors.New("failed to resubmit job for retry")
	ErrInvalidQuery              = errors.New("query is invalid")
	ErrNilPreparedStatement      = errors.New("internal error: prepared statement is nil before execution")
	ErrNilPreparedStatementCache = errors.New("internal error: global prepared statement cache returned nil statement without error")
	ErrFailedToPrepareStatement  = errors.New("failed to prepare statement")
	ErrPreparedCacheRequired     = errors.New("prepared statement cache is required when using prepared queries")
	ErrCacheClosed               = errors.New("statement cache is closed")
	ErrBatchContainsNonQuery     = errors.New("batch jobs can only contain Query jobs, not Transaction or nested Batch jobs")
	ErrConnectionUnhealthy       = errors.New("database connection is unhealthy")
	ErrBackupDestinationExists   = errors.New("backup destination already exists")
	ErrBackupDriverUnsupported   = errors.New("driver does not support backup API")
	ErrBackupFailed              = errors.New("backup failed")
	ErrBackupInit                = errors.New("failed to initialize backup")
	ErrBackupStep                = errors.New("backup step failed")
	ErrBackupConnection          = errors.New("failed to get connection for backup")
	ErrBackupInvalidMethod       = errors.New("unknown backup method")
	ErrQueueFull                 = errors.New("worker queue is full")
)

// ErrorCategory provides granular error classification for better handling
type ErrorCategory int

const (
	// ErrorCategoryConnection indicates connection-related errors
	ErrorCategoryConnection ErrorCategory = iota
	// ErrorCategoryLock indicates database locking/concurrency errors
	ErrorCategoryLock
	// ErrorCategoryConstraint indicates constraint violation errors
	ErrorCategoryConstraint
	// ErrorCategorySchema indicates schema-related errors
	ErrorCategorySchema
	// ErrorCategoryResource indicates resource exhaustion errors
	ErrorCategoryResource
	// ErrorCategoryTimeout indicates timeout/deadline errors
	ErrorCategoryTimeout
	// ErrorCategoryPermission indicates access control errors
	ErrorCategoryPermission
	// ErrorCategoryInternal indicates internal QWR errors
	ErrorCategoryInternal
	// ErrorCategoryUnknown indicates unclassified errors
	ErrorCategoryUnknown
)

// String returns the string representation of ErrorCategory
func (ec ErrorCategory) String() string {
	switch ec {
	case ErrorCategoryConnection:
		return "connection"
	case ErrorCategoryLock:
		return "lock"
	case ErrorCategoryConstraint:
		return "constraint"
	case ErrorCategorySchema:
		return "schema"
	case ErrorCategoryResource:
		return "resource"
	case ErrorCategoryTimeout:
		return "timeout"
	case ErrorCategoryPermission:
		return "permission"
	case ErrorCategoryInternal:
		return "internal"
	case ErrorCategoryUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// RetryStrategy defines how errors should be retried
type RetryStrategy int

const (
	// RetryStrategyNone indicates no retry should be attempted
	RetryStrategyNone RetryStrategy = iota
	// RetryStrategyImmediate indicates immediate retry with no delay
	RetryStrategyImmediate
	// RetryStrategyExponential indicates exponential backoff retry
	RetryStrategyExponential
	// RetryStrategyLinear indicates linear backoff retry
	RetryStrategyLinear
)

// String returns the string representation of RetryStrategy
func (rs RetryStrategy) String() string {
	switch rs {
	case RetryStrategyNone:
		return "none"
	case RetryStrategyImmediate:
		return "immediate"
	case RetryStrategyExponential:
		return "exponential"
	case RetryStrategyLinear:
		return "linear"
	default:
		return "unknown"
	}
}

// QWRError provides structured error information with enhanced classification
type QWRError struct {
	// Original error from the underlying operation
	Original error
	// Category of the error for granular handling
	Category ErrorCategory
	// RetryStrategy for this specific error
	Strategy RetryStrategy
	// Context provides additional information about the error
	Context map[string]any
	// Timestamp when the error occurred
	Timestamp time.Time
	// Operation that caused the error (query, transaction, etc.)
	Operation string
}

// Error implements the error interface
func (qe *QWRError) Error() string {
	if qe.Original != nil {
		return qe.Original.Error()
	}
	return "unknown QWR error"
}

// Unwrap allows error unwrapping for errors.Is and errors.As
func (qe *QWRError) Unwrap() error {
	return qe.Original
}

// IsRetriable returns true if the error should be retried
func (qe *QWRError) IsRetriable() bool {
	return qe.Strategy != RetryStrategyNone
}

// NewQWRError creates a new structured QWR error
func NewQWRError(original error, category ErrorCategory, strategy RetryStrategy, operation string) *QWRError {
	return &QWRError{
		Original:  original,
		Category:  category,
		Strategy:  strategy,
		Context:   make(map[string]any),
		Timestamp: time.Now(),
		Operation: operation,
	}
}

// WithContext adds context information to the error
func (qe *QWRError) WithContext(key string, value any) *QWRError {
	if qe.Context == nil {
		qe.Context = make(map[string]any)
	}
	qe.Context[key] = value
	return qe
}

// sqliteErrorCode is satisfied by SQLite driver error types that expose the
// underlying result code (e.g. modernc.org/sqlite.Error, mattn/go-sqlite3.Error).
// This keeps classification driver-agnostic - no driver imports needed.
type sqliteErrorCode interface {
	Code() int
}

// SQLite primary result codes used for error classification.
// These are stable across all SQLite versions and drivers.
const (
	sqliteError      = 1  // SQLITE_ERROR - generic error
	sqlitePerm       = 3  // SQLITE_PERM - access permission denied
	sqliteBusy       = 5  // SQLITE_BUSY - database file is locked
	sqliteLocked     = 6  // SQLITE_LOCKED - table in the database is locked
	sqliteNoMem      = 7  // SQLITE_NOMEM - malloc failed
	sqliteReadOnly   = 8  // SQLITE_READONLY - attempt to write a readonly database
	sqliteInterrupt  = 9  // SQLITE_INTERRUPT - operation terminated by sqlite3_interrupt
	sqliteIOErr      = 10 // SQLITE_IOERR - disk I/O error
	sqliteCorrupt    = 11 // SQLITE_CORRUPT - database disk image is malformed
	sqliteFull       = 13 // SQLITE_FULL - database is full
	sqliteCantOpen   = 14 // SQLITE_CANTOPEN - unable to open the database file
	sqliteConstraint = 19 // SQLITE_CONSTRAINT - constraint violation
	sqliteAuth       = 23 // SQLITE_AUTH - authorisation denied
	sqliteNotADB     = 26 // SQLITE_NOTADB - not a database file
)

// ClassifyError provides enhanced error classification with detailed categorisation.
// If the error implements the sqliteErrorCode interface (i.e. exposes a Code() int
// method), classification uses the structured result code. Otherwise, it falls back
// to string matching on the error message.
func ClassifyError(err error, operation string) *QWRError {
	if err == nil {
		return NewQWRError(nil, ErrorCategoryInternal, RetryStrategyNone, operation)
	}

	// Check for known internal QWR errors first - these are never from SQLite
	for _, qwrErr := range []error{
		ErrReaderDisabled, ErrWriterDisabled, ErrWorkerNotRunning,
		ErrErrorQueueDisabled,
	} {
		if errors.Is(err, qwrErr) {
			return NewQWRError(err, ErrorCategoryInternal, RetryStrategyNone, operation).
				WithContext("internal_error", true)
		}
	}

	// Try structured classification via SQLite error code
	var coded sqliteErrorCode
	if errors.As(err, &coded) {
		return classifySQLiteCode(coded.Code(), err, operation)
	}

	// Fallback to string matching for drivers that don't expose error codes
	return classifyByMessage(err, operation)
}

// classifySQLiteCode maps a SQLite result code to an error category and retry
// strategy. Extended error codes encode the primary code in the lower byte,
// so masking with 0xFF gives us the base category.
func classifySQLiteCode(code int, err error, operation string) *QWRError {
	primary := code & 0xFF

	switch primary {
	case sqliteBusy, sqliteLocked:
		return NewQWRError(err, ErrorCategoryLock, RetryStrategyExponential, operation).
			WithContext("sqlite_code", code)

	case sqliteIOErr, sqliteCantOpen:
		return NewQWRError(err, ErrorCategoryConnection, RetryStrategyLinear, operation).
			WithContext("sqlite_code", code)

	case sqliteConstraint:
		return NewQWRError(err, ErrorCategoryConstraint, RetryStrategyNone, operation).
			WithContext("sqlite_code", code)

	case sqliteReadOnly, sqlitePerm, sqliteAuth:
		return NewQWRError(err, ErrorCategoryPermission, RetryStrategyNone, operation).
			WithContext("sqlite_code", code)

	case sqliteFull, sqliteNoMem:
		return NewQWRError(err, ErrorCategoryResource, RetryStrategyLinear, operation).
			WithContext("sqlite_code", code)

	case sqliteInterrupt:
		return NewQWRError(err, ErrorCategoryTimeout, RetryStrategyLinear, operation).
			WithContext("sqlite_code", code)

	case sqliteCorrupt, sqliteNotADB:
		return NewQWRError(err, ErrorCategorySchema, RetryStrategyNone, operation).
			WithContext("sqlite_code", code)

	default:
		return NewQWRError(err, ErrorCategoryUnknown, RetryStrategyNone, operation).
			WithContext("sqlite_code", code)
	}
}

// classifyByMessage uses string matching as a fallback for drivers that don't
// expose structured error codes. This path is less precise but ensures
// reasonable classification regardless of the underlying driver.
func classifyByMessage(err error, operation string) *QWRError {
	errMsg := strings.ToLower(err.Error())

	// Database locking errors
	if strings.Contains(errMsg, "database is locked") ||
		strings.Contains(errMsg, "database locked") {
		return NewQWRError(err, ErrorCategoryLock, RetryStrategyExponential, operation)
	}

	// Context timeout errors
	if strings.Contains(errMsg, "context") &&
		(strings.Contains(errMsg, "deadline exceeded") || strings.Contains(errMsg, "timeout")) {
		return NewQWRError(err, ErrorCategoryTimeout, RetryStrategyLinear, operation)
	}

	// File I/O errors
	if strings.Contains(errMsg, "i/o error") ||
		strings.Contains(errMsg, "broken pipe") {
		return NewQWRError(err, ErrorCategoryConnection, RetryStrategyLinear, operation)
	}

	// Resource exhaustion errors
	if strings.Contains(errMsg, "disk full") ||
		strings.Contains(errMsg, "no space left") ||
		strings.Contains(errMsg, "out of memory") {
		return NewQWRError(err, ErrorCategoryResource, RetryStrategyLinear, operation)
	}

	// Constraint violation errors
	if strings.Contains(errMsg, "constraint") ||
		strings.Contains(errMsg, "unique") ||
		strings.Contains(errMsg, "foreign key") ||
		strings.Contains(errMsg, "not null") ||
		strings.Contains(errMsg, "primary key") {
		return NewQWRError(err, ErrorCategoryConstraint, RetryStrategyNone, operation)
	}

	// Schema-related errors
	if strings.Contains(errMsg, "no such table") ||
		strings.Contains(errMsg, "no such column") ||
		strings.Contains(errMsg, "syntax error") {
		return NewQWRError(err, ErrorCategorySchema, RetryStrategyNone, operation)
	}

	// Permission/access errors
	if strings.Contains(errMsg, "permission denied") ||
		strings.Contains(errMsg, "access denied") ||
		strings.Contains(errMsg, "readonly") ||
		strings.Contains(errMsg, "read-only") {
		return NewQWRError(err, ErrorCategoryPermission, RetryStrategyNone, operation)
	}

	return NewQWRError(err, ErrorCategoryUnknown, RetryStrategyNone, operation)
}
