package qwr

import (
	"errors"
	"fmt"
	"testing"
)

// testSQLiteError implements sqliteErrorCode for testing without importing a driver.
type testSQLiteError struct {
	code int
	msg  string
}

func (e *testSQLiteError) Error() string { return e.msg }
func (e *testSQLiteError) Code() int     { return e.code }

func TestClassifyError_SQLiteCode(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		category ErrorCategory
		retry    RetryStrategy
	}{
		{"BUSY", 5, ErrorCategoryLock, RetryStrategyExponential},
		{"LOCKED", 6, ErrorCategoryLock, RetryStrategyExponential},
		{"IOERR", 10, ErrorCategoryConnection, RetryStrategyLinear},
		{"CANTOPEN", 14, ErrorCategoryConnection, RetryStrategyLinear},
		{"CONSTRAINT", 19, ErrorCategoryConstraint, RetryStrategyNone},
		{"READONLY", 8, ErrorCategoryPermission, RetryStrategyNone},
		{"PERM", 3, ErrorCategoryPermission, RetryStrategyNone},
		{"AUTH", 23, ErrorCategoryPermission, RetryStrategyNone},
		{"FULL", 13, ErrorCategoryResource, RetryStrategyLinear},
		{"NOMEM", 7, ErrorCategoryResource, RetryStrategyLinear},
		{"INTERRUPT", 9, ErrorCategoryTimeout, RetryStrategyLinear},
		{"CORRUPT", 11, ErrorCategorySchema, RetryStrategyNone},
		{"NOTADB", 26, ErrorCategorySchema, RetryStrategyNone},
		{"ERROR", 1, ErrorCategoryUnknown, RetryStrategyNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &testSQLiteError{code: tt.code, msg: "test"}
			result := ClassifyError(err, "test")
			if result.Category != tt.category {
				t.Errorf("code %d: got category %s, want %s", tt.code, result.Category, tt.category)
			}
			if result.Strategy != tt.retry {
				t.Errorf("code %d: got strategy %s, want %s", tt.code, result.Strategy, tt.retry)
			}
		})
	}
}

func TestClassifyError_ExtendedCodes(t *testing.T) {
	// Extended codes encode the primary code in the lower byte.
	// SQLITE_CONSTRAINT_UNIQUE = 2067 (19 | 8<<8)
	// SQLITE_BUSY_RECOVERY = 261 (5 | 1<<8)
	tests := []struct {
		name     string
		code     int
		category ErrorCategory
	}{
		{"CONSTRAINT_UNIQUE", 2067, ErrorCategoryConstraint},
		{"CONSTRAINT_FOREIGNKEY", 787, ErrorCategoryConstraint},
		{"CONSTRAINT_NOTNULL", 1299, ErrorCategoryConstraint},
		{"BUSY_RECOVERY", 261, ErrorCategoryLock},
		{"BUSY_SNAPSHOT", 517, ErrorCategoryLock},
		{"IOERR_READ", 266, ErrorCategoryConnection},
		{"READONLY_CANTINIT", 1288, ErrorCategoryPermission},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &testSQLiteError{code: tt.code, msg: "test"}
			result := ClassifyError(err, "test")
			if result.Category != tt.category {
				t.Errorf("extended code %d: got category %s, want %s", tt.code, result.Category, tt.category)
			}
		})
	}
}

func TestClassifyError_WrappedSQLiteError(t *testing.T) {
	// errors.As traverses the wrap chain
	inner := &testSQLiteError{code: 5, msg: "database is locked"}
	wrapped := fmt.Errorf("exec failed: %w", inner)

	result := ClassifyError(wrapped, "test")
	if result.Category != ErrorCategoryLock {
		t.Errorf("wrapped BUSY: got category %s, want lock", result.Category)
	}
}

func TestClassifyError_StringFallback(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		category ErrorCategory
		retry    RetryStrategy
	}{
		{"locked", "database is locked", ErrorCategoryLock, RetryStrategyExponential},
		{"context timeout", "context deadline exceeded", ErrorCategoryTimeout, RetryStrategyLinear},
		{"io error", "i/o error occurred", ErrorCategoryConnection, RetryStrategyLinear},
		{"disk full", "disk full", ErrorCategoryResource, RetryStrategyLinear},
		{"constraint", "UNIQUE constraint failed", ErrorCategoryConstraint, RetryStrategyNone},
		{"no such table", "no such table: users", ErrorCategorySchema, RetryStrategyNone},
		{"permission", "permission denied", ErrorCategoryPermission, RetryStrategyNone},
		{"unknown", "something unexpected", ErrorCategoryUnknown, RetryStrategyNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			result := ClassifyError(err, "test")
			if result.Category != tt.category {
				t.Errorf("%q: got category %s, want %s", tt.msg, result.Category, tt.category)
			}
			if result.Strategy != tt.retry {
				t.Errorf("%q: got strategy %s, want %s", tt.msg, result.Strategy, tt.retry)
			}
		})
	}
}

func TestClassifyError_InternalErrors(t *testing.T) {
	// Internal QWR errors should be classified before attempting SQLite code extraction
	for _, err := range []error{ErrReaderDisabled, ErrWriterDisabled, ErrWorkerNotRunning} {
		result := ClassifyError(err, "test")
		if result.Category != ErrorCategoryInternal {
			t.Errorf("%v: got category %s, want internal", err, result.Category)
		}
	}
}

func TestClassifyError_Nil(t *testing.T) {
	result := ClassifyError(nil, "test")
	if result.Category != ErrorCategoryInternal {
		t.Errorf("nil: got category %s, want internal", result.Category)
	}
}

func TestClassifyError_SQLiteCodeContext(t *testing.T) {
	// Verify the sqlite_code is stored in context for code-based classification
	err := &testSQLiteError{code: 2067, msg: "UNIQUE constraint failed"}
	result := ClassifyError(err, "test")
	code, ok := result.Context["sqlite_code"]
	if !ok {
		t.Fatal("expected sqlite_code in context")
	}
	if code != 2067 {
		t.Errorf("got sqlite_code %v, want 2067", code)
	}
}
