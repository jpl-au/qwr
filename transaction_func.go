// transaction_func.go provides callback-based transactions for interleaved
// read-write operations within the serialised writer.
//
// Unlike Transaction (which pre-collects statements and runs them as Exec),
// TransactionFunc gives the caller a *sql.Tx and lets them interleave reads
// and writes freely. qwr manages BeginTx/Commit/Rollback.

package qwr

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TransactionFunc executes a caller-provided function within a serialised
// transaction. The callback receives a *sql.Tx for full read-write access.
// qwr handles transaction lifecycle (begin, commit on success, rollback on
// error or panic).
type TransactionFunc struct {
	fn      func(*sql.Tx) (any, error)
	id      int64
	ctx     context.Context
	manager *Manager
}

// TransactionFunc creates a new callback-based transaction. The function
// receives a *sql.Tx and may perform any combination of reads and writes.
// Return a non-nil error to trigger rollback; return nil to commit.
func (m *Manager) TransactionFunc(fn func(*sql.Tx) (any, error)) *TransactionFunc {
	return &TransactionFunc{
		fn:      fn,
		id:      nextJobID(),
		manager: m,
	}
}

// WithContext adds context to the transaction. When set, qwr uses BeginTx
// with a timeout derived from Options.TransactionTimeout.
func (tf *TransactionFunc) WithContext(ctx context.Context) *TransactionFunc {
	tf.ctx = ctx
	return tf
}

// ID returns the unique identifier for this transaction.
func (tf *TransactionFunc) ID() int64 {
	return tf.id
}

// Exec runs the transaction through the serialised writer queue. Blocks
// until the callback completes and the transaction is committed or rolled back.
func (tf *TransactionFunc) Exec() (*TransactionFuncResult, error) {
	ctx := tf.ctx
	if ctx == nil && tf.manager.ctx != nil {
		ctx = tf.manager.ctx
	}

	var result JobResult
	var err error

	if ctx != nil {
		result, err = tf.manager.serialiser.SubmitWait(ctx, NewTransactionFuncJob(*tf))
	} else {
		result, err = tf.manager.serialiser.SubmitWaitNoContext(NewTransactionFuncJob(*tf))
	}

	if err != nil {
		return nil, err
	}

	if result.Type != ResultTypeTransactionFunc {
		return nil, ErrInvalidResult
	}
	return &result.TransactionFuncResult, result.TransactionFuncResult.err
}

// Write executes the transaction directly on the writer connection,
// bypassing the queue. The callback still runs within a real transaction.
func (tf *TransactionFunc) Write() (*TransactionFuncResult, error) {
	if tf.manager.writer == nil {
		return nil, ErrWriterDisabled
	}

	start := time.Now()
	result := tf.ExecuteWithContext(tf.ctx, tf.manager.writer)

	if result.Type != ResultTypeTransactionFunc {
		return nil, ErrInvalidResult
	}

	execTime := time.Since(start)
	failed := result.TransactionFuncResult.err != nil

	if failed {
		tf.manager.events.Emit(Event{
			Type:     EventDirectWriteFailed,
			JobID:    tf.id,
			ExecTime: execTime,
			Err:      result.TransactionFuncResult.err,
		})
	} else {
		tf.manager.events.Emit(Event{
			Type:     EventDirectWriteCompleted,
			JobID:    tf.id,
			ExecTime: execTime,
		})
	}

	return &result.TransactionFuncResult, result.TransactionFuncResult.err
}

// ExecuteWithContext runs the callback within a transaction on the given db.
// Used by the write serialiser to dispatch TransactionFunc jobs.
func (tf *TransactionFunc) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	start := time.Now()
	result := &TransactionFuncResult{id: tf.id}

	// Apply transaction timeout when context is available.
	if ctx != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tf.manager.options.TransactionTimeout)
		defer cancel()
	}

	tx, err := tf.beginTx(ctx, db)
	if err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionFuncResult(*result)
	}

	val, err := tf.callFn(tx)
	if err != nil {
		tx.Rollback()
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionFuncResult(*result)
	}

	if err := tx.Commit(); err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionFuncResult(*result)
	}

	result.Value = val
	result.duration = time.Since(start)
	return NewTransactionFuncResult(*result)
}

// beginTx starts a transaction. Uses BeginTx when context is available,
// plain Begin otherwise.
func (tf *TransactionFunc) beginTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if ctx == nil {
		return db.Begin()
	}
	return db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
}

// callFn invokes the user callback, converting any panic into an error.
// This protects the write serialiser worker from being killed by a
// misbehaving callback.
func (tf *TransactionFunc) callFn(tx *sql.Tx) (val any, err error) {
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			err = fmt.Errorf("transaction callback panicked: %v", p)
		}
	}()
	return tf.fn(tx)
}
