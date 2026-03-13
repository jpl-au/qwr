package qwr

import "time"

// TransactionFuncResult holds the outcome of a TransactionFunc execution.
type TransactionFuncResult struct {
	// Value is the result returned by the callback on success.
	Value any

	err      error
	id       int64
	duration time.Duration
}

// ID returns the ID of the transaction that produced this result.
func (r *TransactionFuncResult) ID() int64 {
	return r.id
}

// Error returns any error from the callback or transaction lifecycle.
func (r *TransactionFuncResult) Error() error {
	return r.err
}

// Duration returns how long the transaction took to execute.
func (r *TransactionFuncResult) Duration() time.Duration {
	return r.duration
}
