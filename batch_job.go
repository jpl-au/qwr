package qwr

import (
	"context"
	"database/sql"
	"time"
)

// BatchJob represents a collection of database jobs to be executed as a batch
type BatchJob struct {
	Queries []Job
	id      int64
}

// ExecuteWithContext runs each job in the batch within a single transaction
func (b BatchJob) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	start := time.Now()
	result := &BatchResult{
		id: b.id,
	}

	// Validate that all jobs are Query type
	for _, job := range b.Queries {
		if job.Type != JobTypeQuery {
			result.err = ErrBatchContainsNonQuery
			result.duration = time.Since(start)
			return NewBatchResult(*result)
		}
	}

	// Handle nil context
	if ctx == nil {
		ctx = context.Background()
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewBatchResult(*result)
	}

	// Prepare for rollback in case of panic
	defer func() {
		if p := recover(); p != nil {
			if tx != nil {
				_ = tx.Rollback()
			}
			panic(p)
		}
	}()

	results := make([]JobResult, len(b.Queries))

	// Execute each job in the batch
	for i, job := range b.Queries {
		var jobResult JobResult

		// Handle Query jobs specially for transaction context
		switch job.Type {
		case JobTypeQuery:
			qStart := time.Now()
			qr := &QueryResult{id: job.Query.id}

			sqlResult, err := tx.ExecContext(ctx, job.Query.SQL, job.Query.Args...)
			if err != nil {
				_ = tx.Rollback()
				qr.err = err
				results[i] = NewQueryResult(*qr)
				result.err = err
				result.Results = results
				result.duration = time.Since(start)
				return NewBatchResult(*result)
			}

			qr.SQLResult = sqlResult
			qr.duration = time.Since(qStart)
			jobResult = NewQueryResult(*qr)

		default:
			// All other jobs use ExecuteWithContext
			// Note: This passes db instead of tx which may not be correct for Transaction jobs
			jobResult = job.ExecuteWithContext(ctx, db)
		}

		if jobResult.Error() != nil {
			_ = tx.Rollback()
			result.err = jobResult.Error()
			results[i] = jobResult
			result.Results = results
			result.duration = time.Since(start)
			return NewBatchResult(*result)
		}

		results[i] = jobResult
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		result.err = err
	}
	result.Results = results
	result.duration = time.Since(start)
	return NewBatchResult(*result)
}

// ID returns the unique identifier for this batch
func (b BatchJob) ID() int64 {
	return b.id
}
