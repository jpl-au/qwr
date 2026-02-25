package qwr

import (
	"context"
	"database/sql"
	"time"
)

// BatchJob represents a collection of database jobs to be executed as a batch
type BatchJob struct {
	Queries   []Job
	id        int64
	stmtCache *StmtCache // optional cache for prepared statement reuse within the batch
}

// ExecuteWithContext runs each job in the batch within a single transaction.
// If a statement cache is available, prepared queries reuse cached statements
// via tx.Stmt() to avoid re-parsing SQL on every execution.
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

	// Pre-fetch prepared statements from cache before starting transaction
	var preparedStmts map[string]*sql.Stmt
	if b.stmtCache != nil {
		preparedStmts = make(map[string]*sql.Stmt)
		for _, job := range b.Queries {
			if job.Query.prepared {
				if _, exists := preparedStmts[job.Query.SQL]; !exists {
					if stmt, err := b.stmtCache.Get(db, job.Query.SQL); err == nil {
						preparedStmts[job.Query.SQL] = stmt
					}
				}
			}
		}
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

	// Execute each query in the batch
	for i, job := range b.Queries {
		qStart := time.Now()
		qr := &QueryResult{id: job.Query.id}

		var sqlResult sql.Result

		if job.Query.prepared {
			if stmt, exists := preparedStmts[job.Query.SQL]; exists {
				txStmt := tx.StmtContext(ctx, stmt)
				sqlResult, err = txStmt.ExecContext(ctx, job.Query.Args...)
				txStmt.Close()
			} else {
				sqlResult, err = tx.ExecContext(ctx, job.Query.SQL, job.Query.Args...)
			}
		} else {
			sqlResult, err = tx.ExecContext(ctx, job.Query.SQL, job.Query.Args...)
		}

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
		results[i] = NewQueryResult(*qr)
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
