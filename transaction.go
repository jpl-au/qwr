package qwr

import (
	"context"
	"database/sql"
	"time"
)

// Transaction represents multiple SQL statements to execute in a transaction
type Transaction struct {
	queries []Query
	id      int64
	ctx     context.Context
	manager *Manager
}

// Transaction creates a new transaction
func (m *Manager) Transaction(capacity ...int) *Transaction {
	cap := 2 // default capacity
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}

	return &Transaction{
		queries: make([]Query, 0, cap),
		id:      nextJobID(),
		ctx:     nil,
		manager: m,
	}
}

// WithContext adds context to the transaction
func (t *Transaction) WithContext(ctx context.Context) *Transaction {
	t.ctx = ctx
	return t
}

// Add adds a query to the transaction
func (t *Transaction) Add(sql string, args ...any) *Transaction {
	query := Query{
		SQL:      sql,
		Args:     append(make([]any, 0, len(args)), args...),
		id:       nextJobID(),
		prepared: false,
	}
	t.queries = append(t.queries, query)
	return t
}

// AddPrepared adds a prepared query to the transaction
func (t *Transaction) AddPrepared(sql string, args ...any) *Transaction {
	query := Query{
		SQL:      sql,
		Args:     append(make([]any, 0, len(args)), args...),
		id:       nextJobID(),
		prepared: true,
	}
	t.queries = append(t.queries, query)
	return t
}

// Write executes the transaction directly
func (t *Transaction) Write() (*TransactionResult, error) {
	if t.manager.writer == nil {
		return nil, ErrWriterDisabled
	}

	start := time.Now()
	result := t.ExecuteWithContext(t.ctx, t.manager.writer)

	if result.Type != ResultTypeTransaction {
		return nil, ErrInvalidResult
	}

	execTime := time.Since(start)
	failed := result.TransactionResult.err != nil

	if failed {
		t.manager.events.Emit(Event{
			Type:     EventDirectWriteFailed,
			JobID:    t.id,
			ExecTime: execTime,
			Err:      result.TransactionResult.err,
		})
	} else {
		t.manager.events.Emit(Event{
			Type:     EventDirectWriteCompleted,
			JobID:    t.id,
			ExecTime: execTime,
		})
	}

	return &result.TransactionResult, result.TransactionResult.err
}

// Exec runs the transaction through the worker pool
func (t *Transaction) Exec() (*TransactionResult, error) {
	ctx := t.ctx
	if ctx == nil {
		if t.manager.ctx != nil {
			ctx = t.manager.ctx
		} else {
			ctx = nil
		}
	}

	var result JobResult
	var err error

	if ctx != nil {
		result, err = t.manager.serialiser.SubmitWait(ctx, NewTransactionJob(*t))
	} else {
		result, err = t.manager.serialiser.SubmitWaitNoContext(NewTransactionJob(*t))
	}

	if err != nil {
		return nil, err
	}

	if result.Type != ResultTypeTransaction {
		return nil, ErrInvalidResult
	}
	return &result.TransactionResult, result.TransactionResult.err
}

// ID returns the transaction ID
func (t *Transaction) ID() int64 {
	return t.id
}

// ExecuteWithContext runs all queries within a single transaction.
// Used by the write serialiser to dispatch Transaction jobs.
func (t *Transaction) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	start := time.Now()
	result := &TransactionResult{
		id:      t.id,
		Results: make([]*QueryResult, len(t.queries)),
	}

	if len(t.queries) == 0 {
		result.err = ErrInvalidQuery
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	// Apply transaction timeout when context is available.
	if ctx != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.manager.options.TransactionTimeout)
		defer cancel()
	}

	preparedStmts := t.fetchPreparedStmts(db)

	tx, err := t.beginTx(ctx, db)
	if err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	for i, query := range t.queries {
		qStart := time.Now()
		qr := &QueryResult{id: query.ID()}

		sqlResult, err := t.execQuery(ctx, tx, query, preparedStmts)
		if err != nil {
			tx.Rollback()
			qr.err = err
			result.Results[i] = qr
			result.err = err
			result.duration = time.Since(start)
			return NewTransactionResult(*result)
		}

		qr.SQLResult = sqlResult
		qr.duration = time.Since(qStart)
		result.Results[i] = qr
	}

	if err := tx.Commit(); err != nil {
		result.err = err
	}

	result.duration = time.Since(start)
	return NewTransactionResult(*result)
}

// beginTx starts a transaction. Uses BeginTx when context is available,
// plain Begin otherwise.
func (t *Transaction) beginTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if ctx == nil {
		return db.Begin()
	}
	return db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
}

// fetchPreparedStmts pre-fetches cached prepared statements for all
// queries that request them. Called before beginning the transaction
// to avoid cache lookups inside the critical section.
func (t *Transaction) fetchPreparedStmts(db *sql.DB) map[string]*sql.Stmt {
	stmts := make(map[string]*sql.Stmt)
	for _, query := range t.queries {
		if query.prepared && t.manager.writeStmtCache != nil {
			if _, exists := stmts[query.SQL]; !exists {
				if stmt, err := t.manager.writeStmtCache.Get(db, query.SQL); err == nil {
					stmts[query.SQL] = stmt
				}
			}
		}
	}
	return stmts
}

// execQuery runs a single query within a transaction, using context-aware
// methods when ctx is non-nil.
func (t *Transaction) execQuery(ctx context.Context, tx *sql.Tx, query Query, stmts map[string]*sql.Stmt) (sql.Result, error) {
	if query.prepared {
		if stmt, exists := stmts[query.SQL]; exists {
			if ctx != nil {
				txStmt := tx.StmtContext(ctx, stmt)
				defer txStmt.Close()
				return txStmt.ExecContext(ctx, query.Args...)
			}
			txStmt := tx.Stmt(stmt)
			defer txStmt.Close()
			return txStmt.Exec(query.Args...)
		}
	}
	if ctx != nil {
		return tx.ExecContext(ctx, query.SQL, query.Args...)
	}
	return tx.Exec(query.SQL, query.Args...)
}
