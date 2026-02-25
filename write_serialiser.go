package qwr

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"
)

// WriteSerialiser manages a single worker that processes database jobs
type WriteSerialiser struct {
	db            *sql.DB
	queue         chan workItem
	workerRunning atomic.Bool
	workerWg      sync.WaitGroup
	events        *EventBus
	writeCache    *StmtCache
	mu            sync.RWMutex
	shutdownOnce  sync.Once

	// Store timeout values directly
	jobTimeout         time.Duration
	transactionTimeout time.Duration
	queueSubmitTimeout time.Duration
}

type workItem struct {
	job        Job
	ctx        context.Context
	queuedAt   time.Time
	resultChan chan JobResult
}

// NewWorkerPool creates a new worker pool for database jobs
func NewWorkerPool(db *sql.DB, queueDepth int, events *EventBus, writeCache *StmtCache, options Options) *WriteSerialiser {
	wp := &WriteSerialiser{
		db:                 db,
		queue:              make(chan workItem, queueDepth),
		events:             events,
		writeCache:         writeCache,
		jobTimeout:         options.JobTimeout,
		transactionTimeout: options.TransactionTimeout,
		queueSubmitTimeout: options.QueueSubmitTimeout,
	}

	wp.workerRunning.Store(true)
	return wp
}

// QueueLen returns the current number of items in the work queue.
func (wp *WriteSerialiser) QueueLen() int {
	return len(wp.queue)
}

// Start begins the worker processing loop
func (wp *WriteSerialiser) Start(ctx context.Context) {
	wp.workerWg.Go(func() {
		wp.worker(ctx)
	})
	wp.events.Emit(Event{Type: EventWorkerStarted})
}

// Stop shuts down the worker pool
func (wp *WriteSerialiser) Stop() error {
	var err error

	wp.shutdownOnce.Do(func() {
		// Signal shutdown to prevent new submissions
		wp.workerRunning.Store(false)

		// Close the queue to signal worker to stop after processing remaining items
		wp.mu.Lock()
		close(wp.queue)
		wp.mu.Unlock()

		// Wait for worker to finish processing all remaining items and exit
		wp.workerWg.Wait()

		wp.events.Emit(Event{Type: EventWorkerStopped})
	})

	return err
}

// SubmitWait submits a job to the queue and waits for its result
func (wp *WriteSerialiser) SubmitWait(ctx context.Context, job Job) (JobResult, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return JobResult{}, ErrWorkerNotRunning
	}

	resultChan := make(chan JobResult, 1)
	item := workItem{
		job:        job,
		ctx:        ctx,
		queuedAt:   time.Now(),
		resultChan: resultChan,
	}

	select {
	case wp.queue <- item:
		wp.events.Emit(Event{Type: EventJobQueued, JobID: job.ID(), JobType: job.Type})
	case <-ctx.Done():
		// Do not close resultChan here as the worker might be about to write to it
		return JobResult{}, ctx.Err()
	}

	select {
	case result := <-resultChan:
		return result, result.Error()
	case <-ctx.Done():
		return JobResult{}, ctx.Err()
	}
}

// SubmitNoWait submits a job to the queue without waiting
func (wp *WriteSerialiser) SubmitNoWait(ctx context.Context, job Job) (int64, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return 0, ErrWorkerNotRunning
	}

	item := workItem{
		job:        job,
		ctx:        ctx,
		queuedAt:   time.Now(),
		resultChan: nil,
	}

	select {
	case wp.queue <- item:
		wp.events.Emit(Event{Type: EventJobQueued, JobID: job.ID(), JobType: job.Type})
		return job.ID(), nil
	case <-ctx.Done():
		return job.ID(), ctx.Err()
	}
}

// SubmitWaitNoContext submits a job without using any context
func (wp *WriteSerialiser) SubmitWaitNoContext(job Job) (JobResult, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return JobResult{}, ErrWorkerNotRunning
	}

	resultChan := make(chan JobResult, 1)
	item := workItem{
		job:        job,
		ctx:        nil,
		queuedAt:   time.Now(),
		resultChan: resultChan,
	}

	// Submit to queue with timeout to prevent deadlock when queue is full
	select {
	case wp.queue <- item:
		wp.events.Emit(Event{Type: EventJobQueued, JobID: job.ID(), JobType: job.Type})
	case <-time.After(wp.queueSubmitTimeout):
		// Do not close resultChan to avoid race
		return JobResult{}, ErrQueueTimeout
	}

	result := <-resultChan
	return result, result.Error()
}

// SubmitNoWaitNoContext submits a job without using any context
func (wp *WriteSerialiser) SubmitNoWaitNoContext(job Job) (int64, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return 0, ErrWorkerNotRunning
	}

	item := workItem{
		job:        job,
		ctx:        nil,
		queuedAt:   time.Now(),
		resultChan: nil,
	}

	// Submit to queue with timeout to prevent deadlock when queue is full
	select {
	case wp.queue <- item:
		wp.events.Emit(Event{Type: EventJobQueued, JobID: job.ID(), JobType: job.Type})
		return job.ID(), nil
	case <-time.After(wp.queueSubmitTimeout):
		return 0, ErrQueueTimeout
	}
}

func (wp *WriteSerialiser) worker(ctx context.Context) {
	for {
		select {
		case item, ok := <-wp.queue:
			if !ok {
				// Channel closed - exit worker
				return
			}
			wp.processJob(item)

		case <-ctx.Done():
			return
		}
	}
}

// processJob handles the execution of a single job
func (wp *WriteSerialiser) processJob(item workItem) {
	queueWait := time.Since(item.queuedAt)
	start := time.Now()

	wp.events.Emit(Event{
		Type:      EventJobStarted,
		JobID:     item.job.ID(),
		JobType:   item.job.Type,
		QueueWait: queueWait,
	})

	var result JobResult

	// Execute jobs directly without wrapper methods
	switch item.job.Type {
	case JobTypeQuery:
		result = wp.executeQuery(item.ctx, item.job.Query)

	case JobTypeBatch:
		result = item.job.BatchJob.ExecuteWithContext(item.ctx, wp.db)

	case JobTypeTransaction:
		result = item.job.Transaction.ExecuteWithContext(item.ctx, wp.db)

	default:
		result = NewQueryResult(QueryResult{
			id:  item.job.ID(),
			err: ErrInvalidQuery,
		})
	}

	execTime := time.Since(start)

	// Only query jobs have a single SQL statement and a retry counter.
	// Batch and transaction jobs contain multiple statements, so these
	// fields are left at zero for them.
	var jobSQL string
	var attempt int
	if item.job.Type == JobTypeQuery {
		jobSQL = item.job.Query.SQL
		attempt = item.job.Query.retries
	}

	if result.Error() != nil {
		wp.events.Emit(Event{
			Type:      EventJobFailed,
			JobID:     item.job.ID(),
			JobType:   item.job.Type,
			SQL:       jobSQL,
			QueueWait: queueWait,
			ExecTime:  execTime,
			Err:       result.Error(),
			Attempt:   attempt,
		})
	} else {
		wp.events.Emit(Event{
			Type:      EventJobCompleted,
			JobID:     item.job.ID(),
			JobType:   item.job.Type,
			SQL:       jobSQL,
			QueueWait: queueWait,
			ExecTime:  execTime,
			Attempt:   attempt,
		})
	}

	// Deliver result if someone is waiting
	if item.resultChan != nil {
		item.resultChan <- result
		close(item.resultChan)
	}
}

// executeQuery handles Query execution with cache access
func (wp *WriteSerialiser) executeQuery(ctx context.Context, query Query) JobResult {
	start := time.Now()
	result := &QueryResult{
		id: query.id,
	}

	var sqlResult sql.Result
	var err error
	var stmt *sql.Stmt

	// Handle prepared statement execution with cache
	if query.prepared && wp.writeCache != nil {
		stmt, err = wp.writeCache.Get(wp.db, query.SQL)
		if err != nil {
			result.err = err
			result.duration = time.Since(start)
			return NewQueryResult(*result)
		}

		if ctx != nil {
			jobCtx, cancel := context.WithTimeout(ctx, wp.jobTimeout)
			defer cancel()
			sqlResult, err = stmt.ExecContext(jobCtx, query.Args...)
		} else {
			sqlResult, err = stmt.Exec(query.Args...)
		}
	} else {
		// Direct execution
		if ctx != nil {
			jobCtx, cancel := context.WithTimeout(ctx, wp.jobTimeout)
			defer cancel()
			sqlResult, err = wp.db.ExecContext(jobCtx, query.SQL, query.Args...)
		} else {
			sqlResult, err = wp.db.Exec(query.SQL, query.Args...)
		}
	}

	result.SQLResult = sqlResult
	result.err = err
	result.duration = time.Since(start)
	return NewQueryResult(*result)
}
