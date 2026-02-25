package qwr

import (
	"context"
	"strings"
	"sync"
	"time"
)

// BatchCollector manages automatic batching of database jobs for asynchronous execution.
type BatchCollector struct {
	queries []Job            // Current batch of jobs awaiting execution
	timer   *time.Timer      // Timer for timeout-based batch flushing
	mutex   sync.Mutex       // Protects batch state during concurrent access
	ws      *WriteSerialiser // Worker pool for async job execution
	events  *EventBus        // Event bus for notifications
	options Options          // Configuration options for batch behavior
	dbPath  string           // Database path for logging context
	ctx     context.Context  // Context for all batch operations (never nil)
}

// NewBatchCollector creates a new batch collector with pre-allocated capacity and context.
func NewBatchCollector(ctx context.Context, ws *WriteSerialiser, events *EventBus, options Options, dbPath string) *BatchCollector {
	if ctx == nil {
		panic("BatchCollector requires a non-nil context")
	}

	return &BatchCollector{
		queries: make([]Job, 0, options.BatchSize),
		ws:      ws,
		events:  events,
		options: options,
		dbPath:  dbPath,
		ctx:     ctx,
	}
}

// Add adds a job to the current batch for eventual execution using the collector's context.
// Returns ErrQueueFull if a size-triggered flush cannot submit to the worker queue.
func (bc *BatchCollector) Add(job Job) error {
	// Mark query as async if it's a Query type
	if job.Type == JobTypeQuery {
		job.Query.async = true
	}

	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	// Start timer if this is the first item
	if len(bc.queries) == 0 {
		bc.startBatchTimer()
	}

	// Add to batch
	bc.queries = append(bc.queries, job)

	bc.events.Emit(Event{Type: EventBatchQueryAdded, BatchSize: len(bc.queries)})

	// Check if we need to flush due to size
	if len(bc.queries) >= bc.options.BatchSize {
		return bc.flushBatch("size_limit")
	}
	return nil
}

// startBatchTimer starts the timeout timer for batch flushing
func (bc *BatchCollector) startBatchTimer() {
	bc.timer = time.AfterFunc(bc.options.BatchTimeout, func() {
		bc.mutex.Lock()
		defer bc.mutex.Unlock()

		if len(bc.queries) > 0 {
			bc.flushBatch("timeout") // error emitted via EventBatchSubmitFailed
		}
		bc.timer = nil
	})
}

// flushBatch processes and submits the current batch (must be called with mutex held).
// Returns ErrQueueFull if the worker queue cannot accept the batch.
func (bc *BatchCollector) flushBatch(reason string) error {
	if len(bc.queries) == 0 {
		return nil
	}

	batchSize := len(bc.queries)
	batchJob := BatchJob{
		Queries: make([]Job, len(bc.queries)),
		id:      nextJobID(),
	}
	copy(batchJob.Queries, bc.queries)

	bc.events.Emit(Event{
		Type:        EventBatchFlushed,
		BatchID:     batchJob.id,
		BatchSize:   batchSize,
		BatchReason: reason,
	})

	// Process batch (inline inserts if enabled)
	if bc.options.InlineInserts && batchSize > 1 {
		if combined, ok := bc.inlineInserts(batchJob.Queries); ok {
			bc.events.Emit(Event{
				Type:          EventBatchInlineOptimised,
				BatchID:       batchJob.id,
				OriginalCount: len(batchJob.Queries),
				CombinedCount: len(combined),
			})
			batchJob.Queries = combined
		}
	}

	// Reset batch state
	bc.queries = bc.queries[:0]
	if bc.timer != nil {
		bc.timer.Stop()
		bc.timer = nil
	}

	// Submit to worker queue
	return bc.submitBatch(batchJob)
}

// submitBatch sends the batch to the worker queue.
// Returns ErrWorkerNotRunning or ErrQueueFull if submission fails.
func (bc *BatchCollector) submitBatch(batchJob BatchJob) error {
	if !bc.ws.workerRunning.Load() {
		bc.events.Emit(Event{Type: EventBatchSubmitFailed, BatchID: batchJob.id, BatchReason: "worker_not_running"})
		return ErrWorkerNotRunning
	}

	item := workItem{
		job:      Job{Type: JobTypeBatch, BatchJob: batchJob},
		ctx:      bc.ctx,
		queuedAt: time.Now(),
	}

	select {
	case bc.ws.queue <- item:
		bc.events.Emit(Event{Type: EventJobQueued, JobID: batchJob.id, JobType: JobTypeBatch})
		bc.events.Emit(Event{Type: EventBatchSubmitted, BatchID: batchJob.id, BatchSize: len(batchJob.Queries)})
		return nil
	default:
		bc.events.Emit(Event{Type: EventBatchSubmitFailed, BatchID: batchJob.id, BatchReason: "queue_full"})
		return ErrQueueFull
	}
}

// Close flushes any pending batch and stops the timer.
// Returns ErrQueueFull if the final batch cannot be submitted.
func (bc *BatchCollector) Close() error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.timer != nil {
		bc.timer.Stop()
		bc.timer = nil
	}

	if len(bc.queries) == 0 {
		return nil
	}

	batchJob := BatchJob{
		Queries: make([]Job, len(bc.queries)),
		id:      nextJobID(),
	}
	copy(batchJob.Queries, bc.queries)

	bc.events.Emit(Event{
		Type:        EventBatchFlushed,
		BatchID:     batchJob.id,
		BatchSize:   len(bc.queries),
		BatchReason: "close",
	})

	// Process final batch
	if bc.options.InlineInserts && len(bc.queries) > 1 {
		if combined, ok := bc.inlineInserts(batchJob.Queries); ok {
			bc.events.Emit(Event{
				Type:          EventBatchInlineOptimised,
				BatchID:       batchJob.id,
				OriginalCount: len(batchJob.Queries),
				CombinedCount: len(combined),
			})
			batchJob.Queries = combined
		}
	}

	bc.queries = bc.queries[:0]

	// Synchronous submission for close
	if !bc.ws.workerRunning.Load() {
		bc.events.Emit(Event{Type: EventBatchSubmitFailed, BatchID: batchJob.id, BatchReason: "worker_not_running"})
		return ErrWorkerNotRunning
	}

	item := workItem{
		job:        Job{Type: JobTypeBatch, BatchJob: batchJob},
		ctx:        bc.ctx,
		queuedAt:   time.Now(),
		resultChan: make(chan JobResult, 1),
	}

	select {
	case bc.ws.queue <- item:
		bc.events.Emit(Event{Type: EventJobQueued, JobID: batchJob.id, JobType: JobTypeBatch})
		<-item.resultChan // Wait for completion during close
		return nil
	default:
		bc.events.Emit(Event{Type: EventBatchSubmitFailed, BatchID: batchJob.id, BatchReason: "queue_full"})
		return ErrQueueFull
	}
}

// inlineInserts attempts to combine multiple INSERT statements
func (bc *BatchCollector) inlineInserts(items []Job) ([]Job, bool) {
	if len(items) < 2 {
		return items, false
	}

	// Group queries by SQL to combine similar INSERTs
	queryGroups := make(map[string][]Query)
	nonQueryJobs := make([]Job, 0)
	var sqlKeys []string

	// Separate queries from other jobs and group by SQL
	for _, item := range items {
		if item.Type == JobTypeQuery {
			sql := item.Query.SQL
			if _, exists := queryGroups[sql]; !exists {
				sqlKeys = append(sqlKeys, sql)
			}
			queryGroups[sql] = append(queryGroups[sql], item.Query)
		} else {
			nonQueryJobs = append(nonQueryJobs, item)
		}
	}

	if len(queryGroups) == 0 {
		return items, false
	}

	combinedJobs := make([]Job, 0, len(sqlKeys)+len(nonQueryJobs))
	anyCombined := false

	// Process each SQL group
	for _, sql := range sqlKeys {
		group := queryGroups[sql]
		if len(group) == 1 {
			combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: group[0]})
		} else {
			// Try to combine this group
			if combinedSQL, combinedArgs, ok := bc.combineInsertGroup(group); ok {
				combinedQuery := Query{
					SQL:   combinedSQL,
					Args:  combinedArgs,
					id:    nextJobID(),
					async: true,
				}
				combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: combinedQuery})
				anyCombined = true
			} else {
				for _, q := range group {
					combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: q})
				}
			}
		}
	}

	combinedJobs = append(combinedJobs, nonQueryJobs...)
	return combinedJobs, anyCombined
}

// combineInsertGroup attempts to combine a group of queries with identical SQL
func (bc *BatchCollector) combineInsertGroup(queries []Query) (string, []any, bool) {
	if len(queries) < 2 {
		return "", nil, false
	}

	firstQuery := queries[0]
	upperSQL := strings.ToUpper(firstQuery.SQL)

	// Check if it's an INSERT
	if !strings.HasPrefix(upperSQL, "INSERT") {
		return "", nil, false
	}

	// Reject INSERT...SELECT (cannot be combined)
	if strings.Contains(upperSQL, "SELECT") {
		return "", nil, false
	}

	// Find VALUES keyword and the opening parenthesis after it
	valuesIdx := strings.LastIndex(upperSQL, "VALUES")
	if valuesIdx == -1 {
		return "", nil, false
	}

	// Find the ( after VALUES in the original SQL (preserving case)
	afterValues := firstQuery.SQL[valuesIdx:]
	parenIdx := strings.Index(afterValues, "(")
	if parenIdx == -1 {
		return "", nil, false
	}

	placeholderPattern := afterValues[parenIdx:]

	// Validate placeholder count matches argument count
	expectedPlaceholders := len(firstQuery.Args)
	actualPlaceholders := strings.Count(placeholderPattern, "?")
	if actualPlaceholders != expectedPlaceholders {
		// Pattern extraction likely failed (e.g., parenthesis in string literal)
		return "", nil, false
	}

	// Build the combined query
	repeatingPattern := ", " + placeholderPattern
	estimatedSize := len(firstQuery.SQL) + (len(queries)-1)*len(repeatingPattern)

	var b strings.Builder
	b.Grow(estimatedSize)
	b.WriteString(firstQuery.SQL)

	for i := 1; i < len(queries); i++ {
		b.WriteString(repeatingPattern)
	}

	// Flatten all arguments
	argCount := len(firstQuery.Args)
	args := make([]any, 0, len(queries)*argCount)
	for _, query := range queries {
		args = append(args, query.Args...)
	}

	return b.String(), args, true
}
