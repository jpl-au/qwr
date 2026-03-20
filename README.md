# qwr - Query Write Reader

A Go library for SQLite that provides serialised writes and concurrent reads with optional context support. qwr uses a worker pool pattern with a single worker to sequentially queue writes to SQLite. It supports a configurable reader with connections.

## Quick Start

```go
package main

import (
    "log"
    
    "github.com/jpl-au/qwr"
    "github.com/jpl-au/qwr/profile"
)

func main() {
    // Create manager with default options
    manager, err := qwr.New("test.db").
        Reader(profile.ReadBalanced()).
        Writer(profile.WriteBalanced()).
        Open()
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close()

    // Write bypassing queue
    result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").Write()

    // Synchronous write
    result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Jane").Execute()
    if err != nil {
        log.Fatal(err)
    }

    // Asynchronous write
    jobID, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Bob").Async()
    if err != nil {
        log.Fatal(err)
    }

    // Read data
    rows, err := manager.Query("SELECT * FROM users").Read()
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Write Operations

**Direct Write**
Bypasses the worker queue for immediate execution.

```go
result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").Write()
```

**Synchronous Write**
Uses the worker pool to serialise the write, blocking until the write is complete.
```go
result, err := manager.Query("INSERT INTO USERS (name) VALUES (?)", "Jane").Execute()
```

**Async Write**
Asynchronous writes are non-blocking and are guarded by an error queue. The error queue attempts to retry transactions automatically with an exponential backoff + jitter. Certain errors are not recoverable.

```go
jobID, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Bob").Async()
```

Note: Async operations that fail are automatically added to the error queue. Since there's no immediate error return, you must check the error queue to detect failures:

```go
if jobErr, found := manager.GetErrorByID(jobID); found {
    log.Printf("Async job %d failed: %v", jobID, jobErr.Error())
}
```

**Batch Write**
Batching creates a slice of queries for deferred processing. Writing occurs either at a timed interval, or once the queue depth reaches a pre-determined threshold. An experimental feature inlines common queries in the batch to optimise the write process.

```go
// These will be automatically batched together
manager.Query("INSERT INTO users (name) VALUES (?)", "Charlie").Batch()
manager.Query("INSERT INTO users (name) VALUES (?)", "Diana").Batch()
```

**Transactions**
Multi-statement atomic operations with pre-declared statements.
```go
tx := manager.Transaction().
    Add("INSERT INTO users (name) VALUES (?)", "Eve").
    Add("UPDATE users SET active = ? WHERE name = ?", true, "Eve")

result, err := tx.Write() // or tx.Exec() for queued
```

**Callback Transactions**
For interleaved reads and writes within a single transaction. The callback receives a `*sql.Tx` - qwr manages begin, commit, and rollback.
```go
result, err := manager.TransactionFunc(func(tx *sql.Tx) (any, error) {
    var maxPos int
    tx.QueryRow("SELECT MAX(position) FROM items").Scan(&maxPos)

    _, err := tx.Exec("INSERT INTO items (position) VALUES (?)", maxPos+1)
    if err != nil {
        return nil, err
    }
    return maxPos + 1, nil
}).Exec() // or .Write() for direct execution
```

### Read Operations

Read operations use the reader connection pool and can be executed concurrently:

**Multiple Rows**:
```go
rows, err := manager.Query("SELECT * FROM users WHERE active = ?", true).Read()
if err != nil {
    log.Fatal(err)
}
defer rows.Close() // Must manually close

for rows.Next() {
    var user User
    if err := rows.Scan(&user.ID, &user.Name); err != nil {
        log.Fatal(err)
    }
    // process user...
}
```

**Multiple Rows with Automatic Cleanup**:
```go
var users []User
err := manager.Query("SELECT * FROM users WHERE active = ?", true).ReadClose(func(rows *sql.Rows) error {
    for rows.Next() {
        var user User
        if err := rows.Scan(&user.ID, &user.Name); err != nil {
            return err
        }
        users = append(users, user)
    }
    return nil
})
// rows.Close() called automatically
```

**Single Row**:
```go
row, err := manager.Query("SELECT name FROM users WHERE id = ?", 1).ReadRow()
if err != nil {
    log.Fatal(err)
}

var name string
if err := row.Scan(&name); err != nil {
    log.Fatal(err)
}
```

### Prepared Statements

Prepared statements are cached automatically when enabled. This reduces preparation overhead for repeated queries:
```go
result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").
    Prepared().
    Write()
```

### Context Support (Optional)

Contexts can be used for timeouts and cancellation but are not required. The library functions normally without any context usage:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Per-query context
result, err := manager.Query("SELECT * FROM users").
    WithContext(ctx).
    Read()

// Manager-level context (affects all operations)
manager, err := qwr.New("test.db").
    WithContext(ctx).
    Open()
```

## Database Profiles

Database profiles configure connection pools and SQLite PRAGMA settings for different use cases. qwr includes several pre-configured profiles:

### Read Profiles
- `profile.ReadLight()` - Low resource usage (5 connections, 30MB cache)
- `profile.ReadBalanced()` - General purpose (10 connections, 75MB cache)  
- `profile.ReadHeavy()` - High concurrency (25 connections, 150MB cache)

### Write Profiles
- `profile.WriteLight()` - Basic performance (50MB cache, 4KB pages)
- `profile.WriteBalanced()` - Most applications (100MB cache, 8KB pages)
- `profile.WriteHeavy()` - High volume (200MB cache, 8KB pages)

### Custom Profiles
```go
customProfile := profile.New().
    WithMaxOpenConns(15).
    WithCacheSize(-102400). // 100MB
    WithJournalMode(profile.JournalWal).
    WithSynchronous(profile.SyncNormal).
    WithPageSize(8192)

manager, err := qwr.New("test.db").
    Reader(customProfile).
    Writer(profile.WriteBalanced()).
    Open()
```

## Observing Events

qwr emits structured events for all significant operations. Subscribe to receive events for logging, metrics, tracing, or alerting:

```go
// Subscribe to all events
manager.Subscribe(func(e qwr.Event) {
    fmt.Printf("event: %d at %v\n", e.Type, e.Timestamp)
})

// Subscribe to specific event types using filters
manager.SubscribeFiltered(func(e qwr.Event) {
    fmt.Printf("error: %v\n", e.Err)
}, qwr.ErrorEvents())

// Unsubscribe when no longer needed
id := manager.Subscribe(handler)
manager.Unsubscribe(id)
```

To capture events from the moment the manager opens (including `EventManagerOpened`), use `WithObserver` during construction:

```go
manager, err := qwr.New("test.db").
    WithObserver(func(e qwr.Event) {
        log.Printf("qwr: %d", e.Type)
    }).
    Open()
```

Available filter constructors:

| Filter | Matches |
|--------|---------|
| `JobEvents()` | `JobQueued`, `JobStarted`, `JobCompleted`, `JobFailed` |
| `ErrorEvents()` | `JobFailed`, `ErrorStored`, `ErrorPersisted`, `ErrorQueueOverflow`, `RetryExhausted`, `DirectWriteFailed` |
| `CacheEvents()` | `CacheHit`, `CacheMiss`, `CacheEvicted`, `CachePrepError` |
| `RetryEvents()` | `RetryScheduled`, `RetryStarted`, `RetryExhausted` |
| `WriteEvents()` | `JobQueued`, `JobStarted`, `JobCompleted`, `JobFailed`, `DirectWriteCompleted`, `DirectWriteFailed` |
| `BatchEvents()` | `BatchQueryAdded`, `BatchFlushed`, `BatchInlineOptimized`, `BatchSubmitted`, `BatchSubmitFailed` |
| `BackupEvents()` | `BackupStarted`, `BackupCompleted`, `BackupFailed`, `BackupFallback` |
| `LifecycleEvents()` | `ManagerOpened`, `ManagerClosing`, `ManagerClosed`, `WorkerStarted`, `WorkerStopped` |

## Configuration Options

The Options struct controls various aspects of qwr's behaviour. All options have sensible defaults:

```go
options := qwr.Options{
    // Worker configuration
    WorkerQueueDepth:  50000,         // Queue buffer size
    EnableReader:      true,          // Enable read operations
    EnableWriter:      true,          // Enable write operations

    // Batching
    BatchSize:         200,           // Queries per batch
    BatchTimeout:      1*time.Second, // Max batch wait time
    InlineInserts:     false,         // Experimental: combine INSERT statements

    // Context behaviour
    UseContexts:       false,         // Default context usage

    // Statement caching
    StmtCacheMaxSize:      1000,      // Max cached prepared statements
    UsePreparedStatements: false,     // Use prepared statements for all queries by default

    // Error handling
    ErrorQueueMaxSize: 1000,          // Max errors in memory
    ErrorLogPath:      "",            // Set via WithErrorDB() with an empty string disabling it
    EnableAutoRetry:   false,         // Automatic retry
    MaxRetries:        3,             // Max retry attempts
    BaseRetryDelay:    30*time.Second,// Initial retry delay

    // Timeouts
    JobTimeout:         30*time.Second, // Individual job timeout
    TransactionTimeout: 30*time.Second, // Transaction timeout
    RetrySubmitTimeout: 5*time.Second,  // Retry submission timeout
    QueueSubmitTimeout: 5*time.Minute,  // Timeout for context-free queue submissions
}
```

## Error Handling & Retry

For asynchronous processing of queries, qwr provides error classifications to optimise retry strategies.

### Enhanced Error Classification
qwr classifies errors:
- **Connection Errors**: File I/O issues, permission problems → Linear backoff retry
- **Lock Errors**: Database busy, locked → Exponential backoff retry
- **Constraint Violations**: Unique key, foreign key, NOT NULL → No retry (permanent failure)
- **Schema Errors**: Missing tables/columns, syntax errors → No retry (permanent failure)  
- **Resource Errors**: Disk full, out of memory → Linear backoff retry
- **Timeout Errors**: Context timeouts, deadlines → Linear backoff retry
- **Permission Errors**: Access denied, read-only → No retry (permanent failure)
- **Internal Errors**: qwr-specific errors → No retry
- **Unknown Errors**: Unclassified → No retry (safe default)

### Error Logging
Errors from async operations can be persisted to a separate SQLite database. This is disabled by default and must be explicitly enabled using `WithErrorDB()`:

```go
manager, err := qwr.New("test.db").
    WithErrorDB("errors.db").  // Enable persistent error logging
    Open()
```

When enabled, errors are logged with full context:
- SQL statement and parameters (CBOR encoded)
- Error type and message
- Retry attempts and timestamps
- Failure reason

If `WithErrorDB()` is not called, error logging is disabled. Using `:memory:` is rejected to prevent unbound memory growth.

### Retry Configuration
```go
options := qwr.Options{
    EnableAutoRetry: true,
    MaxRetries:      3,
    BaseRetryDelay:  30 * time.Second,
}

manager, err := qwr.New("test.db", options).Open()
```

When auto-retry is enabled, each retriable failure schedules its own retry using the calculated exponential backoff delay. No polling is needed.

### Error Queue Management
```go
// Get all errors
errors := manager.GetErrors()

// Get specific error with enhanced information
if jobErr, found := manager.GetErrorByID(jobID); found {
    fmt.Printf("Job %d failed: %v\n", jobID, jobErr.Error())
    
    // Access structured error information
    if qwrErr := jobErr.errType; qwrErr != nil {
        fmt.Printf("Error category: %s\n", qwrErr.Category)
        fmt.Printf("Retry strategy: %s\n", qwrErr.Strategy) 
        fmt.Printf("Context: %+v\n", qwrErr.Context)
    }
}

// Manually retry a failed job
if err := manager.RetryJob(jobID); err != nil {
    switch {
    case errors.Is(err, qwr.ErrJobNotFound):
        fmt.Printf("Job %d not found in error queue\n", jobID)
    case errors.Is(err, qwr.ErrRetrySubmissionFailed):
        fmt.Printf("Failed to resubmit job %d\n", jobID)
    default:
        fmt.Printf("Retry error: %v\n", err)
    }
}

// Clear error queue
manager.ClearErrors()
```

### Automatic Batching
The batch collector groups multiple operations together and executes them in a single transaction. This reduces the number of database round trips:
```go
// Configure batching
options := qwr.Options{
    BatchSize:    200,                // Execute after 200 queries
    BatchTimeout: 1 * time.Second,    // Or after 1 second
    InlineInserts: true,              // Experimental: opt-in for simple INSERTs
}
```

### Statement Caching
Prepared statements are cached using ristretto with LRU eviction. The maximum size is controlled by `StmtCacheMaxSize` (default: 1000).

### Inline INSERT Optimisation (Experimental)
Inline INSERT optimises statements with identical SQL are combined into a single multi-value INSERT:
```go
// These individual statements:
INSERT INTO users (name) VALUES ('Alice')
INSERT INTO users (name) VALUES ('Bob')
INSERT INTO users (name) VALUES ('Charlie')

// Becomes:
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')
```

## Monitoring

qwr emits structured events for all significant operations. Subscribe to events to build custom monitoring:

```go
// Count completed jobs
var jobsCompleted atomic.Int64
manager.SubscribeFiltered(func(e qwr.Event) {
    jobsCompleted.Add(1)
}, qwr.JobEvents())

// Track cache hit ratio
var hits, misses atomic.Int64
manager.SubscribeFiltered(func(e qwr.Event) {
    if e.Type == qwr.EventCacheHit {
        hits.Add(1)
    } else if e.Type == qwr.EventCacheMiss {
        misses.Add(1)
    }
}, qwr.CacheEvents())

// Log errors
manager.SubscribeFiltered(func(e qwr.Event) {
    log.Printf("qwr error: %v (job %d)", e.Err, e.JobID)
}, qwr.ErrorEvents())
```

### Observer Performance & Safety

*   **Synchronous Dispatch:** Event handlers are executed synchronously on the caller's goroutine (e.g., the write worker loop). **Never** perform blocking I/O or slow operations directly in a handler; always offload them to a separate goroutine if they might block.
*   **Re-entrancy:** It is safe to call `Subscribe` or `Unsubscribe` from within an event handler (the subscriber list is snapshot-copied before dispatch). However, this is not recommended as it makes event flow harder to reason about.
*   **Panic Isolation:** Panics in event handlers are recovered automatically and will not crash the caller. However, handlers should still handle their own errors gracefully to avoid silent failures.
*   **Lifecycle Guarantees:** `EventManagerClosed` is the last event emitted and is guaranteed to be delivered before `Close()` returns. After `Close()`, no further events are dispatched.

### Wait for Queue to Drain
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := manager.WaitForIdle(ctx); err != nil {
    log.Printf("Timeout waiting for queue to drain: %v", err)
}
```

### Manual Cache Management
```go
// Clear statement cache (frees memory, cache rebuilds on demand)
manager.ResetCaches()
```

### Database Maintenance
```go
// Full vacuum (rebuilds entire database)
err := manager.RunVacuum()

// Incremental vacuum (reclaims some space)
err := manager.RunIncrementalVacuum(1000) // 1000 pages

// Manual WAL checkpoint
err := manager.RunCheckpoint(checkpoint.Passive)

// Online backup (Default tries API first, falls back to Vacuum)
err := manager.Backup("/path/to/backup.db", backup.Default)

// Backup using SQLite backup API (less locking, better for large DBs)
err := manager.Backup("/path/to/backup.db", backup.API)

// Backup using VACUUM INTO (creates optimized/defragmented copy)
err := manager.Backup("/path/to/backup.db", backup.Vacuum)
```

### Automatic WAL Checkpoint on Close

Configure automatic WAL checkpointing when the manager closes:

```go
import "github.com/jpl-au/qwr/checkpoint"

manager, err := qwr.New("test.db").
    Checkpoint(checkpoint.Truncate).
    Open()
```

Available checkpoint modes:
- `checkpoint.None` - No checkpoint on close (default)
- `checkpoint.Passive` - Non-blocking, best-effort checkpoint
- `checkpoint.Full` - Wait for writers, checkpoint all frames
- `checkpoint.Restart` - Full + restart WAL from beginning
- `checkpoint.Truncate` - Restart + truncate WAL to zero bytes

## Caveats & Design Decisions

### Statement Cache

The prepared statement cache uses [ristretto](https://github.com/dgraph-io/ristretto) with LRU eviction. The maximum cache size is controlled by `StmtCacheMaxSize` (default: 1000). When the cache is full, least recently used statements are evicted.

- Always use parameterised queries with `?` placeholders
- Avoid building SQL strings with dynamic values embedded

### Error Queue Overflow Behaviour

When the error queue exceeds `ErrorQueueMaxSize`, the oldest errors are persisted to the error log database and then removed from the in-memory queue.

1. In-memory queue fills to `ErrorQueueMaxSize` (default: 1,000 errors)
2. New errors trigger overflow handling
3. Oldest errors are written to the SQLite3 error log database
4. Oldest errors are removed from memory to make space
5. Error information is preserved on disk for later analysis

If the error log database write fails (e.g., disk full, permissions), the error data is permanently lost.

## Using Custom SQLite Drivers

qwr uses `modernc.org/sqlite` by default, but you can bring your own SQLite driver using the `NewSQL()` constructor:

```go
package main

import (
    "database/sql"
    "log"

    "github.com/jpl-au/qwr"
    "github.com/jpl-au/qwr/profile"
    _ "github.com/mattn/go-sqlite3" // Your preferred SQLite driver
)

func main() {
    // Open your own database connections
    readerDB, err := sql.Open("sqlite3", "test.db")
    if err != nil {
        log.Fatal(err)
    }

    writerDB, err := sql.Open("sqlite3", "test.db")
    if err != nil {
        log.Fatal(err)
    }

    // Pass connections to qwr and enable error logging
    manager, err := qwr.NewSQL(readerDB, writerDB).
        WithErrorDB("errors.db").  // Optional: enable persistent error logging
        Reader(profile.ReadBalanced()).
        Writer(profile.WriteBalanced()).
        Open()
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close() // This will also close readerDB and writerDB

    // Use manager normally...
}
```
The Manager takes full ownership of your database connections. Call `Manager.Close()` to close both reader and writer connections. Use `WithErrorDB()` to enable persistent error logging for async operations.

Profiles can still be applied to your connections (profiles are just specified SQLite PRAGMAS).

qwr was inspired by numerous articles that describe using SQLite3 in production systems.

### Consider SQLite
**Author**: Wesley Aptekar-Cassels
**URL**: https://blog.wesleyac.com/posts/consider-sqlite

### Scaling SQLite to 4M QPS on a Single Server
**Author**: Expensify Engineering
**URL**: https://use.expensify.com/blog/scaling-sqlite-to-4m-qps-on-a-single-server

### Your Database is Your Prison - Here's How Expensify Broke Free
**Author**: First Round Review
**URL**: https://review.firstround.com/your-database-is-your-prison-heres-how-expensify-broke-free/

### How (and Why) to Run SQLite in Production
**Author**: Stephen Margheim
**URL**: https://fractaledmind.github.io/2023/12/23/rubyconftw/

### Gotchas with SQLite in Production
**Author**: Anže Pečar
**URL**: https://blog.pecar.me/sqlite-prod
