# AGENTS.md

Instructions for AI coding agents generating application code that uses `github.com/jpl-au/qwr`.

## What qwr Is

qwr is a Go library for SQLite that serialises writes through a single worker and provides concurrent reads. It manages separate reader and writer connections with configurable profiles.

## Setup

```go
import (
    "github.com/jpl-au/qwr"
    "github.com/jpl-au/qwr/profile"
)

manager, err := qwr.New("path/to/db.sqlite",
    qwr.Options{BatchSize: 200},
).
    Reader(profile.ReadBalanced()).
    Writer(profile.WriteBalanced()).
    Open()
if err != nil {
    return err
}
defer manager.Close()
```

Always `defer manager.Close()`. Close is idempotent and safe to call multiple times.

Omitting `Options` uses `qwr.DefaultOptions`. Omitting `Reader()` or `Writer()` uses balanced profiles.

## Choosing a Write Method

There are four write methods. Pick the right one:

| Method | Queued | Blocks | Error Return | Use When |
|--------|--------|--------|--------------|----------|
| `Write()` | No | Yes | Immediate | You need the result now and don't need serialisation |
| `Execute()` | Yes | Yes | Immediate | You need serialised writes with error feedback |
| `Async()` | Yes | No | Submission only | Fire-and-forget; check error queue later |
| `Batch()` | Yes | No | Submission only | High-volume inserts where latency can be deferred |

```go
// Direct write — bypasses queue, immediate result
result, err := manager.Query("INSERT INTO t (a) VALUES (?)", 1).Write()

// Queued synchronous — serialised, blocks until done
result, err := manager.Query("INSERT INTO t (a) VALUES (?)", 1).Execute()

// Async — returns job ID, errors go to error queue
jobID, err := manager.Query("INSERT INTO t (a) VALUES (?)", 1).Async()

// Batch — collected and flushed by size or timeout
jobID, err := manager.Query("INSERT INTO t (a) VALUES (?)", 1).Batch()
```

`Async()` and `Batch()` return `ErrQueueFull` if the worker queue is saturated. Always check the returned error.

## Reading Data

```go
// Multiple rows — caller MUST close
rows, err := manager.Query("SELECT id, name FROM users WHERE active = ?", true).Read()
if err != nil {
    return err
}
defer rows.Close()

// Multiple rows with automatic cleanup (preferred)
err := manager.Query("SELECT id, name FROM users").ReadClose(func(rows *sql.Rows) error {
    for rows.Next() {
        var u User
        if err := rows.Scan(&u.ID, &u.Name); err != nil {
            return err
        }
        users = append(users, u)
    }
    return nil
})

// Single row
row, err := manager.Query("SELECT name FROM users WHERE id = ?", 1).ReadRow()
if err != nil {
    return err
}
if err := row.Scan(&name); err != nil {
    return err // may be sql.ErrNoRows
}
```

Prefer `ReadClose()` over `Read()` — it prevents forgotten `rows.Close()` calls.

## Transactions

**Declarative** — pre-built list of statements, no mid-transaction reads:

```go
tx := manager.Transaction().
    Add("INSERT INTO users (name) VALUES (?)", "Alice").
    Add("UPDATE counters SET n = n + 1 WHERE name = ?", "users")

// Direct execution
result, err := tx.Write()

// Through the worker queue
result, err := tx.Exec()
```

Use `AddPrepared()` instead of `Add()` for repeated SQL patterns within a transaction.

**Callback** — full `*sql.Tx` access for interleaved reads and writes:

```go
result, err := manager.TransactionFunc(func(tx *sql.Tx) (any, error) {
    var maxPos int
    if err := tx.QueryRow("SELECT MAX(position) FROM items").Scan(&maxPos); err != nil {
        return nil, err
    }
    _, err := tx.Exec("INSERT INTO items (position) VALUES (?)", maxPos+1)
    return maxPos + 1, err
}).Exec()

// result.Value holds the returned value (type assert as needed)
pos := result.Value.(int)
```

qwr manages `BeginTx`/`Commit`/`Rollback`. Return a non-nil error from the callback to trigger rollback. Both `Exec()` (queued) and `Write()` (direct) are supported.

## Common Mistakes

**Do not reuse a QueryBuilder after execution.** Every terminal method (`Write`, `Execute`, `Async`, `Batch`, `Read`, `ReadClose`, `ReadRow`) releases the builder back to an internal pool. Using it afterwards is undefined behaviour.

```go
// WRONG
q := manager.Query("INSERT INTO t (a) VALUES (?)", 1)
q.Write()
q.Write() // undefined — builder has been released

// CORRECT
manager.Query("INSERT INTO t (a) VALUES (?)", 1).Write()
manager.Query("INSERT INTO t (a) VALUES (?)", 2).Write()
```

**Always use parameterised queries.** Never interpolate values into SQL strings.

```go
// WRONG
manager.Query(fmt.Sprintf("INSERT INTO t (name) VALUES ('%s')", name)).Write()

// CORRECT
manager.Query("INSERT INTO t (name) VALUES (?)", name).Write()
```

**Async errors are not returned at call time.** `Async()` only returns submission errors (e.g. queue full). Execution errors appear in the error queue:

```go
jobID, err := manager.Query("INSERT INTO t (a) VALUES (?)", 1).Async()
if err != nil {
    // Submission failed — query was never queued
    return err
}
// Execution errors must be checked separately
if jobErr, found := manager.GetErrorByID(jobID); found {
    // Query was queued but failed during execution
}
```

**Batch ignores per-query contexts.** `WithContext()` has no effect on `Batch()` operations. The manager's internal context is used for all batched queries.

## Contexts

Contexts are opt-in. Most applications don't need them:

```go
// Per-query context
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
rows, err := manager.Query("SELECT ...").WithContext(ctx).Read()

// Manager-level context (applies to all operations by default)
manager, err := qwr.New("db.sqlite").WithContext(ctx).Open()
```

Context cancellation on `Execute()` cancels the *wait*, not the *execution*. The write still happens.

## Profiles

Pick based on workload:

| Profile | Connections | Cache | Use Case |
|---------|-------------|-------|----------|
| `ReadLight()` | 5 | 30MB | Low traffic |
| `ReadBalanced()` | 10 | 75MB | General purpose |
| `ReadHeavy()` | 25 | 150MB | High concurrency |
| `WriteLight()` | 1 | 50MB | Basic writes |
| `WriteBalanced()` | 1 | 100MB | Most applications |
| `WriteHeavy()` | 1 | 200MB | High volume |

## Observing Events

Subscribe to events for logging or metrics. Keep handlers fast — they run synchronously on the worker goroutine.

```go
manager.SubscribeFiltered(func(e qwr.Event) {
    log.Printf("error: %v", e.Err)
}, qwr.ErrorEvents())
```

Available filters: `JobEvents()`, `ErrorEvents()`, `CacheEvents()`, `RetryEvents()`, `WriteEvents()`, `BatchEvents()`, `BackupEvents()`, `LifecycleEvents()`.

To capture `EventManagerOpened`, use `WithObserver()` during construction:

```go
manager, err := qwr.New("db.sqlite").
    WithObserver(func(e qwr.Event) { /* ... */ }).
    Open()
```

## Shutdown

```go
defer manager.Close()
```

`Close()` flushes pending batches, drains the worker queue, checkpoints WAL (if configured), and closes all connections. It is idempotent.

To wait for in-flight work before shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
manager.WaitForIdle(ctx)
manager.Close()
```
