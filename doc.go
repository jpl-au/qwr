// Package qwr provides serialised writes and concurrent reads for SQLite databases.
//
// qwr (Query Write Reader) uses a worker pool pattern with a single writer to
// sequentially queue writes to SQLite while allowing concurrent read operations
// through a configurable connection pool.
//
// # Quick Start
//
//	manager, err := qwr.New("app.db").
//		Reader(profile.ReadBalanced()).
//		Writer(profile.WriteBalanced()).
//		Open()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer manager.Close()
//
// # Write Operations
//
// qwr provides several write modes:
//
//   - Direct Write: Bypasses the queue for immediate execution
//   - Synchronous Write: Uses the worker pool, blocks until complete
//   - Async Write: Non-blocking, errors captured in error queue
//   - Batch Write: Groups multiple operations into single transactions
//   - Transactions: Multi-statement atomic operations
//
// Example writes:
//
//	// Direct write (bypasses queue)
//	result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").Write()
//
//	// Synchronous write (queued, blocks)
//	result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Bob").Execute()
//
//	// Async write (queued, non-blocking)
//	jobID, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Charlie").Async()
//
// # Read Operations
//
// Reads use the connection pool and can be executed concurrently:
//
//	rows, err := manager.Query("SELECT * FROM users").Read()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer rows.Close()
//
// # Attached Databases
//
// qwr supports SQLite's ATTACH DATABASE for working with multiple database
// files through a single manager. Attached databases share the main connection
// pool and write serialiser, enabling cross-database queries and atomic
// transactions across databases.
//
// Attach databases at construction time via the builder:
//
//	manager, err := qwr.New("main.db").
//		Reader(profile.ReadBalanced()).
//		Writer(profile.WriteBalanced()).
//		Attach("analytics", "analytics.db", profile.Attached().
//			WithJournalMode(profile.JournalWal)).
//		Open()
//
// Or at runtime via the manager:
//
//	manager.Attach("logs", "logs.db")
//	manager.ResetReaderPool() // force immediate reader access
//
// Queries reference attached databases using the schema-qualified syntax:
//
//	// Cross-database query
//	rows, _ := manager.Query("SELECT u.name FROM users u JOIN analytics.events e ON ...").Read()
//
//	// Write to attached database
//	manager.Query("INSERT INTO analytics.events (action) VALUES (?)", "login").Execute()
//
// Always use schema-qualified table names for attached databases
// (e.g. analytics.events, not just events). Unqualified names resolve to
// the main database.
//
// Bare :memory: paths are rejected because each pooled connection would get
// its own isolated in-memory database. Use file::memory:?cache=shared for a
// shared in-memory attached database.
//
// Do not use [Query.Prepared] for schema-qualified queries before the schema
// is attached - the preparation will fail on every call until the schema exists.
//
// For parallel writes to independent databases, use separate [Manager]
// instances rather than ATTACH. Attached databases share a single serialised
// writer, which is correct for cross-database transactions but does not offer
// write parallelism.
//
// Attach is not supported with [NewSQL] because qwr cannot control connection
// creation for user-provided database handles.
//
// # Observing Events
//
// qwr emits structured events for all significant operations. Subscribe to
// receive events for logging, metrics, tracing, or alerting:
//
//	manager.Subscribe(func(e qwr.Event) {
//		fmt.Printf("event: %d at %v\n", e.Type, e.Timestamp)
//	})
//
// Use filters to receive only specific event types:
//
//	manager.SubscribeFiltered(func(e qwr.Event) {
//		fmt.Printf("error: %v\n", e.Err)
//	}, qwr.ErrorEvents())
//
// # Profiles
//
// Database profiles configure connection pools and SQLite PRAGMA settings.
// Pre-configured profiles are available in the profile subpackage:
//
//   - profile.ReadLight(), ReadBalanced(), ReadHeavy()
//   - profile.WriteLight(), WriteBalanced(), WriteHeavy()
//   - profile.Attached() for per-schema PRAGMAs on attached databases
//
// # Error Handling
//
// Async operations capture errors in an error queue with automatic retry
// support for transient failures. Errors are classified by type to determine
// appropriate retry strategies.
//
//	if jobErr, found := manager.ErrorByID(jobID); found {
//		log.Printf("Job %d failed: %v", jobID, jobErr.Error())
//	}
//
// # Custom SQLite Drivers
//
// qwr uses modernc.org/sqlite by default. Use [NewSQL] to provide your own
// database connections with a different driver. Note that [NewSQL] does not
// support Attach - manage ATTACH statements on your own connections.
package qwr
