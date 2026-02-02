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
// # Profiles
//
// Database profiles configure connection pools and SQLite PRAGMA settings.
// Pre-configured profiles are available in the profile subpackage:
//
//   - profile.ReadLight(), ReadBalanced(), ReadHeavy()
//   - profile.WriteLight(), WriteBalanced(), WriteHeavy()
//
// # Error Handling
//
// Async operations capture errors in an error queue with automatic retry
// support for transient failures. Errors are classified by type to determine
// appropriate retry strategies.
//
//	if jobErr, found := manager.GetErrorByID(jobID); found {
//		log.Printf("Job %d failed: %v", jobID, jobErr.Error())
//	}
//
// # Custom SQLite Drivers
//
// qwr uses modernc.org/sqlite by default. Use [NewSQL] to provide your own
// database connections with a different driver.
package qwr
