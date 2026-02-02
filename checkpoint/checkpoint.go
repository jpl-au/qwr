// Package checkpoint defines WAL checkpoint modes for SQLite.
package checkpoint

// Mode defines the WAL checkpoint behavior on close.
// These map directly to SQLite's PRAGMA wal_checkpoint modes.
type Mode string

// Checkpoint modes for use with the Checkpoint() builder method.
const (
	None     Mode = ""
	Passive  Mode = "PASSIVE"
	Full     Mode = "FULL"
	Restart  Mode = "RESTART"
	Truncate Mode = "TRUNCATE"
)
