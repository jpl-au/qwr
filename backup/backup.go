// Package backup defines backup methods for SQLite databases.
package backup

// Method defines the backup method to use.
type Method int

const (
	// Default uses the SQLite backup API if available, otherwise falls back to Vacuum.
	Default Method = iota

	// API uses SQLite's online backup API (sqlite3_backup_*).
	// Less locking - only locks while reading pages, not for entire operation.
	// Better for large databases with concurrent access.
	API

	// Vacuum uses SQLite's VACUUM INTO command.
	// Creates an optimized, defragmented copy.
	// Holds locks for the entire operation but produces a smaller file.
	Vacuum
)
