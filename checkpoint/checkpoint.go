// Package checkpoint defines WAL checkpoint modes for SQLite.
package checkpoint

import "fmt"

// Mode defines the WAL checkpoint behaviour on close.
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

// Valid reports whether m is a recognised checkpoint mode.
func (m Mode) Valid() bool {
	switch m {
	case None, Passive, Full, Restart, Truncate:
		return true
	}
	return false
}

// ErrInvalidMode is returned when a checkpoint mode is not recognised.
var ErrInvalidMode = fmt.Errorf("invalid checkpoint mode")
