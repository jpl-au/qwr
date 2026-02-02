// Package profiles provides pre-configured database profiles for different workload types.
// Each profile optimises SQLite settings for specific use cases like read-heavy operations,
// write-intensive workloads, or mixed scenarios.
package profile

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// JournalMode defines available journal modes for SQLite
type JournalMode string

const (
	JournalDelete   JournalMode = "DELETE"
	JournalTruncate JournalMode = "TRUNCATE"
	JournalPersist  JournalMode = "PERSIST"
	JournalMemory   JournalMode = "MEMORY"
	JournalWal      JournalMode = "WAL"
	JournalOff      JournalMode = "OFF"
)

// SynchronousMode defines available synchronous levels for SQLite
type SynchronousMode string

const (
	SyncOff    SynchronousMode = "0"
	SyncNormal SynchronousMode = "1"
	SyncFull   SynchronousMode = "2"
	SyncExtra  SynchronousMode = "3"
)

// TempStore defines available temp_store modes
type TempStore string

const (
	TempStoreDefault TempStore = "DEFAULT"
	TempStoreFile    TempStore = "FILE"
	TempStoreMemory  TempStore = "MEMORY"
)

// AutoVacuum defines available auto_vacuum modes
type AutoVacuum string

const (
	AutoVacuumNone        AutoVacuum = "NONE"
	AutoVacuumFull        AutoVacuum = "FULL"
	AutoVacuumIncremental AutoVacuum = "INCREMENTAL"
)

// LockingMode defines available locking modes
type LockingMode string

const (
	LockingNormal    LockingMode = "NORMAL"
	LockingExclusive LockingMode = "EXCLUSIVE"
)

// Profile holds configuration for database connections
type Profile struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	Pragmas         map[string]interface{}
}

// New creates a new empty profile with initialized pragmas map
func New() *Profile {
	return &Profile{
		Pragmas: make(map[string]interface{}),
	}
}

var allowedPragmas = map[string]bool{
	"journal_mode":       true,
	"synchronous":        true,
	"foreign_keys":       true,
	"cache_size":         true,
	"mmap_size":          true,
	"temp_store":         true,
	"auto_vacuum":        true,
	"page_size":          true,
	"busy_timeout":       true,
	"locking_mode":       true,
	"recursive_triggers": true,
	"secure_delete":      true,
	"query_only":         true,
}

// Apply configures a database connection with this profile
func (p *Profile) Apply(db *sql.DB) error {
	db.SetMaxOpenConns(p.MaxOpenConns)
	db.SetMaxIdleConns(p.MaxIdleConns)
	if p.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(p.ConnMaxLifetime)
	}
	for name, value := range p.Pragmas {
		if !allowedPragmas[name] {
			return fmt.Errorf("pragma %q is not in allowlist", name)
		}
		query := fmt.Sprintf("PRAGMA %s = %v", name, value)
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

// String returns a string representation of the profile
func (p *Profile) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("MaxOpenConns: %d, MaxIdleConns: %d",
		p.MaxOpenConns, p.MaxIdleConns))

	if p.ConnMaxLifetime > 0 {
		b.WriteString(fmt.Sprintf(", ConnMaxLifetime: %v", p.ConnMaxLifetime))
	}

	b.WriteString(", Pragmas: {")
	i := 0
	for name, value := range p.Pragmas {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%s: %v", name, value))
		i++
	}
	b.WriteString("}")
	return b.String()
}

// Clone returns a deep copy of the profile
func (p *Profile) Clone() *Profile {
	clone := &Profile{
		MaxOpenConns:    p.MaxOpenConns,
		MaxIdleConns:    p.MaxIdleConns,
		ConnMaxLifetime: p.ConnMaxLifetime,
		Pragmas:         make(map[string]interface{}, len(p.Pragmas)),
	}
	for k, v := range p.Pragmas {
		clone.Pragmas[k] = v
	}
	return clone
}

// WithMaxOpenConns sets the maximum number of open connections to the database.
// For read profiles: higher values allow more concurrent queries (5-25 typical).
// For write profiles: should be 1 due to SQLite's single-writer constraint.
func (p *Profile) WithMaxOpenConns(n int) *Profile {
	p.MaxOpenConns = n
	return p
}

// WithMaxIdleConns sets the maximum number of idle connections in the pool.
// Should be less than or equal to MaxOpenConns. Higher values reduce connection
// overhead but use more resources. Typical values: 2-12 depending on workload.
func (p *Profile) WithMaxIdleConns(n int) *Profile {
	p.MaxIdleConns = n
	return p
}

// WithConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Prevents accumulation of connection-specific state and handles server restarts.
// Common values: time.Hour (default), time.Minute*30 (high churn), 0 (unlimited)
func (p *Profile) WithConnMaxLifetime(d time.Duration) *Profile {
	p.ConnMaxLifetime = d
	return p
}

// WithJournalMode sets the journal_mode pragma
func (p *Profile) WithJournalMode(mode JournalMode) *Profile {
	p.Pragmas["journal_mode"] = string(mode)
	return p
}

// WithSynchronous sets the synchronous pragma
func (p *Profile) WithSynchronous(mode SynchronousMode) *Profile {
	p.Pragmas["synchronous"] = string(mode)
	return p
}

// WithForeignKeys sets the foreign_keys pragma
func (p *Profile) WithForeignKeys(enabled bool) *Profile {
	if enabled {
		p.Pragmas["foreign_keys"] = "ON"
	} else {
		p.Pragmas["foreign_keys"] = "OFF"
	}
	return p
}

// WithCacheSize sets the cache_size pragma for SQLite's page cache.
// Positive values specify cache size in KiB, negative values specify number of pages.
// Larger cache improves read performance but uses more memory.
// Example: -102400 = 100MB cache (recommended for most applications)
func (p *Profile) WithCacheSize(kibibytes int) *Profile {
	p.Pragmas["cache_size"] = kibibytes
	return p
}

// WithMMapSize sets the mmap_size pragma for memory-mapped I/O.
// Specifies the maximum size in bytes that SQLite will use for memory-mapped files.
// Larger values can improve performance for read-heavy workloads.
// Common values: 268435456 (256MB), 536870912 (512MB), 0 (disable mmap)
func (p *Profile) WithMMapSize(bytes int64) *Profile {
	p.Pragmas["mmap_size"] = bytes
	return p
}

// WithTempStore sets the temp_store pragma
func (p *Profile) WithTempStore(store TempStore) *Profile {
	p.Pragmas["temp_store"] = string(store)
	return p
}

// WithAutoVacuum sets the auto_vacuum pragma
func (p *Profile) WithAutoVacuum(mode AutoVacuum) *Profile {
	p.Pragmas["auto_vacuum"] = string(mode)
	return p
}

// WithPageSize sets the page_size pragma for database pages.
// Must be a power of 2 between 512 and 65536 bytes.
// Larger pages reduce overhead for large records but use more memory.
// Common values: 4096 (default), 8192 (good for write-heavy), 16384 (large records)
func (p *Profile) WithPageSize(bytes int) *Profile {
	p.Pragmas["page_size"] = bytes
	return p
}

// WithBusyTimeout sets the busy_timeout pragma for database lock retries.
// Specifies how long (in milliseconds) to wait for locks before returning SQLITE_BUSY.
// Higher values reduce lock contention errors but may increase latency.
// Common values: 5000 (5 seconds, default), 10000 (high contention), 1000 (low latency)
func (p *Profile) WithBusyTimeout(ms int) *Profile {
	p.Pragmas["busy_timeout"] = ms
	return p
}

// WithLockingMode sets the locking_mode pragma
func (p *Profile) WithLockingMode(mode LockingMode) *Profile {
	p.Pragmas["locking_mode"] = string(mode)
	return p
}

// WithRecursiveTriggers sets the recursive_triggers pragma
func (p *Profile) WithRecursiveTriggers(enabled bool) *Profile {
	if enabled {
		p.Pragmas["recursive_triggers"] = "ON"
	} else {
		p.Pragmas["recursive_triggers"] = "OFF"
	}
	return p
}

// WithSecureDelete sets the secure_delete pragma
func (p *Profile) WithSecureDelete(enabled bool) *Profile {
	if enabled {
		p.Pragmas["secure_delete"] = "ON"
	} else {
		p.Pragmas["secure_delete"] = "OFF"
	}
	return p
}

// WithQueryOnly sets the query_only pragma
func (p *Profile) WithQueryOnly(enabled bool) *Profile {
	if enabled {
		p.Pragmas["query_only"] = "ON"
	} else {
		p.Pragmas["query_only"] = "OFF"
	}
	return p
}

// Read profiles optimised for read operations

// ReadLight provides basic read performance with low resource usage
func ReadLight() *Profile {
	return New().
		WithMaxOpenConns(5).
		WithMaxIdleConns(2).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-30720). // 30MB
		WithPageSize(4096).
		WithMMapSize(134217728). // 128MB
		WithBusyTimeout(5000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumIncremental).
		WithRecursiveTriggers(true)
}

// ReadBalanced provides good read performance suitable for most applications
func ReadBalanced() *Profile {
	return New().
		WithMaxOpenConns(10).
		WithMaxIdleConns(5).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-76800). // 75MB
		WithPageSize(4096).
		WithMMapSize(268435456). // 256MB
		WithBusyTimeout(5000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumIncremental).
		WithRecursiveTriggers(true)
}

// ReadHeavy maximises read throughput for high-concurrency scenarios
func ReadHeavy() *Profile {
	return New().
		WithMaxOpenConns(25).
		WithMaxIdleConns(12).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-153600). // 150MB
		WithPageSize(4096).
		WithMMapSize(536870912). // 512MB
		WithBusyTimeout(5000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumIncremental).
		WithRecursiveTriggers(true)
}

// Write profiles optimised for write operations

// WriteLight provides basic write performance with low resource usage
func WriteLight() *Profile {
	return New().
		WithMaxOpenConns(1). // SQLite single writer constraint
		WithMaxIdleConns(1).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-51200). // 50MB
		WithPageSize(4096).
		WithMMapSize(134217728). // 128MB
		WithBusyTimeout(5000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumIncremental).
		WithRecursiveTriggers(true)
}

// WriteBalanced provides good write performance suitable for most applications
func WriteBalanced() *Profile {
	return New().
		WithMaxOpenConns(1). // SQLite single writer constraint
		WithMaxIdleConns(1).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-102400). // 100MB
		WithPageSize(8192).
		WithMMapSize(268435456). // 256MB
		WithBusyTimeout(5000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumNone).
		WithRecursiveTriggers(true)
}

// WriteHeavy maximises write throughput for high-volume write scenarios
func WriteHeavy() *Profile {
	return New().
		WithMaxOpenConns(1). // SQLite single writer constraint
		WithMaxIdleConns(1).
		WithConnMaxLifetime(time.Hour).
		WithJournalMode(JournalWal).
		WithSynchronous(SyncNormal).
		WithForeignKeys(true).
		WithCacheSize(-204800). // 200MB
		WithPageSize(8192).
		WithMMapSize(536870912). // 512MB
		WithBusyTimeout(10000).
		WithTempStore(TempStoreMemory).
		WithAutoVacuum(AutoVacuumNone).
		WithRecursiveTriggers(true)
}
