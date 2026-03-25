// Package profile provides pre-configured database profiles for different workload types.
// Each profile optimises SQLite settings for specific use cases like read-heavy operations,
// write-intensive workloads, or mixed scenarios.
//
// Pre-configured profiles:
//
//   - [ReadLight], [ReadBalanced], [ReadHeavy] for reader connection pools
//   - [WriteLight], [WriteBalanced], [WriteHeavy] for the serialised writer
//   - [Attached] for per-schema PRAGMAs on attached databases
//
// Use [New] to build a custom profile from scratch.
package profile

import (
	"database/sql"
	"fmt"
	"maps"
	"regexp"
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

// Pragma identifies a SQLite PRAGMA by name.
type Pragma string

// Known pragma names for use with the typed With* methods. Use [Profile.Custom]
// to set pragmas not listed here.
const (
	PragmaJournalMode       Pragma = "journal_mode"
	PragmaSynchronous       Pragma = "synchronous"
	PragmaForeignKeys       Pragma = "foreign_keys"
	PragmaCacheSize         Pragma = "cache_size"
	PragmaMMapSize          Pragma = "mmap_size"
	PragmaTempStore         Pragma = "temp_store"
	PragmaAutoVacuum        Pragma = "auto_vacuum"
	PragmaPageSize          Pragma = "page_size"
	PragmaBusyTimeout       Pragma = "busy_timeout"
	PragmaLockingMode       Pragma = "locking_mode"
	PragmaRecursiveTriggers Pragma = "recursive_triggers"
	PragmaSecureDelete      Pragma = "secure_delete"
	PragmaQueryOnly         Pragma = "query_only"
)

// validPragmaName matches a valid SQLite pragma identifier.
var validPragmaName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Profile holds configuration for database connections
type Profile struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	pragmas         map[Pragma]any
}

// New creates a new empty profile with initialised pragmas map
func New() *Profile {
	return &Profile{
		pragmas: make(map[Pragma]any),
	}
}

// Apply configures a database connection with this profile.
// Sets pool parameters and runs PRAGMAs via db.Exec. Note that PRAGMAs
// applied this way only reach one connection in the pool. For reliable
// per-connection PRAGMAs, prefer DSNPragmas with a custom connector.
func (p *Profile) Apply(db *sql.DB) error {
	p.ApplyPool(db)
	for name, value := range p.pragmas {
		if !validPragmaName.MatchString(string(name)) {
			return fmt.Errorf("pragma name %q is not a valid identifier", name)
		}
		query := fmt.Sprintf("PRAGMA %s = %v", name, value)
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

// ApplyPool sets connection pool parameters on the database handle
// without running any PRAGMAs. Use this with sql.OpenDB when PRAGMAs
// are handled by the DSN or a custom connector.
func (p *Profile) ApplyPool(db *sql.DB) {
	db.SetMaxOpenConns(p.MaxOpenConns)
	db.SetMaxIdleConns(p.MaxIdleConns)
	if p.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(p.ConnMaxLifetime)
	}
}

// DSNPragmas returns the profile's PRAGMAs formatted for the modernc.org/sqlite
// DSN query string. Each entry is a key=value pair like "_pragma=journal_mode(WAL)".
// The driver applies these on every new connection, solving the per-connection
// PRAGMA problem that Apply has with pooled connections.
func (p *Profile) DSNPragmas() []string {
	params := make([]string, 0, len(p.pragmas))
	for name, value := range p.pragmas {
		if !validPragmaName.MatchString(string(name)) {
			continue
		}
		params = append(params, fmt.Sprintf("_pragma=%s(%v)", name, value))
	}
	return params
}

// SchemaStatements returns the profile's PRAGMAs as schema-qualified SQL
// statements for an attached database. For example, with schema "analytics",
// a cache_size pragma becomes "PRAGMA analytics.cache_size = -30720".
func (p *Profile) SchemaStatements(schema string) []string {
	stmts := make([]string, 0, len(p.pragmas))
	for name, value := range p.pragmas {
		if !validPragmaName.MatchString(string(name)) {
			continue
		}
		stmts = append(stmts, fmt.Sprintf("PRAGMA %s.%s = %v", schema, name, value))
	}
	return stmts
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
	for name, value := range p.pragmas {
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
		pragmas:         make(map[Pragma]any, len(p.pragmas)),
	}
	maps.Copy(clone.pragmas, p.pragmas)
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
	p.pragmas[PragmaJournalMode] = string(mode)
	return p
}

// WithSynchronous sets the synchronous pragma
func (p *Profile) WithSynchronous(mode SynchronousMode) *Profile {
	p.pragmas[PragmaSynchronous] = string(mode)
	return p
}

// WithForeignKeys sets the foreign_keys pragma
func (p *Profile) WithForeignKeys(enabled bool) *Profile {
	if enabled {
		p.pragmas[PragmaForeignKeys] = "ON"
	} else {
		p.pragmas[PragmaForeignKeys] = "OFF"
	}
	return p
}

// WithCacheSize sets the cache_size pragma for SQLite's page cache.
// Positive values specify cache size in KiB, negative values specify number of pages.
// Larger cache improves read performance but uses more memory.
// Example: -102400 = 100MB cache (recommended for most applications)
func (p *Profile) WithCacheSize(kibibytes int) *Profile {
	p.pragmas[PragmaCacheSize] = kibibytes
	return p
}

// WithMMapSize sets the mmap_size pragma for memory-mapped I/O.
// Specifies the maximum size in bytes that SQLite will use for memory-mapped files.
// Larger values can improve performance for read-heavy workloads.
// Common values: 268435456 (256MB), 536870912 (512MB), 0 (disable mmap)
func (p *Profile) WithMMapSize(bytes int64) *Profile {
	p.pragmas[PragmaMMapSize] = bytes
	return p
}

// WithTempStore sets the temp_store pragma
func (p *Profile) WithTempStore(store TempStore) *Profile {
	p.pragmas[PragmaTempStore] = string(store)
	return p
}

// WithAutoVacuum sets the auto_vacuum pragma
func (p *Profile) WithAutoVacuum(mode AutoVacuum) *Profile {
	p.pragmas[PragmaAutoVacuum] = string(mode)
	return p
}

// WithPageSize sets the page_size pragma for database pages.
// Must be a power of 2 between 512 and 65536 bytes.
// Larger pages reduce overhead for large records but use more memory.
// Common values: 4096 (default), 8192 (good for write-heavy), 16384 (large records)
func (p *Profile) WithPageSize(bytes int) *Profile {
	p.pragmas[PragmaPageSize] = bytes
	return p
}

// WithBusyTimeout sets the busy_timeout pragma for database lock retries.
// Specifies how long (in milliseconds) to wait for locks before returning SQLITE_BUSY.
// Higher values reduce lock contention errors but may increase latency.
// Common values: 5000 (5 seconds, default), 10000 (high contention), 1000 (low latency)
func (p *Profile) WithBusyTimeout(ms int) *Profile {
	p.pragmas[PragmaBusyTimeout] = ms
	return p
}

// WithLockingMode sets the locking_mode pragma
func (p *Profile) WithLockingMode(mode LockingMode) *Profile {
	p.pragmas[PragmaLockingMode] = string(mode)
	return p
}

// WithRecursiveTriggers sets the recursive_triggers pragma
func (p *Profile) WithRecursiveTriggers(enabled bool) *Profile {
	if enabled {
		p.pragmas[PragmaRecursiveTriggers] = "ON"
	} else {
		p.pragmas[PragmaRecursiveTriggers] = "OFF"
	}
	return p
}

// WithSecureDelete sets the secure_delete pragma
func (p *Profile) WithSecureDelete(enabled bool) *Profile {
	if enabled {
		p.pragmas[PragmaSecureDelete] = "ON"
	} else {
		p.pragmas[PragmaSecureDelete] = "OFF"
	}
	return p
}

// WithQueryOnly sets the query_only pragma
func (p *Profile) WithQueryOnly(enabled bool) *Profile {
	if enabled {
		p.pragmas[PragmaQueryOnly] = "ON"
	} else {
		p.pragmas[PragmaQueryOnly] = "OFF"
	}
	return p
}

// Custom sets a PRAGMA not covered by the typed With* methods. The name must
// be a valid SQLite identifier. Invalid names are rejected by the consumption
// methods (Apply, DSNPragmas, SchemaStatements).
//
// Example:
//
//	profile.New().Custom(profile.Pragma("wal_autocheckpoint"), 1000)
func (p *Profile) Custom(name Pragma, value any) *Profile {
	p.pragmas[name] = value
	return p
}

// ReadLight targets low-memory environments or applications with infrequent
// reads. 5 connections and a 30MB cache keep the footprint small at the
// cost of reduced concurrency under load.
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

// ReadBalanced is the recommended default for most applications. 10
// connections with a 75MB cache handles moderate concurrency without
// excessive memory use.
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

// ReadHeavy targets high-concurrency workloads where many goroutines
// read simultaneously. 25 connections and a 150MB cache reduce contention
// at the cost of higher memory use. Only worthwhile when read concurrency
// is the bottleneck.
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

// WriteLight targets low-memory environments or applications with
// infrequent writes. Uses 4096 page size and incremental auto-vacuum,
// trading write throughput for a smaller on-disk footprint.
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

// WriteBalanced is the recommended default for most applications. Uses
// 8192 page size for better write throughput and disables auto-vacuum
// to avoid write amplification. The larger page size is the primary
// performance differentiator over WriteLight.
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

// WriteHeavy targets high-volume write workloads. Uses the same 8192
// page size as Balanced but doubles the cache and mmap sizes, and
// increases busy_timeout. The extra memory helps when the working set
// exceeds Balanced's cache - otherwise performance is identical.
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

// Attached creates a profile for an attached database. Only PRAGMA settings
// are meaningful for attached databases - connection pool parameters
// (MaxOpenConns, MaxIdleConns, ConnMaxLifetime) are ignored because attached
// databases share the main connection pool.
//
// Use the With* methods to configure per-schema PRAGMAs:
//
//	profile.Attached().
//		WithJournalMode(profile.JournalWal).
//		WithCacheSize(-30720)
func Attached() *Profile {
	return New()
}
