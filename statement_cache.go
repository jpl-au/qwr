package qwr

import (
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
)

// StmtCache is a high-performance prepared statement cache using ristretto
// for optimal concurrent access with LRU eviction. Stores SQL prepared
// statements keyed by their query string.
type StmtCache struct {
	cache   *ristretto.Cache // LRU cache with eviction
	events  *EventBus
	options Options
	closed  atomic.Bool
}

// NewStmtCache creates a new prepared statement cache with LRU eviction.
// maxSize controls the maximum number of statements to cache (0 = default 1000).
func NewStmtCache(events *EventBus, options Options) (*StmtCache, error) {
	maxSize := options.StmtCacheMaxSize
	if maxSize <= 0 {
		maxSize = 1000 // Default max size
	}

	cache := &StmtCache{
		events:  events,
		options: options,
	}

	// Create ristretto cache with eviction and rejection callbacks.
	// OnReject is critical: when TinyLFU rejects a Set(), the statement
	// never enters the cache, so the eviction callback never fires.
	// Without OnReject, rejected statements leak — database/sql does not
	// set a GC finaliser on *sql.Stmt.
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(maxSize * 10), // 10x max size for frequency tracking
		MaxCost:     int64(maxSize),      // Each statement has cost of 1
		BufferItems: 64,                  // Internal buffer size
		OnEvict: func(item *ristretto.Item) {
			if stmt, ok := item.Value.(*sql.Stmt); ok {
				stmt.Close()
			}
			cache.events.Emit(Event{Type: EventCacheEvicted})
		},
		OnReject: func(item *ristretto.Item) {
			if stmt, ok := item.Value.(*sql.Stmt); ok {
				stmt.Close()
			}
		},
	})
	if err != nil {
		return nil, err
	}

	cache.cache = c
	return cache, nil
}

// Get retrieves a prepared statement from cache or prepares it if not found
func (c *StmtCache) Get(db *sql.DB, query string) (*sql.Stmt, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}

	// Fast path: check cache
	if value, found := c.cache.Get(query); found {
		c.events.Emit(Event{Type: EventCacheHit, CacheQuery: query})
		return value.(*sql.Stmt), nil
	}

	// Cache miss - prepare the statement
	start := time.Now()
	stmt, err := db.Prepare(query)
	prepDuration := time.Since(start)

	if err != nil {
		c.events.Emit(Event{Type: EventCachePrepError, CacheQuery: query, Err: err})
		return nil, err
	}

	c.events.Emit(Event{Type: EventCacheMiss, CacheQuery: query, CachePrepTime: prepDuration})

	// Store in cache (cost of 1 per statement). Ristretto processes
	// Set() asynchronously — a subsequent Get() may miss and prepare
	// again, but that's cheaper than blocking here with Wait().
	c.cache.Set(query, stmt, 1)

	return stmt, nil
}

// Clear closes all cached statements and clears the cache.
// The cache remains usable after Clear() - new statements will be prepared on demand.
func (c *StmtCache) Clear() {
	c.cache.Clear()
}

// Close gracefully shuts down the cache.
// After Close(), the cache cannot be used - Get() will return ErrCacheClosed.
func (c *StmtCache) Close() {
	c.closed.Store(true)
	c.cache.Close()
}
