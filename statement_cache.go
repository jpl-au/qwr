package qwr

import (
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
)

// SlowQuery represents a slow statement preparation for metrics tracking
type SlowQuery struct {
	Query    string        // The SQL query that was slow to prepare
	Duration time.Duration // How long the preparation took
	When     time.Time     // When the slow preparation occurred
}

// StmtCache is a high-performance prepared statement cache using ristretto
// for optimal concurrent access with LRU eviction. Stores SQL prepared
// statements keyed by their query string.
type StmtCache struct {
	cache   *ristretto.Cache // LRU cache with eviction
	metrics *StmtCacheMetrics
	options Options
	closed  atomic.Bool
}

// NewStmtCache creates a new prepared statement cache with LRU eviction.
// maxSize controls the maximum number of statements to cache (0 = default 1000).
func NewStmtCache(metrics *StmtCacheMetrics, options Options) (*StmtCache, error) {
	maxSize := options.StmtCacheMaxSize
	if maxSize <= 0 {
		maxSize = 1000 // Default max size
	}

	cache := &StmtCache{
		metrics: metrics,
		options: options,
	}

	// Create ristretto cache with eviction callback
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(maxSize * 10), // 10x max size for frequency tracking
		MaxCost:     int64(maxSize),      // Each statement has cost of 1
		BufferItems: 64,                  // Internal buffer size
		OnEvict: func(item *ristretto.Item) {
			// Close the statement when evicted
			if stmt, ok := item.Value.(*sql.Stmt); ok {
				stmt.Close()
			}
			if cache.metrics != nil {
				cache.metrics.Size.Add(-1)
				cache.metrics.Evictions.Add(1)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	cache.cache = c

	if metrics != nil {
		metrics.MaxSize = int64(maxSize)
	}

	return cache, nil
}

// Get retrieves a prepared statement from cache or prepares it if not found
func (c *StmtCache) Get(db *sql.DB, query string) (*sql.Stmt, error) {
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}

	// Fast path: check cache
	if value, found := c.cache.Get(query); found {
		if c.metrics != nil {
			c.metrics.recordCacheHit()
		}
		return value.(*sql.Stmt), nil
	}

	// Cache miss - prepare the statement
	if c.metrics != nil {
		c.metrics.recordCacheMiss()
	}

	start := time.Now()
	stmt, err := db.Prepare(query)
	prepDuration := time.Since(start)

	if err != nil {
		if c.metrics != nil {
			c.metrics.recordPrepError()
		}
		return nil, err
	}

	if c.metrics != nil {
		c.metrics.recordPrepTime(prepDuration)

		if prepDuration > c.options.StmtCacheSlowThreshold && c.shouldSample() {
			c.recordSlowQuery(query, prepDuration)
		}
	}

	// Store in cache (cost of 1 per statement)
	if c.cache.Set(query, stmt, 1) {
		// Wait for value to be stored (ristretto is async)
		c.cache.Wait()
		if c.metrics != nil {
			c.metrics.Size.Add(1)
		}
	}

	return stmt, nil
}

// Clear closes all cached statements and clears the cache.
// The cache remains usable after Clear() - new statements will be prepared on demand.
func (c *StmtCache) Clear() {
	c.cache.Clear()
	if c.metrics != nil {
		c.metrics.Size.Store(0)
	}
}

// Close gracefully shuts down the cache.
// After Close(), the cache cannot be used - Get() will return ErrCacheClosed.
func (c *StmtCache) Close() {
	c.closed.Store(true)
	c.cache.Close()
	if c.metrics != nil {
		c.metrics.Size.Store(0)
	}
}

// shouldSample determines if this operation should be sampled for detailed metrics
func (c *StmtCache) shouldSample() bool {
	if c.metrics == nil {
		return false
	}
	return c.metrics.Hits.Load()%c.options.StmtCacheSampleRate == 0
}

// recordSlowQuery records a slow statement preparation
func (c *StmtCache) recordSlowQuery(query string, duration time.Duration) {
	if c.metrics != nil {
		c.metrics.recordSlowQuery(query, duration)
	}
}
