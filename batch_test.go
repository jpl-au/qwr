package qwr

import (
	"testing"
	"testing/synctest"
	"time"
)

// TestBatchTimeout verifies batches flush after timeout using synctest.
func TestBatchTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := DefaultOptions
		opts.BatchSize = 10 // Large batch size
		opts.BatchTimeout = 100 * time.Millisecond

		mgr := newTestMgr(t, opts)
		defer mgr.Close()

		setupTable(t, mgr)

		// Add a single query (won't reach batch size)
		_, err := mgr.Query(
			"INSERT INTO users (name) VALUES (?)",
			"Alice",
		).Batch()
		if err != nil {
			t.Fatalf("batch add failed: %v", err)
		}

		// Wait for batch timeout to trigger
		time.Sleep(opts.BatchTimeout * 2)
		synctest.Wait()

		// Verify the batch was flushed
		row, err := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
		if err != nil {
			t.Fatalf("count query failed: %v", err)
		}

		var count int
		if err := row.Scan(&count); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		if count != 1 {
			t.Errorf("got %d rows, want 1 (batch should have flushed)", count)
		}
	})
}

// TestBatchSize verifies batches flush when size is reached using synctest.
func TestBatchSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := DefaultOptions
		opts.BatchSize = 3
		opts.BatchTimeout = 10 * time.Second // Long timeout

		mgr := newTestMgr(t, opts)
		defer mgr.Close()

		setupTable(t, mgr)

		// Add exactly BatchSize queries
		for i := range opts.BatchSize {
			_, err := mgr.Query(
				"INSERT INTO users (name) VALUES (?)",
				"User",
			).Batch()
			if err != nil {
				t.Fatalf("batch add %d failed: %v", i, err)
			}
		}

		// Wait for all goroutines to settle (deterministic, no flaky sleep)
		synctest.Wait()

		// Verify all rows were written
		row, err := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
		if err != nil {
			t.Fatalf("count query failed: %v", err)
		}

		var count int
		if err := row.Scan(&count); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		if count != opts.BatchSize {
			t.Errorf("got %d rows, want %d (batch should have flushed)", count, opts.BatchSize)
		}
	})
}
