// query_bench_test.go benchmarks all QueryBuilder execution paths against
// file-backed SQLite to measure real disk I/O costs. Covers direct writes,
// queued writes, async/batch submission, and all read variants. Prepared
// statement and context variants isolate the overhead of each feature.

package qwr

import (
	"context"
	"database/sql"
	"testing"
)

// Write benchmarks measure the cost of a single INSERT through each
// execution path. The difference between Write (direct) and Execute
// (queued) shows the serialiser channel overhead. Async and Batch
// measure submission cost only — actual write happens asynchronously.

func BenchmarkQueryWrite(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Write()
	}
}

func BenchmarkQueryWritePrepared(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Prepared().Write()
	}
}

func BenchmarkQueryWriteWithContext(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").WithContext(ctx).Write()
	}
}

func BenchmarkQueryExecute(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Execute()
	}
}

func BenchmarkQueryExecutePrepared(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Prepared().Execute()
	}
}

func BenchmarkQueryExecuteWithContext(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").WithContext(ctx).Execute()
	}
}

// Async measures submission cost only — the write is dispatched to the
// worker and completes later. Errors surface via the error queue.
func BenchmarkQueryAsync(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Async()
	}
}

// Batch measures the cost of appending to the batch collector. The actual
// write is deferred until the batch flushes by size or timeout.
func BenchmarkQueryBatch(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Batch()
	}
}

// Read benchmarks measure query cost against a pre-seeded table.
// The difference between plain and prepared variants shows the
// parsing overhead that the statement cache eliminates.

func BenchmarkQueryRead(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkQueryReadPrepared(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").Prepared().Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkQueryReadWithContext(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	ctx := context.Background()
	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").WithContext(ctx).Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

// ReadClose includes row iteration cost — measures the full read cycle
// that most application code actually performs.
func BenchmarkQueryReadClose(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	for b.Loop() {
		mgr.Query("SELECT id, name FROM bench").ReadClose(func(rows *sql.Rows) error {
			for rows.Next() {
				var id int
				var name string
				rows.Scan(&id, &name)
			}
			return nil
		})
	}
}

func BenchmarkQueryReadRow(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 10)

	for b.Loop() {
		row, err := mgr.Query("SELECT name FROM bench WHERE id = ?", 1).ReadRow()
		if err != nil {
			b.Fatal(err)
		}
		var name string
		row.Scan(&name)
	}
}

func BenchmarkQueryReadRowPrepared(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 10)

	for b.Loop() {
		row, err := mgr.Query("SELECT name FROM bench WHERE id = ?", 1).Prepared().ReadRow()
		if err != nil {
			b.Fatal(err)
		}
		var name string
		row.Scan(&name)
	}
}

func seedBenchRows(b *testing.B, mgr *Manager, n int) {
	b.Helper()
	for i := range n {
		_, err := mgr.Query("INSERT INTO bench (name) VALUES (?)", i).Write()
		if err != nil {
			b.Fatalf("failed to seed row %d: %v", i, err)
		}
	}
}
