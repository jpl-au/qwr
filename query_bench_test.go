package qwr

import (
	"context"
	"database/sql"
	"testing"
)

// --- Write path benchmarks ---

func BenchmarkQueryWrite(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Write()
	}
}

func BenchmarkQueryWritePrepared(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Prepared().Write()
	}
}

func BenchmarkQueryWriteWithContext(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").WithContext(ctx).Write()
	}
}

func BenchmarkQueryExecute(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Execute()
	}
}

func BenchmarkQueryExecutePrepared(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Prepared().Execute()
	}
}

func BenchmarkQueryExecuteWithContext(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").WithContext(ctx).Execute()
	}
}

func BenchmarkQueryAsync(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Async()
	}
}

func BenchmarkQueryBatch(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Batch()
	}
}

// --- Read path benchmarks ---

func BenchmarkQueryRead(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	b.ResetTimer()
	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkQueryReadPrepared(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	b.ResetTimer()
	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").Prepared().Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkQueryReadWithContext(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := mgr.Query("SELECT id, name FROM bench").WithContext(ctx).Read()
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkQueryReadClose(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 100)

	b.ResetTimer()
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
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 10)

	b.ResetTimer()
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
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)
	seedBenchRows(b, mgr, 10)

	b.ResetTimer()
	for b.Loop() {
		row, err := mgr.Query("SELECT name FROM bench WHERE id = ?", 1).Prepared().ReadRow()
		if err != nil {
			b.Fatal(err)
		}
		var name string
		row.Scan(&name)
	}
}

// seedBenchRows inserts n rows into the bench table for read benchmarks.
func seedBenchRows(b *testing.B, mgr *Manager, n int) {
	b.Helper()
	for i := range n {
		_, err := mgr.Query("INSERT INTO bench (name) VALUES (?)", i).Write()
		if err != nil {
			b.Fatalf("failed to seed row %d: %v", i, err)
		}
	}
}
