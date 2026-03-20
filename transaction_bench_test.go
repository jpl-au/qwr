// transaction_bench_test.go benchmarks Transaction (declarative) and
// TransactionFunc (callback) execution paths against file-backed SQLite.
// Each benchmark inserts two rows within a transaction to measure the
// real cost of begin/commit with disk I/O. Exec routes through the
// serialised worker queue; Write bypasses it for direct execution.
//
// Shared benchmark helpers (benchMgr, setupBenchTable) live here because
// this file was the first benchmark file in the package.

package qwr

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/jpl-au/qwr/profile"
)

// Declarative transaction benchmarks - pre-built statement list.

func BenchmarkTransactionExec(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Transaction().
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Exec()
	}
}

func BenchmarkTransactionWrite(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.Transaction().
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Write()
	}
}

func BenchmarkTransactionExecWithContext(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	for b.Loop() {
		mgr.Transaction().
			WithContext(ctx).
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Exec()
	}
}

// Callback transaction benchmarks - caller receives *sql.Tx for
// interleaved reads and writes within qwr's serialised writer.

func BenchmarkTransactionFuncExec(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).Exec()
	}
}

func BenchmarkTransactionFuncWrite(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).Write()
	}
}

func BenchmarkTransactionFuncExecWithContext(b *testing.B) {
	mgr := benchMgr(b, nil, nil)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).WithContext(ctx).Exec()
	}
}

// benchMgr creates a file-backed manager for benchmarks. Pass nil for
// either profile to use defaults. All benchmarks use real disk I/O to
// measure actual SQLite write costs including WAL and fsync overhead.
func benchMgr(b *testing.B, r *profile.Profile, w *profile.Profile) *Manager {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.db")
	builder := New(path)
	if r != nil {
		builder = builder.Reader(r)
	}
	if w != nil {
		builder = builder.Writer(w)
	}
	mgr, err := builder.Open()
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	return mgr
}

func setupBenchTable(b *testing.B, mgr *Manager) {
	b.Helper()
	_, err := mgr.Query(
		"CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, name TEXT)",
	).Execute()
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}
}
