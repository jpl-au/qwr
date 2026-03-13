package qwr

import (
	"context"
	"database/sql"
	"testing"
)

func BenchmarkTransactionExec(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Transaction().
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Exec()
	}
}

func BenchmarkTransactionWrite(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.Transaction().
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Write()
	}
}

func BenchmarkTransactionExecWithContext(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		mgr.Transaction().
			WithContext(ctx).
			Add("INSERT INTO bench (name) VALUES (?)", "alice").
			Add("INSERT INTO bench (name) VALUES (?)", "bob").
			Exec()
	}
}

func BenchmarkTransactionFuncExec(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).Exec()
	}
}

func BenchmarkTransactionFuncWrite(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	b.ResetTimer()
	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).Write()
	}
}

func BenchmarkTransactionFuncExecWithContext(b *testing.B) {
	mgr := newBenchMgr(b)
	defer mgr.Close()
	setupBenchTable(b, mgr)

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
			tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
			return nil, nil
		}).WithContext(ctx).Exec()
	}
}

func newBenchMgr(b *testing.B) *Manager {
	b.Helper()
	mgr, err := New("file::memory:?cache=shared", DefaultOptions).Open()
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
