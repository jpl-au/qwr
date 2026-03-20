package qwr

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestTransactionFuncCommit(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	result, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		_, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
		if err != nil {
			return nil, err
		}

		var count int
		if err := tx.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			return nil, err
		}
		return count, nil
	}).Exec()

	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if result.Value.(int) != 1 {
		t.Errorf("value = %v, want 1", result.Value)
	}

	// Verify the row persisted.
	row, _ := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
	var count int
	row.Scan(&count)
	if count != 1 {
		t.Errorf("got %d rows, want 1", count)
	}
}

func TestTransactionFuncRollback(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	sentinel := errors.New("intentional error")
	_, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		tx.Exec("INSERT INTO users (name) VALUES (?)", "Bob")
		return nil, sentinel
	}).Exec()

	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}

	// Row should not exist - transaction was rolled back.
	row, _ := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
	var count int
	row.Scan(&count)
	if count != 0 {
		t.Errorf("got %d rows, want 0 (rollback expected)", count)
	}
}

func TestTransactionFuncInterleavedReadWrite(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Seed some data.
	mgr.Query("INSERT INTO users (name) VALUES (?)", "seed").Execute()

	// The core use case: read within a transaction, use the result for a write.
	result, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		var maxID int
		if err := tx.QueryRow("SELECT COALESCE(MAX(id), 0) FROM users").Scan(&maxID); err != nil {
			return nil, err
		}

		_, err := tx.Exec("INSERT INTO users (id, name) VALUES (?, ?)", maxID+100, "derived")
		if err != nil {
			return nil, err
		}
		return maxID + 100, nil
	}).Exec()

	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}

	insertedID := result.Value.(int)
	if insertedID < 100 {
		t.Errorf("inserted id = %d, want >= 100", insertedID)
	}

	// Verify the derived row exists.
	row, _ := mgr.Query("SELECT name FROM users WHERE id = ?", insertedID).ReadRow()
	var name string
	row.Scan(&name)
	if name != "derived" {
		t.Errorf("name = %q, want derived", name)
	}
}

func TestTransactionFuncWrite(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Write() bypasses the queue - same callback, direct execution.
	result, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		_, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "direct")
		return "ok", err
	}).Write()

	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if result.Value.(string) != "ok" {
		t.Errorf("value = %v, want ok", result.Value)
	}
}

func TestTransactionFuncWithContext(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	ctx := context.Background()
	result, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		_, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "ctx")
		return "done", err
	}).WithContext(ctx).Exec()

	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if result.Value.(string) != "done" {
		t.Errorf("value = %v, want done", result.Value)
	}
}

func TestTransactionFuncPanicWrite(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Panic inside the callback is converted to an error.
	// Transaction should be rolled back, row should not exist.
	_, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		tx.Exec("INSERT INTO users (name) VALUES (?)", "panic")
		panic("boom")
	}).Write()

	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("error = %q, want panic message", err)
	}

	// Row should not exist - transaction was rolled back.
	row, _ := mgr.Query("SELECT COUNT(*) FROM users").ReadRow()
	var count int
	row.Scan(&count)
	if count != 0 {
		t.Errorf("got %d rows, want 0 (rollback expected)", count)
	}
}

func TestTransactionFuncPanicExec(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	// Panic during Exec (queued execution) must not kill the worker.
	_, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		tx.Exec("INSERT INTO users (name) VALUES (?)", "panic")
		panic("boom")
	}).Exec()

	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("error = %q, want panic message", err)
	}

	// The worker must still be alive - verify by executing another write.
	_, err = mgr.Query("INSERT INTO users (name) VALUES (?)", "after-panic").Execute()
	if err != nil {
		t.Fatalf("worker died: subsequent Execute failed: %v", err)
	}

	row, _ := mgr.Query("SELECT COUNT(*) FROM users WHERE name = ?", "after-panic").ReadRow()
	var count int
	row.Scan(&count)
	if count != 1 {
		t.Errorf("got %d rows, want 1 (worker should still be alive)", count)
	}
}

func TestTransactionFuncResult(t *testing.T) {
	mgr := newTestMgr(t, DefaultOptions)
	defer mgr.Close()

	setupTable(t, mgr)

	result, err := mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
		return 42, nil
	}).Exec()

	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if result.ID() == 0 {
		t.Error("expected non-zero ID")
	}
	if result.Duration() == 0 {
		t.Error("expected non-zero duration")
	}
	if result.Error() != nil {
		t.Errorf("unexpected error: %v", result.Error())
	}
}
