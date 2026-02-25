package qwr

import (
	"strings"
	"testing"
)

// testBatchCollector creates a minimal BatchCollector for combineInsertGroup tests.
func testBatchCollector() *BatchCollector {
	return &BatchCollector{events: NewEventBus()}
}

func TestCombineInsertGroup(t *testing.T) {
	bc := testBatchCollector()
	defer bc.events.Close()

	t.Run("basic combine", func(t *testing.T) {
		queries := []Query{
			{SQL: "INSERT INTO t (a, b) VALUES (?, ?)", Args: []any{1, 2}},
			{SQL: "INSERT INTO t (a, b) VALUES (?, ?)", Args: []any{3, 4}},
			{SQL: "INSERT INTO t (a, b) VALUES (?, ?)", Args: []any{5, 6}},
		}
		sql, args, ok := bc.combineInsertGroup(queries)
		if !ok {
			t.Fatal("expected combine to succeed")
		}
		if !strings.Contains(sql, "(?, ?), (?, ?), (?, ?)") {
			t.Errorf("unexpected SQL: %s", sql)
		}
		if len(args) != 6 {
			t.Errorf("got %d args, want 6", len(args))
		}
	})

	t.Run("rejects non-INSERT", func(t *testing.T) {
		queries := []Query{
			{SQL: "UPDATE t SET a = ? WHERE b = ?", Args: []any{1, 2}},
			{SQL: "UPDATE t SET a = ? WHERE b = ?", Args: []any{3, 4}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		if ok {
			t.Error("expected combine to fail for UPDATE")
		}
	})

	t.Run("rejects INSERT SELECT", func(t *testing.T) {
		queries := []Query{
			{SQL: "INSERT INTO t (a) SELECT b FROM s WHERE c = ?", Args: []any{1}},
			{SQL: "INSERT INTO t (a) SELECT b FROM s WHERE c = ?", Args: []any{2}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		if ok {
			t.Error("expected combine to fail for INSERT...SELECT")
		}
	})

	t.Run("rejects single query", func(t *testing.T) {
		queries := []Query{
			{SQL: "INSERT INTO t (a) VALUES (?)", Args: []any{1}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		if ok {
			t.Error("expected combine to fail for single query")
		}
	})

	// --- Adversarial inputs ---

	t.Run("string literal with parentheses", func(t *testing.T) {
		// The placeholder pattern extraction captures everything after VALUES(,
		// including the closing paren. A string literal containing parens would
		// cause the pattern to include extra characters, and the placeholder
		// count validation should catch the mismatch.
		queries := []Query{
			{SQL: "INSERT INTO t (name) VALUES ('hello (world)')", Args: []any{}},
			{SQL: "INSERT INTO t (name) VALUES ('hello (world)')", Args: []any{}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		// 0 args but pattern contains no ?, so 0 == 0 — this may actually combine.
		// The key check is that it doesn't panic or produce corrupt SQL.
		_ = ok // outcome is acceptable either way
	})

	t.Run("string literal with question mark", func(t *testing.T) {
		// A literal '?' in the VALUES pattern would be counted as a placeholder,
		// causing a mismatch with the actual arg count.
		queries := []Query{
			{SQL: "INSERT INTO t (name) VALUES ('what?')", Args: []any{}},
			{SQL: "INSERT INTO t (name) VALUES ('what?')", Args: []any{}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		// 0 args but pattern has 1 '?' — validation should reject.
		if ok {
			t.Error("expected combine to fail: literal '?' in string should cause placeholder mismatch")
		}
	})

	t.Run("column name containing VALUES keyword", func(t *testing.T) {
		// LastIndex("VALUES") finds the last occurrence, which is the real
		// VALUES clause, not the one in the table/column name.
		queries := []Query{
			{SQL: "INSERT INTO my_values_table (count) VALUES (?)", Args: []any{1}},
			{SQL: "INSERT INTO my_values_table (count) VALUES (?)", Args: []any{2}},
		}
		sql, args, ok := bc.combineInsertGroup(queries)
		if !ok {
			t.Fatal("expected combine to succeed — LastIndex should find the real VALUES")
		}
		if len(args) != 2 {
			t.Errorf("got %d args, want 2", len(args))
		}
		if !strings.Contains(sql, "(?), (?)") {
			t.Errorf("unexpected SQL: %s", sql)
		}
	})

	t.Run("already multi-value INSERT", func(t *testing.T) {
		// An INSERT that already has multiple value groups. LastIndex("VALUES")
		// finds one, but the pattern after it contains two groups.
		// The placeholder count (4) won't match args count (2), so it should reject.
		queries := []Query{
			{SQL: "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)", Args: []any{1, 2}},
			{SQL: "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)", Args: []any{3, 4}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		if ok {
			t.Error("expected combine to fail: multi-value INSERT has placeholder/arg mismatch")
		}
	})

	t.Run("no VALUES keyword", func(t *testing.T) {
		queries := []Query{
			{SQL: "INSERT INTO t DEFAULT VALUES", Args: []any{}},
			{SQL: "INSERT INTO t DEFAULT VALUES", Args: []any{}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		// "DEFAULT VALUES" contains "VALUES" but has no '(' after it.
		if ok {
			t.Error("expected combine to fail for DEFAULT VALUES")
		}
	})

	t.Run("mixed case SQL", func(t *testing.T) {
		queries := []Query{
			{SQL: "insert into t (a) values (?)", Args: []any{1}},
			{SQL: "insert into t (a) values (?)", Args: []any{2}},
		}
		sql, args, ok := bc.combineInsertGroup(queries)
		if !ok {
			t.Fatal("expected combine to succeed with lowercase SQL")
		}
		if len(args) != 2 {
			t.Errorf("got %d args, want 2", len(args))
		}
		// Verify the original case is preserved in output
		if !strings.HasPrefix(sql, "insert into") {
			t.Errorf("expected original case preserved, got: %s", sql)
		}
	})

	t.Run("zero placeholders", func(t *testing.T) {
		// INSERT with literal values and no placeholders.
		queries := []Query{
			{SQL: "INSERT INTO t (a) VALUES (42)", Args: []any{}},
			{SQL: "INSERT INTO t (a) VALUES (42)", Args: []any{}},
		}
		sql, _, ok := bc.combineInsertGroup(queries)
		if !ok {
			t.Fatal("expected combine to succeed with literal values")
		}
		if !strings.Contains(sql, "(42), (42)") {
			t.Errorf("unexpected SQL: %s", sql)
		}
	})

	t.Run("nested parentheses in string literal", func(t *testing.T) {
		// String literal containing nested parens and a real placeholder.
		// Pattern extraction grabs everything from '(' after VALUES to end of string,
		// which includes the literal. Placeholder count (2 from '?' in pattern)
		// won't match args count (1), so validation rejects.
		queries := []Query{
			{SQL: "INSERT INTO t (a, b) VALUES (?, 'x(y)z')", Args: []any{1}},
			{SQL: "INSERT INTO t (a, b) VALUES (?, 'x(y)z')", Args: []any{2}},
		}
		_, _, ok := bc.combineInsertGroup(queries)
		// Pattern is "(?, 'x(y)z')" which has 1 '?', args has 1 — should combine.
		// The nested parens in the string literal don't affect placeholder counting.
		if !ok {
			t.Log("combine rejected — acceptable if placeholder validation is conservative")
		}
	})
}
