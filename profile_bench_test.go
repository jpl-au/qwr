// profile_bench_test.go compares read/write performance across Light,
// Balanced, and Heavy profile pairs. Uses file-backed SQLite so that
// differences in page_size, cache_size, and mmap_size produce
// observable effects on real disk I/O rather than being masked by
// in-memory operation.

package qwr

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jpl-au/qwr/profile"
)

func BenchmarkProfiles(b *testing.B) {
	profiles := []struct {
		name   string
		reader *profile.Profile
		writer *profile.Profile
	}{
		{"Light", profile.ReadLight(), profile.WriteLight()},
		{"Balanced", profile.ReadBalanced(), profile.WriteBalanced()},
		{"Heavy", profile.ReadHeavy(), profile.WriteHeavy()},
	}

	for _, p := range profiles {
		b.Run(p.name, func(b *testing.B) {
			mgr := benchMgr(b, p.reader, p.writer)
			defer mgr.Close()
			setupBenchTable(b, mgr)

			b.Run("Write", func(b *testing.B) {
				for b.Loop() {
					mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Write()
				}
			})

			b.Run("Execute", func(b *testing.B) {
				for b.Loop() {
					mgr.Query("INSERT INTO bench (name) VALUES (?)", "alice").Execute()
				}
			})

			b.Run("Transaction", func(b *testing.B) {
				for b.Loop() {
					mgr.Transaction().
						Add("INSERT INTO bench (name) VALUES (?)", "alice").
						Add("INSERT INTO bench (name) VALUES (?)", "bob").
						Write()
				}
			})

			b.Run("TransactionFunc", func(b *testing.B) {
				for b.Loop() {
					mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
						tx.Exec("INSERT INTO bench (name) VALUES (?)", "alice")
						tx.Exec("INSERT INTO bench (name) VALUES (?)", "bob")
						return nil, nil
					}).Write()
				}
			})

			// Seed rows so read benchmarks operate on a non-trivial dataset.
			for i := range 500 {
				mgr.Query("INSERT INTO bench (name) VALUES (?)", i).Write()
			}

			b.Run("ReadRow", func(b *testing.B) {
				for b.Loop() {
					row, _ := mgr.Query("SELECT name FROM bench WHERE id = ?", 1).ReadRow()
					var name string
					row.Scan(&name)
				}
			})

			b.Run("Read", func(b *testing.B) {
				for b.Loop() {
					rows, _ := mgr.Query("SELECT id, name FROM bench LIMIT 100").Read()
					rows.Close()
				}
			})

			b.Run("ReadClose", func(b *testing.B) {
				for b.Loop() {
					mgr.Query("SELECT id, name FROM bench LIMIT 100").ReadClose(func(rows *sql.Rows) error {
						for rows.Next() {
							var id int
							var name string
							rows.Scan(&id, &name)
						}
						return nil
					})
				}
			})

			// MixedWorkload simulates a common pattern: read-then-write
			// within a single transaction, as used by task positioning
			// and document versioning in llmd.
			b.Run("MixedWorkload", func(b *testing.B) {
				ctx := context.Background()
				for b.Loop() {
					mgr.TransactionFunc(func(tx *sql.Tx) (any, error) {
						var maxID int
						tx.QueryRow("SELECT COALESCE(MAX(id), 0) FROM bench").Scan(&maxID)
						tx.Exec("INSERT INTO bench (name) VALUES (?)", "mixed")
						return nil, nil
					}).WithContext(ctx).Write()
				}
			})
		})
	}
}
