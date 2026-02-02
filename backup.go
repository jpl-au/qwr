package qwr

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/jpl-au/qwr/backup"
	"modernc.org/sqlite"
)

// backuper is the interface for accessing modernc.org/sqlite's backup methods.
type backuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// Backup creates a backup of the database to the specified destination path.
//
// Available methods:
//   - backup.Default: Uses backup API if available, falls back to Vacuum
//   - backup.API: Uses SQLite's online backup API (less locking)
//   - backup.Vacuum: Uses VACUUM INTO (creates optimized copy)
//
// The destination file must not already exist.
func (m *Manager) Backup(dest string, method backup.Method) error {
	if err := m.validateDest(dest); err != nil {
		return err
	}

	db := m.backupDB()
	if db == nil {
		return ErrWriterDisabled
	}

	switch method {
	case backup.API:
		return m.backupAPI(dest, db)
	case backup.Vacuum:
		return m.backupVacuum(dest, db)
	case backup.Default:
		err := m.backupAPI(dest, db)
		if err == ErrBackupDriverUnsupported {
			slog.Info("Backup API not supported by driver, falling back to VACUUM INTO", "db", m.Database())
			return m.backupVacuum(dest, db)
		}
		return err
	default:
		return fmt.Errorf("%w: %d", ErrBackupInvalidMethod, method)
	}
}

// validateDest checks if the backup destination is valid.
func (m *Manager) validateDest(dest string) error {
	if info, err := os.Stat(dest); err == nil {
		if info.Size() > 0 {
			return fmt.Errorf("%w: %s", ErrBackupDestinationExists, dest)
		}
	}
	return nil
}

// backupDB returns the database connection to use for backup.
// Prefers reader (less contention), falls back to writer.
func (m *Manager) backupDB() *sql.DB {
	if m.reader != nil {
		return m.reader
	}
	return m.writer
}

// backupAPI performs backup using SQLite's online backup API.
func (m *Manager) backupAPI(dest string, db *sql.DB) error {
	slog.Info("Starting database backup", "method", "backup_api", "dest", dest, "db", m.Database())

	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBackupConnection, err)
	}
	defer conn.Close()

	err = conn.Raw(func(dc any) error {
		bc, ok := dc.(backuper)
		if !ok {
			return ErrBackupDriverUnsupported
		}

		bck, err := bc.NewBackup(dest)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBackupInit, err)
		}

		for more := true; more; {
			more, err = bck.Step(-1)
			if err != nil {
				bck.Finish()
				return fmt.Errorf("%w: %v", ErrBackupStep, err)
			}
		}

		return bck.Finish()
	})

	if err != nil {
		if err != ErrBackupDriverUnsupported {
			slog.Error("Backup failed", "error", err, "dest", dest, "db", m.Database())
		}
		return err
	}

	slog.Info("Backup completed", "method", "backup_api", "dest", dest, "db", m.Database())
	return nil
}

// backupVacuum performs backup using SQLite's VACUUM INTO command.
func (m *Manager) backupVacuum(dest string, db *sql.DB) error {
	slog.Info("Starting database backup", "method", "vacuum_into", "dest", dest, "db", m.Database())

	_, err := db.Exec(fmt.Sprintf("VACUUM INTO '%s'", dest))
	if err != nil {
		slog.Error("Backup failed", "error", err, "dest", dest, "db", m.Database())
		return fmt.Errorf("%w: %v", ErrBackupFailed, err)
	}

	slog.Info("Backup completed", "method", "vacuum_into", "dest", dest, "db", m.Database())
	return nil
}
