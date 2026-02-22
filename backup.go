package qwr

import (
	"context"
	"database/sql"
	"fmt"
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
			m.events.Emit(Event{Type: EventBackupFallback, BackupDest: dest})
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
	m.events.Emit(Event{Type: EventBackupStarted, BackupMethod: "api", BackupDest: dest})

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
			m.events.Emit(Event{Type: EventBackupFailed, BackupMethod: "api", BackupDest: dest, Err: err})
		}
		return err
	}

	m.events.Emit(Event{Type: EventBackupCompleted, BackupMethod: "api", BackupDest: dest})
	return nil
}

// backupVacuum performs backup using SQLite's VACUUM INTO command.
func (m *Manager) backupVacuum(dest string, db *sql.DB) error {
	m.events.Emit(Event{Type: EventBackupStarted, BackupMethod: "vacuum", BackupDest: dest})

	_, err := db.Exec(fmt.Sprintf("VACUUM INTO '%s'", dest))
	if err != nil {
		m.events.Emit(Event{Type: EventBackupFailed, BackupMethod: "vacuum", BackupDest: dest, Err: err})
		return fmt.Errorf("%w: %v", ErrBackupFailed, err)
	}

	m.events.Emit(Event{Type: EventBackupCompleted, BackupMethod: "vacuum", BackupDest: dest})
	return nil
}
