package qwr

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
)

// connInit implements driver.Connector to run ATTACH statements and
// per-schema PRAGMAs on every new connection. PRAGMAs for the main
// database are handled by the modernc.org/sqlite driver via DSN
// _pragma parameters, so this connector only needs to handle attachments.
type connInit struct {
	drv driver.Driver
	dsn string

	mu          sync.RWMutex
	attachments []attachment
}

// Connect opens a new database connection via the underlying driver and
// runs ATTACH + per-schema PRAGMA statements for each configured attachment.
func (c *connInit) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	atts := c.attachments
	c.mu.RUnlock()

	if len(atts) == 0 {
		return conn, nil
	}

	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("qwr: driver connection does not support ExecerContext")
	}

	for _, att := range atts {
		if _, err := execer.ExecContext(ctx, att.attachSQL, nil); err != nil {
			conn.Close()
			return nil, fmt.Errorf("qwr: failed to attach %q: %w", att.alias, err)
		}
		for _, stmt := range att.schemaStatements {
			if _, err := execer.ExecContext(ctx, stmt, nil); err != nil {
				conn.Close()
				return nil, fmt.Errorf("qwr: failed to apply pragma for %q: %w", att.alias, err)
			}
		}
	}

	return conn, nil
}

// Driver returns the underlying database driver.
func (c *connInit) Driver() driver.Driver {
	return c.drv
}

// addAttachment appends an attachment to the list. New connections created
// after this call will run the ATTACH statement automatically.
func (c *connInit) addAttachment(att attachment) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attachments = append(c.attachments, att)
}

// hasAlias reports whether the given alias is already attached.
func (c *connInit) hasAlias(alias string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, att := range c.attachments {
		if att.alias == alias {
			return true
		}
	}
	return false
}

// sqliteDriver returns a reference to the registered "sqlite" driver.
// It opens a temporary in-memory database to extract the driver, then
// closes it immediately.
func sqliteDriver() (driver.Driver, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("qwr: failed to open sqlite driver: %w", err)
	}
	drv := db.Driver()
	db.Close()
	return drv, nil
}
