package qwr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jpl-au/qwr/checkpoint"
	"github.com/jpl-au/qwr/profile"
)

// qwrBuilder provides a fluent API for building a Manager
type qwrBuilder struct {
	// Embedding the Manager eliminates duplication
	*Manager

	// Builder-specific fields
	path             string
	pendingObservers []EventHandler
	attachments      []attachment
	attachErrs       []error // deferred validation errors from Attach()
	isSQL            bool    // true when created via NewSQL
}

// New creates a new qwr Manager instance builder
//
// Options are immutable after construction. They can only be set here during
// manager creation and cannot be modified at runtime. If no options are provided,
// DefaultOptions will be used.
//
// To change options, you must stop the application, create a new manager with
// different options, and restart.
func New(path string, opts ...Options) *qwrBuilder {
	// Create a manager with base configuration
	manager := &Manager{
		options: DefaultOptions, // Always start with defaults
		path:    path,           // Store database path for logging context
	}

	// Override with user options if provided
	if len(opts) > 0 {
		manager.options = opts[0]
	} else {
		manager.options = DefaultOptions
	}

	// Validate and set defaults for options
	manager.options.Validate()

	// Create the builder with the embedded manager
	return &qwrBuilder{
		Manager: manager,
		path:    path,
	}
}

// NewSQL creates a new qwr Manager instance builder using user-provided database connections.
// This allows you to bring your own SQLite driver (e.g., mattn/go-sqlite3 instead of modernc.org/sqlite).
//
// Parameters:
//   - reader: Database connection for read operations (pass nil to disable reader)
//   - writer: Database connection for write operations (pass nil to disable writer)
//   - opts: Optional configuration options (variadic). If not provided, DefaultOptions will be used.
//
// Important notes:
//   - Passing nil for reader/writer automatically disables that connection (sets EnableReader/EnableWriter to false)
//   - The Manager takes full ownership of the provided database connections
//   - Calling Manager.Close() will close these database connections
//   - You should not use these connections directly after passing them to NewSQL()
//   - Profiles will be applied to your connections (including SQLite PRAGMAs)
//   - If you provide a non-SQLite database, PRAGMA errors are your responsibility
//   - Use WithErrorLogPath() to enable persistent error logging to disk
//   - Attach() is not supported with NewSQL - manage ATTACH statements on your own connections
//
// Example with mattn/go-sqlite3:
//
//	import _ "github.com/mattn/go-sqlite3"
//
//	readerDB, _ := sql.Open("sqlite3", "mydb.db")
//	writerDB, _ := sql.Open("sqlite3", "mydb.db")
//	opts := qwr.Options{ErrorLogPath: "errors.db"} // Optional error logging
//	manager, err := qwr.NewSQL(readerDB, writerDB, opts).
//	    Reader(profile.ReadBalanced()).
//	    Writer(profile.WriteBalanced()).
//	    Open()
func NewSQL(reader, writer *sql.DB, opts ...Options) *qwrBuilder {
	// Create a manager with base configuration
	manager := &Manager{
		options: DefaultOptions,
		reader:  reader,
		writer:  writer,
	}

	// Override with user options if provided
	if len(opts) > 0 {
		manager.options = opts[0]
	} else {
		manager.options = DefaultOptions
	}

	// Automatically disable reader/writer if nil connections are provided
	if reader == nil {
		manager.options.EnableReader = false
	}
	if writer == nil {
		manager.options.EnableWriter = false
	}

	// Validate and set defaults for options
	manager.options.Validate()

	// Create the builder with the embedded manager
	return &qwrBuilder{
		Manager: manager,
		isSQL:   true,
	}
}

// WithContext sets a context for the manager and enables context usage
func (mb *qwrBuilder) WithContext(ctx context.Context) *qwrBuilder {
	mb.ctx = ctx
	mb.internalCtx = ctx // Set internal context to same as user context
	mb.options.UseContexts = true
	return mb
}

// Reader sets the reader profile
func (mb *qwrBuilder) Reader(p *profile.Profile) *qwrBuilder {
	mb.readerProfile = p
	return mb
}

// Writer sets the writer profile
func (mb *qwrBuilder) Writer(p *profile.Profile) *qwrBuilder {
	mb.writerProfile = p
	return mb
}

// Attach registers a database to be attached on every connection. The alias
// is the schema name used to reference the attached database in SQL queries
// (e.g. SELECT * FROM analytics.events). The path is the database file path,
// resolved relative to the main database's directory if not absolute.
//
// An optional profile configures per-schema PRAGMAs for the attached database.
// Only PRAGMA settings from the profile are used - pool parameters are ignored
// since attached databases share the main connection pool.
//
// Attach is not supported with NewSQL - Open will return an error.
//
// Example:
//
//	manager, err := qwr.New("main.db").
//	    Reader(profile.ReadBalanced()).
//	    Writer(profile.WriteBalanced()).
//	    Attach("analytics", "analytics.db", profile.Attached().
//	        WithJournalMode(profile.JournalWal).
//	        WithCacheSize(-30720)).
//	    Attach("cache", ":memory:").
//	    Open()
func (mb *qwrBuilder) Attach(alias, path string, p ...*profile.Profile) *qwrBuilder {
	var prof *profile.Profile
	if len(p) > 0 {
		prof = p[0]
	}
	att, err := newAttachment(alias, path, mb.path, prof)
	if err != nil {
		mb.attachErrs = append(mb.attachErrs, fmt.Errorf("attachment %q: %w", alias, err))
		return mb
	}
	mb.attachments = append(mb.attachments, att)
	return mb
}

// WithErrorDB sets the path for the error log database.
// If not set, persistent error logging is disabled.
// This method works with both New() and NewSQL() constructors.
// In-memory error logging could cause unbounded memory growth in long-running applications.
func (mb *qwrBuilder) WithErrorDB(path string) *qwrBuilder {
	if path == ":memory:" {
		return mb
	}
	mb.options.ErrorLogPath = path
	return mb
}

// WithObserver registers an event handler that will receive events from the
// moment the manager is opened. Use this to capture EventManagerOpened.
func (mb *qwrBuilder) WithObserver(handler EventHandler) *qwrBuilder {
	mb.pendingObservers = append(mb.pendingObservers, handler)
	return mb
}

// Checkpoint sets the WAL checkpoint mode to run automatically on Close().
// This ensures the WAL is properly checkpointed when shutting down.
//
// Available modes:
//   - checkpoint.None: No checkpoint on close (default)
//   - checkpoint.Passive: Non-blocking, best-effort checkpoint
//   - checkpoint.Full: Wait for writers, checkpoint all frames
//   - checkpoint.Restart: Full + restart WAL from beginning
//   - checkpoint.Truncate: Restart + truncate WAL to zero bytes
func (mb *qwrBuilder) Checkpoint(mode checkpoint.Mode) *qwrBuilder {
	mb.checkpoint = mode
	return mb
}

// Open initialises and opens database connections.
//
// After Open() is called, all options become immutable and cannot be changed.
// The manager must be closed and recreated to modify configuration.
func (mb *qwrBuilder) Open() (*Manager, error) {
	// Reject Attach with NewSQL
	if mb.isSQL && (len(mb.attachments) > 0 || len(mb.attachErrs) > 0) {
		return nil, ErrAttachNotSupported
	}

	// Validate checkpoint mode
	if !mb.checkpoint.Valid() {
		return nil, fmt.Errorf("%w: %q", checkpoint.ErrInvalidMode, mb.checkpoint)
	}

	// Validate path only if we need to open databases ourselves
	if mb.reader == nil && mb.writer == nil && mb.path == "" {
		return nil, errors.New("database path cannot be empty when not using NewSQL()")
	}

	// Surface any deferred attachment validation errors
	if len(mb.attachErrs) > 0 {
		return nil, errors.Join(mb.attachErrs...)
	}

	// Check for duplicate aliases
	seen := make(map[string]bool, len(mb.attachments))
	for _, att := range mb.attachments {
		if seen[att.alias] {
			return nil, fmt.Errorf("%w: %q", ErrAttachDuplicateAlias, att.alias)
		}
		seen[att.alias] = true
	}

	// Ensure internalCtx is never nil - required by BatchCollector
	if mb.internalCtx == nil {
		mb.internalCtx = context.Background()
	}

	// Create directory for database file if it doesn't exist (only if using file-based constructor)
	if mb.path != "" && mb.path != ":memory:" {
		dir := filepath.Dir(mb.path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return nil, fmt.Errorf("failed to create directory for database: %w", err)
			}
		}
	}

	// Create EventBus early so it's available for all components
	mb.events = NewEventBus()

	// Register any pending observers from WithObserver()
	for _, handler := range mb.pendingObservers {
		mb.events.Subscribe(handler)
	}

	// Store attachments and SQL flag on manager for runtime access
	mb.Manager.attachments = mb.attachments
	mb.Manager.isSQL = mb.isSQL

	// cleanup closes all resources allocated so far during Open.
	cleanup := func() {
		if mb.readStmtCache != nil {
			mb.readStmtCache.Close()
		}
		if mb.reader != nil {
			_ = mb.reader.Close()
		}
		if mb.writeStmtCache != nil {
			mb.writeStmtCache.Close()
		}
		if mb.writer != nil {
			_ = mb.writer.Close()
		}
		if mb.events != nil {
			mb.events.Close()
		}
	}

	// Initialise reader if enabled
	if mb.options.EnableReader {
		if mb.readerProfile == nil {
			mb.readerProfile = profile.ReadBalanced()
		}

		// Open database if not already provided by user
		if mb.reader == nil {
			var err error
			mb.reader, mb.readerConnector, err = openWithConnector(mb.path, mb.readerProfile, mb.attachments)
			if err != nil {
				cleanup()
				return nil, err
			}
		} else {
			// Apply profile to user-provided database
			if err := mb.readerProfile.Apply(mb.reader); err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to apply reader profile: %w", err)
			}
		}

		// Initialise reader statement cache
		var err error
		mb.readStmtCache, err = NewStmtCache(mb.events, mb.options)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create reader statement cache: %w", err)
		}
	}

	// Initialise writer if enabled
	if mb.options.EnableWriter {
		var err error

		if mb.writerProfile == nil {
			mb.writerProfile = profile.WriteBalanced()
		}

		// Open database if not already provided by user
		if mb.writer == nil {
			mb.writer, mb.writerConnector, err = openWithConnector(mb.path, mb.writerProfile, mb.attachments)
			if err != nil {
				cleanup()
				return nil, err
			}
		} else {
			// Apply profile to user-provided database
			if err := mb.writerProfile.Apply(mb.writer); err != nil {
				cleanup()
				return nil, fmt.Errorf("failed to apply writer profile: %w", err)
			}
		}

		// SQLite only supports one concurrent writer per database file.
		// The write serialiser depends on this - enforce it regardless
		// of what the profile requests.
		mb.writer.SetMaxOpenConns(1)

		// Initialise writer statement cache
		mb.writeStmtCache, err = NewStmtCache(mb.events, mb.options)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed to create writer statement cache: %w", err)
		}

		// Create worker pool
		mb.serialiser = NewWorkerPool(
			mb.writer,
			mb.options.WorkerQueueDepth,
			mb.events,
			mb.writeStmtCache,
			mb.options,
		)

		// Initialise ErrorQueue
		mb.errorQueue = NewErrorQueue(mb.events, mb.options, mb.path)

		// Start the worker pool
		if mb.ctx != nil {
			mb.serialiser.Start(mb.ctx)
		} else {
			mb.serialiser.Start(context.Background())
		}

		// Wire retry subscriber if auto-retry is enabled
		if mb.options.EnableAutoRetry {
			mb.events.SubscribeFiltered(func(e Event) {
				mb.handleRetryEvent(e)
			}, func(t EventType) bool { return t == EventJobFailed })
		}

		// Initialise batch collector
		mb.batcher = NewBatchCollector(
			mb.internalCtx,
			mb.serialiser,
			mb.events,
			mb.options,
			mb.path,
		)
	}

	mb.events.Emit(Event{Type: EventManagerOpened})
	return mb.Manager, nil
}

// openWithConnector opens a database using a custom connector that handles
// per-connection PRAGMA application via DSN parameters and ATTACH statements.
// Returns the *sql.DB and the connector for later runtime attachment updates.
func openWithConnector(path string, p *profile.Profile, attachments []attachment) (*sql.DB, *connInit, error) {
	drv, err := sqliteDriver()
	if err != nil {
		return nil, nil, err
	}

	dsn := buildDSN(path, p)
	connector := &connInit{
		drv:         drv,
		dsn:         dsn,
		attachments: attachments,
	}

	db := sql.OpenDB(connector)
	p.ApplyPool(db)

	// Verify the connection works (this also triggers the first ATTACH)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("failed to open database: %w", err)
	}

	return db, connector, nil
}

// buildDSN constructs a modernc.org/sqlite DSN with _pragma parameters
// from the profile. This ensures PRAGMAs are applied on every new connection.
func buildDSN(path string, p *profile.Profile) string {
	if p == nil {
		return path
	}

	params := p.DSNPragmas()
	if len(params) == 0 {
		return path
	}

	// Use file: URI format for the DSN.
	var b strings.Builder
	if !strings.HasPrefix(path, "file:") {
		if path == ":memory:" {
			b.WriteString("file::memory:")
		} else {
			u := &url.URL{Scheme: "file", Path: fileURIPath(path)}
			b.WriteString(u.String())
		}
	} else {
		b.WriteString(path)
	}

	// Use & if the path already has query parameters, ? otherwise
	if strings.Contains(path, "?") {
		b.WriteByte('&')
	} else {
		b.WriteByte('?')
	}

	for i, param := range params {
		if i > 0 {
			b.WriteByte('&')
		}
		// The _pragma values use parentheses, not =, so we need to
		// encode only the value portion after the = sign.
		parts := strings.SplitN(param, "=", 2)
		b.WriteString(parts[0])
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(parts[1]))
	}

	return b.String()
}
