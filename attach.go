package qwr

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jpl-au/qwr/profile"
)

// attachment represents a database to attach on every connection.
type attachment struct {
	alias            string   // Schema alias used in SQL (e.g. "analytics")
	path             string   // Absolute path to the database file
	attachSQL        string   // Prebuilt ATTACH DATABASE statement
	schemaStatements []string // Per-schema PRAGMA statements from the profile
}

// reservedSchemas are SQLite schema names that cannot be used as aliases.
var reservedSchemas = map[string]bool{
	"main": true,
	"temp": true,
}

// validAlias matches a valid SQLite identifier: starts with a letter or
// underscore, followed by letters, digits, or underscores.
var validAlias = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var (
	ErrAttachReservedAlias  = errors.New("alias is a reserved SQLite schema name")
	ErrAttachInvalidAlias   = errors.New("alias must be a valid SQLite identifier (letters, digits, underscores)")
	ErrAttachEmptyAlias     = errors.New("alias cannot be empty")
	ErrAttachEmptyPath      = errors.New("path cannot be empty")
	ErrAttachDuplicateAlias = errors.New("alias is already attached")
	ErrAttachMemoryPath     = errors.New(":memory: databases are per-connection and cannot be shared across the pool - use file::memory:?cache=shared instead")
	ErrAttachNotSupported   = errors.New("Attach is not supported with NewSQL - manage ATTACH statements on your own connections")
)

// newAttachment validates inputs and builds an attachment with precomputed SQL.
// If basePath is non-empty and path is relative, it is resolved relative to
// the directory containing basePath.
func newAttachment(alias, path, basePath string, p *profile.Profile) (attachment, error) {
	if alias == "" {
		return attachment{}, ErrAttachEmptyAlias
	}
	if path == "" {
		return attachment{}, ErrAttachEmptyPath
	}

	lower := strings.ToLower(alias)
	if reservedSchemas[lower] {
		return attachment{}, fmt.Errorf("%w: %q", ErrAttachReservedAlias, alias)
	}
	if !validAlias.MatchString(alias) {
		return attachment{}, fmt.Errorf("%w: %q", ErrAttachInvalidAlias, alias)
	}

	// Reject bare :memory: - each pooled connection would get its own
	// isolated database, making cross-pool queries silently broken.
	if path == ":memory:" {
		return attachment{}, ErrAttachMemoryPath
	}

	// Resolve relative paths against the main database's directory.
	if !filepath.IsAbs(path) && !strings.HasPrefix(path, "file:") {
		if basePath == "" || basePath == ":memory:" {
			return attachment{}, fmt.Errorf("relative attachment path %q requires a file-backed main database", path)
		}
		path = filepath.Join(filepath.Dir(basePath), path)
	}

	att := attachment{
		alias:     alias,
		path:      path,
		attachSQL: fmt.Sprintf("ATTACH DATABASE ? AS %s", alias),
	}

	if p != nil {
		att.schemaStatements = p.SchemaStatements(alias)
	}

	return att, nil
}

// validSchema checks that a schema name is a valid SQLite identifier.
func validSchema(schema string) error {
	if !validAlias.MatchString(schema) {
		return fmt.Errorf("invalid schema name: %q", schema)
	}
	return nil
}
