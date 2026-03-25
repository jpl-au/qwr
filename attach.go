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
	ErrAttachReservedAlias = errors.New("alias is a reserved SQLite schema name")
	ErrAttachInvalidAlias  = errors.New("alias must be a valid SQLite identifier (letters, digits, underscores)")
	ErrAttachEmptyAlias    = errors.New("alias cannot be empty")
	ErrAttachEmptyPath     = errors.New("path cannot be empty")
	ErrAttachDuplicateAlias = errors.New("alias is already attached")
	ErrAttachNotSupported  = errors.New("Attach is not supported with NewSQL - manage ATTACH statements on your own connections")
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

	// Resolve relative paths against the main database's directory.
	// :memory: is left as-is.
	if path != ":memory:" && !filepath.IsAbs(path) && basePath != "" {
		path = filepath.Join(filepath.Dir(basePath), path)
	}

	att := attachment{
		alias:     alias,
		path:      path,
		attachSQL: fmt.Sprintf("ATTACH DATABASE '%s' AS %s", path, alias),
	}

	if p != nil && len(p.Pragmas) > 0 {
		att.schemaStatements = p.SchemaStatements(alias)
	}

	return att, nil
}
