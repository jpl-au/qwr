//go:build windows

package qwr

import "path/filepath"

// fileURIPath converts a Windows filesystem path to the path component
// of a file: URI. Drive-letter paths like C:\foo\bar.db become
// /C:/foo/bar.db so that url.URL produces the correct three-slash
// form file:///C:/foo/bar.db.
func fileURIPath(path string) string {
	p := filepath.ToSlash(path)
	if len(p) > 0 && p[0] != '/' {
		p = "/" + p
	}
	return p
}
