//go:build !windows

package qwr

// fileURIPath returns the path unchanged on Unix systems, where absolute
// paths already begin with a forward slash.
func fileURIPath(path string) string {
	return path
}
