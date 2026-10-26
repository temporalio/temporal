//go:build !gomad

package sqlplugin

import (
	"errors"
	"syscall"
)

// isConnectionErrno reports whether err wraps a socket errno that means the
// database connection is gone.
func isConnectionErrno(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED)
}
