//go:build !test_dep

package failpoint

import (
	"context"
)

// ExecuteCtx executes a failpoint if registered and enabled.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
func ExecuteCtx[T any](ctx context.Context, key Key) (T, bool) {
	var zero T
	return zero, false
}

// Execute executes the failpoint for key in the registry of src, if registered and enabled.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
func Execute[T any](src Source, key Key) (T, bool) {
	var zero T
	return zero, false
}
