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

// ExecuteCtx2 executes a two-valued failpoint if registered and enabled.
// Returns the failpoint's return values and a boolean indicating whether the failpoint
// provided return values.
func ExecuteCtx2[T1, T2 any](ctx context.Context, key Key) (T1, T2, bool) {
	var zero1 T1
	var zero2 T2
	return zero1, zero2, false
}
