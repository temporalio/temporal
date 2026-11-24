//go:build test_dep

package testhooks

import (
	// Every server with test hooks can return the server's errors from failpoints.
	_ "go.temporal.io/server/common/testing/failpointerrors"
)
