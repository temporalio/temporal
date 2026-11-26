// Package serverfailpoints declares the failpoint keys that every service shares: the keys of the
// gRPC, HTTP and persistence boundaries, which are named after the operation. Server code executes
// them through the server's failpoint registry, scoped to a namespace ID; they only fire if the
// startup switch (the failpoints.enabled setting of the static config) enabled failpoints. Other
// keys are declared by the package that executes them.
package serverfailpoints

import (
	// Every server can return the server's errors from failpoints.
	_ "go.temporal.io/server/common/testing/failpointerrors"
)
