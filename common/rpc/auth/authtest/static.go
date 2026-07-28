// Package authtest provides shared test helpers for the auth package. The conventional
// "*test" suffix marks it as test-shaped, but it is a regular Go package importable from
// production code (e.g. manual harnesses) when needed.
package authtest

import (
	"context"

	"go.temporal.io/server/common/rpc/auth"
)

// StaticTokenProvider returns the same token for every call. Useful for manual
// harnesses and tests; not a production-grade provider (no expiry, no rotation,
// no per-receiver scoping).
type StaticTokenProvider string

var _ auth.TokenProvider = StaticTokenProvider("")

func (t StaticTokenProvider) GetToken(context.Context, string) (string, error) {
	return string(t), nil
}
