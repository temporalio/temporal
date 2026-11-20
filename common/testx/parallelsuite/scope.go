package parallelsuite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
)

// Scope provides the context and test handles for a test or await attempt.
// It is useful for test helpers.
// Its assertions stop the current test or await attempt on failure.
type Scope interface {
	Context() context.Context
	TB() testing.TB
	Require() *require.Assertions
	AssertionT() require.TestingT

	// Common assertions for test helpers.
	NoError(err error, msgAndArgs ...any)
	Equal(expected, actual any, msgAndArgs ...any)
	Nil(object any, msgAndArgs ...any)
	NotNil(object any, msgAndArgs ...any)
	NotEmpty(object any, msgAndArgs ...any)
	True(value bool, msgAndArgs ...any)
}

type contextScope struct {
	Scope
	ctx context.Context
}

// WithContext returns a scope that uses ctx and delegates assertions to scope.
func WithContext(ctx context.Context, scope Scope) Scope {
	return contextScope{Scope: scope, ctx: ctx}
}

func (s contextScope) Context() context.Context {
	return s.ctx
}

// Await retries fn on assertion failure until the timeout or scope cancellation.
// The scope passed to fn carries the context and assertions for that attempt.
func Await(s Scope, fn func(Scope), timeout, interval time.Duration) {
	tb := s.TB()
	tb.Helper()
	await.Require(s.Context(), tb, func(t *await.T) {
		fn(t)
	}, timeout, interval)
}
