package parallelsuite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
