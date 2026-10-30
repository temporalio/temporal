// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/parallelsuite.
package parallelsuite

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/testx/parallelsuite"
)

// Scope provides the context and test handles for a test or await attempt.
// It is useful for test helpers.
type Scope interface {
	parallelsuite.Scope
	Context() context.Context
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

// Suite extends the shared parallel suite with server assertions.
type Suite[T parallelsuite.TestingSuite] struct {
	parallelsuite.Suite[T]
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire

	suite T
	ctx   context.Context // override set in Init; nil means "use the test context"
}

// Init binds server assertions and context to each test, subtest, or await attempt.
func (s *Suite[T]) Init(suite T) {
	s.suite = suite
	if target, ok := s.AssertionT().(*await.T); ok {
		s.ctx = target.Context()
	}
	s.ProtoAssertions = protorequire.New(s.AssertionT())
	s.HistoryRequire = historyrequire.New(s.AssertionT())
}

// Context returns the test-scoped context (created from [testcontext]).
// Inside an [Await] callback, it returns the await-scoped context.
//
// The result is deliberately not cached; see [testcontext.EnsureRemaining].
func (s *Suite[T]) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return testcontext.For(s.T())
}

// Rcv waits for and returns the next value from ch.
// A closed ch yields the zero value, as with a plain receive.
// It fails the test if the context ends before ch produces a value.
func (s *Suite[T]) Rcv[V any](ch <-chan V) V {
	t := s.T()
	t.Helper()
	return await.Rcv(t, ch)
}

// Snd waits to send value to ch.
// It fails the test if the context ends or ch closes before accepting the value.
func (s *Suite[T]) Snd[V any](ch chan<- V, value V) {
	t := s.T()
	t.Helper()
	await.Snd(t, ch, value)
}

// Await calls fn repeatedly until all assertions pass or timeout is reached.
func (s *Suite[T]) Await(fn func(T), timeout, interval time.Duration) {
	s.Awaitf(fn, timeout, interval, "")
}

// Awaitf is like [Await] but includes a format string appended to the failure message.
func (s *Suite[T]) Awaitf(fn func(T), timeout, interval time.Duration, msg string, args ...any) {
	t := s.T()
	await.Requiref(s.Context(), t, func(at *await.T) {
		fn(parallelsuite.WithAssertions(s.suite, at))
	}, timeout, interval, msg, args...)
}

// AwaitTrue calls fn repeatedly until it returns true or timeout is reached.
//
// Use it for simple local predicates only. Do not use assertions or side effects; use [Await] instead.
func (s *Suite[T]) AwaitTrue(fn func() bool, timeout, interval time.Duration) {
	s.AwaitTruef(fn, timeout, interval, "")
}

// AwaitTruef is like [AwaitTrue] but includes a format string appended to the failure message.
func (s *Suite[T]) AwaitTruef(fn func() bool, timeout, interval time.Duration, msg string, args ...any) {
	await.RequireTruef(s.T(), fn, timeout, interval, msg, args...)
}

func Run[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.Run(t, s, args...)
}

// Deprecated: use [Run] for new tests. This only exists for backwards-compatibility
// with legacy behavior to ease migration.
func RunLegacySequential[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.RunLegacySequential(t, s, args...) //nolint:staticcheck // SA1019: forwarding the legacy runner
}
