package await

import (
	"context"
	"testing"
	"time"
)

// Scope provides the context and test handle for suite waits.
type Scope interface {
	Context() context.Context
	TB() testing.TB
}

// Suite adds polling assertions and channel operations to an embedded test suite.
type Suite[S any] struct {
	scope     Scope
	copySuite func(*T) S
}

// NewSuite binds waits to scope and creates a fresh suite for each retry attempt.
func NewSuite[S any](scope Scope, copySuite func(*T) S) Suite[S] {
	return Suite[S]{scope: scope, copySuite: copySuite}
}

// Rcv waits for and returns the next value from ch.
// A closed ch yields the zero value, as with a plain receive.
// It fails the test if the context ends before ch produces a value.
func (s *Suite[S]) Rcv[V any](ch <-chan V) V {
	t := s.scope.TB()
	t.Helper()
	return Rcv(t, ch)
}

// Snd waits to send value to ch.
// It fails the test if the context ends or ch closes before accepting the value.
func (s *Suite[S]) Snd[V any](ch chan<- V, value V) {
	t := s.scope.TB()
	t.Helper()
	Snd(t, ch, value)
}

// Await calls fn repeatedly until all assertions pass or timeout is reached.
func (s *Suite[S]) Await(fn func(S), timeout, interval time.Duration) {
	s.Awaitf(fn, timeout, interval, "")
}

// Awaitf is like [Await] but includes a format string appended to the failure message.
func (s *Suite[S]) Awaitf(fn func(S), timeout, interval time.Duration, msg string, args ...any) {
	t := s.scope.TB()
	Requiref(s.scope.Context(), t, func(at *T) {
		fn(s.copySuite(at))
	}, timeout, interval, msg, args...)
}

// AwaitTrue calls fn repeatedly until it returns true or timeout is reached.
//
// Use it for simple local predicates only. Do not use assertions or side effects; use [Await] instead.
func (s *Suite[S]) AwaitTrue(fn func() bool, timeout, interval time.Duration) {
	s.AwaitTruef(fn, timeout, interval, "")
}

// AwaitTruef is like [AwaitTrue] but includes a format string appended to the failure message.
func (s *Suite[S]) AwaitTruef(fn func() bool, timeout, interval time.Duration, msg string, args ...any) {
	RequireTruef(s.scope.TB(), fn, timeout, interval, msg, args...)
}
