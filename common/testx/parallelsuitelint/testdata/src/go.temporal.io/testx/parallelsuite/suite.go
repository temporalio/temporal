package parallelsuite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type Suite[T any] struct{ *require.Assertions }

func Run[T any](*testing.T, T, ...any)                 {}
func RunLegacySequential[T any](*testing.T, T, ...any) {}

func (*Suite[T]) Run(string, func(T)) bool                                             { return true }
func (*Suite[T]) RunSequential(string, func(T)) bool                                   { return true }
func (*Suite[T]) T() *testing.T                                                        { return nil }
func (*Suite[T]) TB() testing.TB                                                       { return nil }
func (*Suite[T]) Context() context.Context                                             { return nil }
func (*Suite[T]) Require() *require.Assertions                                         { return nil }
func (*Suite[T]) AssertionT() require.TestingT                                         { return nil }
func (*Suite[T]) Assert() *require.Assertions                                          { return nil }
func (*Suite[T]) SetT(*testing.T)                                                      {}
func (*Suite[T]) SetS(any)                                                             {}
func (*Suite[T]) Await(func(T), time.Duration, time.Duration)                          {}
func (*Suite[T]) Awaitf(func(T), time.Duration, time.Duration, string, ...any)         {}
func (*Suite[T]) AwaitTrue(func() bool, time.Duration, time.Duration)                  {}
func (*Suite[T]) AwaitTruef(func() bool, time.Duration, time.Duration, string, ...any) {}
func (*Suite[T]) Rcv[V any](<-chan V) V                                                { var zero V; return zero }
func (*Suite[T]) Snd[V any](chan<- V, V)                                               {}

type Scope interface {
	Context() context.Context
	TB() testing.TB
	Require() *require.Assertions
	AssertionT() require.TestingT
}
