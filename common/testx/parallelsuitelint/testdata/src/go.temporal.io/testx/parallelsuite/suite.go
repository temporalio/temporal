package parallelsuite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type Suite[T any] struct{ *require.Assertions }

func Run[T any](*testing.T, T, ...any)                 {}
func RunLegacySequential[T any](*testing.T, T, ...any) {}

func (*Suite[T]) Run(string, func(T)) bool           { return true }
func (*Suite[T]) RunSequential(string, func(T)) bool { return true }
func (*Suite[T]) T() *testing.T                      { return nil }
func (*Suite[T]) TB() testing.TB                     { return nil }
func (*Suite[T]) Context() context.Context           { return nil }
func (*Suite[T]) Require() *require.Assertions       { return nil }
func (*Suite[T]) AssertionT() require.TestingT       { return nil }
func (*Suite[T]) Assert() *require.Assertions        { return nil }
func (*Suite[T]) SetT(*testing.T)                    {}
func (*Suite[T]) SetS(any)                           {}

type Scope interface {
	Context() context.Context
	TB() testing.TB
	Require() *require.Assertions
	AssertionT() require.TestingT
}
