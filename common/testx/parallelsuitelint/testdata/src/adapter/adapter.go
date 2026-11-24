package adapter

import (
	"testing"

	ps "go.temporal.io/testx/parallelsuite"
)

func Forward[T any](t *testing.T, s T)    { Run(t, s) }
func Run[T any](t *testing.T, s T)        { t.Helper(); ps.Run(t, s) }
func Sequential[T any](t *testing.T, s T) { ps.RunLegacySequential(t, s) }
func Fake[T any](*testing.T, T)           {}
func Conditional[T any](t *testing.T, s T, enabled bool) {
	if enabled {
		ps.Run(t, s)
	}
}
func DifferentTest[T any](t *testing.T, s T)  { ps.Run(new(testing.T), s) }
func DifferentSuite[T any](t *testing.T, s T) { ps.Run(t, new(T)) }
