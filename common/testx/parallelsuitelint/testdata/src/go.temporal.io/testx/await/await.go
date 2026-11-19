package await

import (
	"context"
	"testing"
	"time"
)

type T struct{}

func Require(context.Context, testing.TB, func(*T), time.Duration, time.Duration)        {}
func RequireTrue(testing.TB, func() bool, time.Duration, time.Duration)                  {}
func RequireTruef(testing.TB, func() bool, time.Duration, time.Duration, string, ...any) {}
func Rcv[V any](testing.TB, <-chan V) V                                                  { var zero V; return zero }
func Snd[V any](testing.TB, chan<- V, V)                                                 {}
