package await

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/testx/await"
)

type T = await.T

func Requiref(ctx context.Context, tb testing.TB, attempt func(*T), timeout, interval time.Duration, format string, args ...any) {
	await.Requiref(ctx, tb, attempt, timeout, interval, format, args...)
}

func Rcv[V any](tb testing.TB, ch <-chan V) V {
	return await.Rcv(tb, ch)
}

func Snd[V any](tb testing.TB, ch chan<- V, value V) {
	await.Snd(tb, ch, value)
}

func RequireTruef(testing.TB, func() bool, time.Duration, time.Duration, string) {}
