package await

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/testx/await"
)

type T = await.T

func Rcv[T any](tb testing.TB, ch <-chan T) T {
	tb.Helper()
	return await.Rcv(tb, ch)
}

func Snd[T any](tb testing.TB, ch chan<- T, value T) {
	tb.Helper()
	await.Snd(tb, ch, value)
}

func Require(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration) {
	tb.Helper()
	await.Require(ctx, tb, condition, timeout, pollInterval)
}

func Requiref(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	await.Requiref(ctx, tb, condition, timeout, pollInterval, msg, args...)
}

func RequireTrue(tb testing.TB, condition func() bool, timeout, pollInterval time.Duration) {
	tb.Helper()
	await.RequireTrue(tb, condition, timeout, pollInterval)
}

func RequireTruef(tb testing.TB, condition func() bool, timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	await.RequireTruef(tb, condition, timeout, pollInterval, msg, args...)
}
