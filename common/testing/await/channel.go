package await

import (
	"testing"

	"go.temporal.io/server/common/testing/testcontext"
)

// Rcv waits for and returns the next value from ch using the test context.
// Like a regular receive, a closed channel returns the element type's zero value.
func Rcv[T any](tb testing.TB, ch <-chan T) T {
	tb.Helper()

	ctx := testcontext.For(tb)
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		tb.Fatalf("context ended while waiting to receive from channel: %v", ctx.Err())
		var zero T
		return zero
	}
}

// Snd waits to send value to ch using the test context.
// It fails the test if the context ends or ch closes before accepting the value.
func Snd[T any](tb testing.TB, ch chan<- T, value T) {
	tb.Helper()

	ctx := testcontext.For(tb)
	defer func() {
		if recover() != nil {
			tb.Fatal("channel closed before sending a value")
		}
	}()

	select {
	case ch <- value:
	case <-ctx.Done():
		tb.Fatalf("context ended while waiting to send to channel: %v", ctx.Err())
	}
}
