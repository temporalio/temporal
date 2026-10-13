package await_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
)

func TestRcv(t *testing.T) {
	t.Parallel()

	t.Run("receives value", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string, 1)
		ch <- "value"

		require.Equal(t, "value", await.Rcv(t, ch))
	})

	t.Run("returns zero value when channel closes", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		close(ch)

		require.Empty(t, await.Rcv(t, ch))
	})

	t.Run("fails when context ends", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		tb := newRecordingTB()

		tb.run(func() {
			cancelTestContext(tb)
			await.Rcv(tb, ch)
		})

		require.Contains(t, tb.fatals(), "context canceled")
	})
}

func TestSnd(t *testing.T) {
	t.Parallel()

	t.Run("sends value", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string, 1)
		await.Snd(t, ch, "value")

		require.Equal(t, "value", <-ch)
	})

	t.Run("fails instead of panicking when channel closes", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		close(ch)
		tb := newRecordingTB()

		tb.run(func() {
			await.Snd(tb, ch, "value")
		})

		require.Contains(t, tb.fatals(), "channel closed before sending a value")
	})

	t.Run("fails when context ends", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		tb := newRecordingTB()

		tb.run(func() {
			cancelTestContext(tb)
			await.Snd(tb, ch, "value")
		})

		require.Contains(t, tb.fatals(), "context canceled")
	})
}

func cancelTestContext(tb testing.TB) {
	testcontext.AttachDecorator(tb, "cancel", func(ctx context.Context) context.Context {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		return ctx
	})
}
