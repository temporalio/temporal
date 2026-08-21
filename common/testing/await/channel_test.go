package await_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
)

func TestRequireReceive(t *testing.T) {
	t.Parallel()

	t.Run("receives value", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string, 1)
		ch <- "value"

		require.Equal(t, "value", await.RequireReceive(t, ch))
	})

	t.Run("fails when channel closes", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		close(ch)
		tb := newRecordingTB()

		tb.run(func() {
			await.RequireReceive(tb, ch)
		})

		require.Contains(t, tb.fatals(), "channel closed before receiving a value")
	})

	t.Run("fails when context ends", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		tb := newRecordingTB()

		tb.run(func() {
			cancelTestContext(tb)
			await.RequireReceive(tb, ch)
		})

		require.Contains(t, tb.fatals(), "context canceled")
	})
}

func TestRequireSend(t *testing.T) {
	t.Parallel()

	t.Run("sends value", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string, 1)
		await.RequireSend(t, ch, "value")

		require.Equal(t, "value", <-ch)
	})

	t.Run("fails when channel closes", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		close(ch)
		tb := newRecordingTB()

		tb.run(func() {
			await.RequireSend(tb, ch, "value")
		})

		require.Contains(t, tb.fatals(), "channel closed before sending a value")
	})

	t.Run("fails when context ends", func(t *testing.T) {
		t.Parallel()

		ch := make(chan string)
		tb := newRecordingTB()

		tb.run(func() {
			cancelTestContext(tb)
			await.RequireSend(tb, ch, "value")
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
