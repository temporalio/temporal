package testcontext

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	ctx := New(t, WithTimeout(time.Second))
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(time.Second), deadline, 50*time.Millisecond)
}

func TestContextDecorators(t *testing.T) {
	t.Parallel()

	t.Run("applied once across calls", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		var calls atomic.Int32
		decorator := func(ctx context.Context) context.Context {
			calls.Add(1)
			return context.WithValue(ctx, key{}, "decorated")
		}

		ctx := New(t, WithContextDecorator(key{}, decorator))
		require.Equal(t, "decorated", ctx.Value(key{}))

		ctx = New(t, WithContextDecorator(key{}, decorator))
		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("applied once in single call", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		var calls atomic.Int32
		decorator := func(ctx context.Context) context.Context {
			calls.Add(1)
			return context.WithValue(ctx, key{}, "decorated")
		}

		ctx := New(t,
			WithContextDecorator(key{}, decorator),
			WithContextDecorator(key{}, decorator),
		)

		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("multiple decorators", func(t *testing.T) {
		t.Parallel()

		type key1 struct{}
		type key2 struct{}

		ctx := New(t,
			WithContextDecorator(key1{}, func(ctx context.Context) context.Context {
				return context.WithValue(ctx, key1{}, "one")
			}),
			WithContextDecorator(key2{}, func(ctx context.Context) context.Context {
				return context.WithValue(ctx, key2{}, "two")
			}),
		)

		require.Equal(t, "one", ctx.Value(key1{}))
		require.Equal(t, "two", ctx.Value(key2{}))
	})

	t.Run("later call decorates cached context", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		ctx := New(t)
		require.Nil(t, ctx.Value(key{}))

		ctx = New(t, WithContextDecorator(key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		}))
		require.Equal(t, "decorated", ctx.Value(key{}))
	})
}

func TestCleanupCancelsContext(t *testing.T) {
	t.Parallel()

	var ctx context.Context
	t.Run("subtest", func(t *testing.T) {
		ctx = New(t)
		require.NoError(t, ctx.Err())
	})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestEnvTimeout(t *testing.T) {
	t.Run("from env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		ctx := New(t)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(10*time.Second), deadline, 50*time.Millisecond)
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		ctx := New(t, WithTimeout(time.Second))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(time.Second), deadline, 50*time.Millisecond)
	})
}
