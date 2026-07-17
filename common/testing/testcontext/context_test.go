package testcontext

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc/metadata"
)

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	t.Run("default", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()), deadline)
			require.Equal(t, 90*time.Second*debug.TimeoutMultiplier, DefaultTimeout())
		})
	})

	t.Run("custom", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

func TestNameMetadata(t *testing.T) {
	t.Parallel()

	ctx := For(t)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
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

		AttachDecorator(t, key{}, decorator)
		ctx := For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		AttachDecorator(t, key{}, decorator)
		ctx = For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("applied once for same key", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		var calls atomic.Int32
		decorator := func(ctx context.Context) context.Context {
			calls.Add(1)
			return context.WithValue(ctx, key{}, "decorated")
		}

		AttachDecorator(t, key{}, decorator)
		AttachDecorator(t, key{}, decorator)
		ctx := For(t)

		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("multiple decorators", func(t *testing.T) {
		t.Parallel()

		type key1 struct{}
		type key2 struct{}

		AttachDecorator(t, key1{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key1{}, "one")
		})
		AttachDecorator(t, key2{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key2{}, "two")
		})
		ctx := For(t)

		require.Equal(t, "one", ctx.Value(key1{}))
		require.Equal(t, "two", ctx.Value(key2{}))
	})

	t.Run("later call decorates cached context", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		ctx := For(t)
		require.Nil(t, ctx.Value(key{}))

		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx = For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))
	})
}

func TestCleanupCancelsContext(t *testing.T) {
	t.Parallel()

	var ctx context.Context
	t.Run("subtest", func(t *testing.T) {
		ctx = For(t)
		require.NoError(t, ctx.Err())
	})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestEnvTimeout(t *testing.T) {
	t.Run("from env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(10*time.Second), deadline)
		})
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

func TestEnsureRemaining(t *testing.T) {
	t.Run("extends when remaining time is too short", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			refreshed, extension := EnsureRemaining(t, ctx, 250*time.Millisecond)
			require.Equal(t, 150*time.Millisecond, extension.ExtendedBy)
			require.Equal(t, start.Add(250*time.Millisecond), extension.Deadline)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, extension.Deadline, refreshedDeadline)
		})
	})

	t.Run("caps ensured remaining time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			refreshed, extension := EnsureRemaining(t, ctx, 10*time.Minute)
			require.Equal(t, maxTimeout-100*time.Millisecond, extension.ExtendedBy)
			require.Equal(t, start.Add(maxTimeout), extension.Deadline)
			require.Same(t, extension.CurrentContext(), refreshed)
		})
	})

	t.Run("replays decorators", func(t *testing.T) {
		type key struct{}

		For(t, WithTimeout(100*time.Millisecond))
		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx := For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		refreshed, extension := EnsureRemaining(t, ctx, time.Second)
		require.Positive(t, extension.ExtendedBy)

		require.Equal(t, "decorated", refreshed.Value(key{}))
	})

	t.Run("preserves test name metadata", func(t *testing.T) {
		ctx := For(t, WithTimeout(100*time.Millisecond))
		refreshed, extension := EnsureRemaining(t, ctx, time.Second)
		require.Positive(t, extension.ExtendedBy)

		md, ok := metadata.FromOutgoingContext(refreshed)
		require.True(t, ok)
		require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
	})

	t.Run("preserves original configured timeout", func(t *testing.T) {
		ctx := For(t, WithTimeout(100*time.Millisecond))
		_, extension := EnsureRemaining(t, ctx, time.Second)
		require.Positive(t, extension.ExtendedBy)

		For(t, WithTimeout(100*time.Millisecond))
	})

	t.Run("recognizes older context after repeated extensions", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			original := For(t, WithTimeout(5*time.Millisecond))

			firstRefresh, extension := EnsureRemaining(t, original, 10*time.Millisecond)
			require.True(t, extension.contains(original))
			require.True(t, extension.contains(firstRefresh))

			refreshed, extension := EnsureRemaining(t, firstRefresh, 20*time.Millisecond)
			require.True(t, extension.contains(original))
			require.True(t, extension.contains(firstRefresh))
			require.True(t, extension.contains(refreshed))
		})
	})

	t.Run("preserves unowned context", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(5*time.Millisecond))
			unowned, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()

			refreshed, extension := EnsureRemaining(t, unowned, 10*time.Millisecond)

			require.Positive(t, extension.ExtendedBy)
			require.Same(t, unowned, refreshed)
		})
	})

	t.Run("safe concurrent calls", func(t *testing.T) {
		ctx := For(t, WithTimeout(100*time.Millisecond))

		var wg sync.WaitGroup
		var negative atomic.Int32
		for range 8 {
			wg.Go(func() {
				_, extension := EnsureRemaining(t, ctx, 10*time.Millisecond)
				if extension.ExtendedBy < 0 {
					negative.Add(1)
				}
			})
		}
		wg.Wait()
		require.Zero(t, negative.Load())
	})
}
