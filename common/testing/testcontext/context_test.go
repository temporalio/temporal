package testcontext

import (
	"context"
	"fmt"
	"runtime"
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
			ctx := GetOrCreate(t)
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
			ctx := GetOrCreate(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

func TestNameMetadata(t *testing.T) {
	t.Parallel()

	ctx := GetOrCreate(t)
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
		ctx := GetOrCreate(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		AttachDecorator(t, key{}, decorator)
		ctx = GetOrCreate(t)
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
		ctx := GetOrCreate(t)

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
		ctx := GetOrCreate(t)

		require.Equal(t, "one", ctx.Value(key1{}))
		require.Equal(t, "two", ctx.Value(key2{}))
	})

	t.Run("later call decorates cached context", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		ctx := GetOrCreate(t)
		require.Nil(t, ctx.Value(key{}))

		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx = GetOrCreate(t)
		require.Equal(t, "decorated", ctx.Value(key{}))
	})
}

func TestCleanupCancelsContext(t *testing.T) {
	t.Parallel()

	var ctx context.Context
	t.Run("subtest", func(t *testing.T) {
		ctx = GetOrCreate(t)
		require.NoError(t, ctx.Err())
	})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestEnvTimeout(t *testing.T) {
	t.Run("from env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(10*time.Second), deadline)
		})
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t, WithTimeout(time.Second))
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
			ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			refreshed := EnsureRemaining(ctx, t, 250*time.Millisecond)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(250*time.Millisecond), refreshedDeadline)
		})
	})

	t.Run("caps ensured remaining time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			refreshed := EnsureRemaining(ctx, t, 10*time.Minute)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(maxTimeout), refreshedDeadline)
		})
	})

	t.Run("replays decorators", func(t *testing.T) {
		type key struct{}

		GetOrCreate(t, WithTimeout(100*time.Millisecond))
		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx := GetOrCreate(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		refreshed := EnsureRemaining(ctx, t, time.Second)

		require.Equal(t, "decorated", refreshed.Value(key{}))
	})

	t.Run("preserves test name metadata", func(t *testing.T) {
		ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))
		refreshed := EnsureRemaining(ctx, t, time.Second)

		md, ok := metadata.FromOutgoingContext(refreshed)
		require.True(t, ok)
		require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
	})

	t.Run("preserves original configured timeout", func(t *testing.T) {
		ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))
		EnsureRemaining(ctx, t, time.Second)

		GetOrCreate(t, WithTimeout(100*time.Millisecond))
	})

	t.Run("recognizes older context after repeated extensions", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			original := GetOrCreate(t, WithTimeout(5*time.Millisecond))

			firstRefresh := EnsureRemaining(original, t, 10*time.Millisecond)
			firstDeadline, ok := firstRefresh.Deadline()
			require.True(t, ok)
			require.Equal(t, time.Now().Add(10*time.Millisecond), firstDeadline)

			refreshed := EnsureRemaining(original, t, 20*time.Millisecond)
			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, time.Now().Add(20*time.Millisecond), refreshedDeadline)
		})
	})

	t.Run("fails for unowned context with earlier deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				GetOrCreate(tb, WithTimeout(5*time.Millisecond))
				unowned, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()

				EnsureRemaining(unowned, tb, 10*time.Millisecond)
			})

			require.Equal(t,
				"testcontext: context is not derived from this test's context; are you using context.Background()?",
				tb.fatal(),
			)
		})
	})

	t.Run("fails for unowned context without earlier deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				GetOrCreate(tb, WithTimeout(5*time.Millisecond))

				EnsureRemaining(context.Background(), tb, 10*time.Millisecond)
			})

			require.Equal(t,
				"testcontext: context is not derived from this test's context; are you using context.Background()?",
				tb.fatal(),
			)
		})
	})

	t.Run("fails for non-positive minimum remaining", func(t *testing.T) {
		tb := newRecordingTB()
		tb.run(func() {
			ctx := GetOrCreate(tb, WithTimeout(5*time.Millisecond))

			EnsureRemaining(ctx, tb, 0)
		})

		require.Equal(t,
			"testcontext: min remaining must be positive: 0s",
			tb.fatal(),
		)
	})

	t.Run("safe concurrent calls", func(t *testing.T) {
		ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				EnsureRemaining(ctx, t, 10*time.Millisecond)
			})
		}
		wg.Wait()
	})
}

func TestGet(t *testing.T) {
	t.Run("reports false when no test context exists", func(t *testing.T) {
		ctx, ok := Get(t)

		require.False(t, ok)
		require.Nil(t, ctx)
	})

	t.Run("returns current test context", func(t *testing.T) {
		want := GetOrCreate(t)
		got, ok := Get(t)

		require.True(t, ok)
		require.True(t, got == want)
	})
}

func TestGetOrDefault(t *testing.T) {
	t.Run("returns testing context when no test context exists", func(t *testing.T) {
		require.True(t, GetOrDefault(t) == t.Context())
	})

	t.Run("returns current test context", func(t *testing.T) {
		want := GetOrCreate(t)

		require.True(t, GetOrDefault(t) == want)
	})
}

type recordingTB struct {
	testing.TB

	mu       sync.Mutex
	cleanups []func()
	fatals   []string
}

func newRecordingTB() *recordingTB {
	return &recordingTB{}
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Name() string {
	return "recordingTB"
}

func (r *recordingTB) Context() context.Context {
	return context.Background()
}

func (r *recordingTB) Cleanup(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, fn)
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) run(fn func()) {
	done := make(chan struct{})
	go func() {
		defer func() {
			r.runCleanups()
			close(done)
		}()
		fn()
	}()
	<-done
}

func (r *recordingTB) runCleanups() {
	r.mu.Lock()
	cleanups := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()

	for _, cleanup := range cleanups {
		cleanup()
	}
}

func (r *recordingTB) fatal() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fatals) == 0 {
		return ""
	}
	return r.fatals[len(r.fatals)-1]
}
