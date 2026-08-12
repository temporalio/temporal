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
			require.Equal(t, start.Add(time.Second*debug.TimeoutMultiplier), deadline)
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

func TestCleanup(t *testing.T) {
	t.Parallel()

	t.Run("reports timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			timeout := time.Millisecond * debug.TimeoutMultiplier

			tb := newRecordingTB()
			tb.run(func() {
				ctx := GetOrCreate(tb, WithTimeout(time.Millisecond))
				<-ctx.Done() // let the deadline pass
			})

			require.Equal(t, fmt.Sprintf("test exceeded timeout of %v", timeout), tb.error())
		})
	})

	t.Run("leaves a canceled context behind", func(t *testing.T) {
		t.Parallel()

		tb := newRecordingTB()
		var st *contextState
		tb.run(func() {
			GetOrCreate(tb)

			testContexts.Lock()
			st = testContexts.byTest[tb]
			testContexts.Unlock()
		})

		// Helpers racing with teardown still hold the state: they must see a
		// canceled context, not a panic. Cleaning up twice is also harmless.
		timedOut, _ := st.cleanup()
		require.False(t, timedOut)
		require.ErrorIs(t, st.current.Err(), context.Canceled)

		// The test is deregistered, so lookups fall back to the testing context.
		require.Equal(t, tb.Context(), GetOrDefault(tb))
	})
}

func TestEnvTimeout(t *testing.T) {
	t.Run("from env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(10*time.Second*debug.TimeoutMultiplier), deadline)
		})
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second*debug.TimeoutMultiplier), deadline)
		})
	})
}

func TestEnsureRemaining(t *testing.T) {
	t.Parallel()

	t.Run("extends when remaining time is too short", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()), originalDeadline)

			refreshed := EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()+10*time.Second), refreshedDeadline)
		})
	})

	t.Run("caps ensured remaining time", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()), originalDeadline)

			refreshed := EnsureRemaining(ctx, t, 10*time.Minute*debug.TimeoutMultiplier)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(maxTimeout*debug.TimeoutMultiplier), refreshedDeadline)
		})
	})

	t.Run("does not extend beyond an explicitly requested timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))

			refreshed := EnsureRemaining(ctx, t, 10*time.Minute)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond*debug.TimeoutMultiplier), refreshedDeadline)
			require.Same(t, ctx, refreshed, "context should not have been replaced")
		})
	})

	t.Run("accepts a derived context", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)

			derived, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			refreshed := EnsureRemaining(derived, t, DefaultTimeout()+10*time.Second)

			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()+10*time.Second), refreshedDeadline)
		})
	})

	t.Run("replays decorators", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx := GetOrCreate(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		refreshed := EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

		require.NotSame(t, ctx, refreshed, "context should have been replaced")
		require.Equal(t, "decorated", refreshed.Value(key{}))
	})

	t.Run("preserves test name metadata", func(t *testing.T) {
		t.Parallel()

		ctx := GetOrCreate(t)
		refreshed := EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

		require.NotSame(t, ctx, refreshed, "context should have been replaced")
		md, ok := metadata.FromOutgoingContext(refreshed)
		require.True(t, ok)
		require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
	})

	t.Run("preserves original configured timeout", func(t *testing.T) {
		t.Parallel()

		ctx := GetOrCreate(t, WithTimeout(100*time.Millisecond))
		EnsureRemaining(ctx, t, time.Second)

		GetOrCreate(t, WithTimeout(100*time.Millisecond))
	})

	t.Run("recognizes older context after repeated extensions", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			original := GetOrCreate(t)

			firstRefresh := EnsureRemaining(original, t, DefaultTimeout()+10*time.Second)
			firstDeadline, ok := firstRefresh.Deadline()
			require.True(t, ok)
			require.Equal(t, time.Now().Add(DefaultTimeout()+10*time.Second), firstDeadline)

			// The original context is outdated by now, but still recognized.
			refreshed := EnsureRemaining(original, t, DefaultTimeout()+20*time.Second)
			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, time.Now().Add(DefaultTimeout()+20*time.Second), refreshedDeadline)
		})
	})

	t.Run("fails for unowned context with earlier deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				GetOrCreate(tb, WithTimeout(5*time.Millisecond))
				unowned, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()

				EnsureRemaining(unowned, tb, 10*time.Millisecond)
			})

			require.Equal(t, notDerivedMessage, tb.fatal())
		})
	})

	t.Run("fails for unowned context without earlier deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				GetOrCreate(tb, WithTimeout(5*time.Millisecond))

				EnsureRemaining(context.Background(), tb, 10*time.Millisecond)
			})

			require.Equal(t, notDerivedMessage, tb.fatal())
		})
	})

	t.Run("accepts the testing context unchanged", func(t *testing.T) {
		t.Parallel()

		GetOrCreate(t)

		// t.Context() is the test context's parent: nothing to extend, but not
		// a foreign context either.
		require.Same(t, t.Context(), EnsureRemaining(t.Context(), t, DefaultTimeout()))
	})

	t.Run("fails for another test's context", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			other := GetOrCreate(t)

			tb := newRecordingTB()
			tb.run(func() {
				GetOrCreate(tb)

				EnsureRemaining(other, tb, 10*time.Millisecond)
			})

			require.Equal(t, notDerivedMessage, tb.fatal())
		})
	})

	t.Run("fails for non-positive minimum remaining", func(t *testing.T) {
		t.Parallel()

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
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := GetOrCreate(t)

			var wg sync.WaitGroup
			refreshed := make([]context.Context, 8)
			for i := range refreshed {
				wg.Go(func() {
					refreshed[i] = EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)
				})
			}
			wg.Wait()

			// All callers observe the extended deadline, no matter who extended it.
			for _, got := range refreshed {
				deadline, ok := got.Deadline()
				require.True(t, ok)
				require.Equal(t, start.Add(DefaultTimeout()+10*time.Second), deadline)
			}
		})
	})
}

func TestGetOrDefault(t *testing.T) {
	t.Parallel()

	t.Run("returns testing context when no test context exists", func(t *testing.T) {
		t.Parallel()

		require.Same(t, t.Context(), GetOrDefault(t))
	})

	t.Run("returns current test context", func(t *testing.T) {
		t.Parallel()

		want := GetOrCreate(t)

		require.Same(t, want, GetOrDefault(t))
	})

	t.Run("returns extended test context", func(t *testing.T) {
		t.Parallel()

		ctx := GetOrCreate(t)
		want := EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

		require.Same(t, want, GetOrDefault(t))
	})
}

// recordingTB records failures instead of failing a real test.
//
// NOTE: it deliberately does not implement Deadline, so it stands in for a
// testing.TB without a `go test -timeout` deadline.
type recordingTB struct {
	testing.TB

	ctx       context.Context
	cancelCtx context.CancelFunc

	mu       sync.Mutex
	cleanups []func()
	fatals   []string
	errors   []string
}

func newRecordingTB() *recordingTB {
	// Like testing.T, hand out a dedicated context that is canceled once the
	// "test" is over - not context.Background().
	ctx, cancel := context.WithCancel(context.Background())
	return &recordingTB{ctx: ctx, cancelCtx: cancel}
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Name() string {
	return "recordingTB"
}

func (r *recordingTB) Context() context.Context {
	return r.ctx
}

func (r *recordingTB) Cleanup(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, fn)
}

func (r *recordingTB) Fatal(args ...any) {
	r.recordFatal(fmt.Sprint(args...))
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.recordFatal(fmt.Sprintf(format, args...))
}

func (r *recordingTB) Error(args ...any) {
	r.recordError(fmt.Sprint(args...))
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.recordError(fmt.Sprintf(format, args...))
}

func (r *recordingTB) recordFatal(msg string) {
	r.mu.Lock()
	r.fatals = append(r.fatals, msg)
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) recordError(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, msg)
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
	r.cancelCtx()
}

func (r *recordingTB) fatal() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fatals) == 0 {
		return ""
	}
	return r.fatals[len(r.fatals)-1]
}

func (r *recordingTB) error() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errors) == 0 {
		return ""
	}
	return r.errors[len(r.errors)-1]
}
