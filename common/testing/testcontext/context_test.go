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
			require.Equal(t, start.Add(maxTimeout), deadline)
			require.Equal(t, 90*time.Second, DefaultTimeout())
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

func TestCleanup(t *testing.T) {
	t.Parallel()

	t.Run("reports timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			timeout := time.Millisecond

			tb := newRecordingTB()
			tb.run(func() {
				ctx := For(tb, WithTimeout(time.Millisecond))
				<-ctx.Done() // let the deadline pass
			})

			require.Equal(t, fmt.Sprintf("test exceeded timeout of %v", timeout), tb.error())
		})
	})

	t.Run("reports extended effective timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			timeout := DefaultTimeout() + 10*time.Second

			tb := newRecordingTB()
			tb.run(func() {
				ctx := For(tb)
				EnsureRemaining(ctx, tb, timeout)
				<-ctx.Done()
			})

			require.Equal(t, fmt.Sprintf("test exceeded timeout of %v", timeout), tb.error())
		})
	})
}

func TestEnvTimeout(t *testing.T) {
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

	t.Run("environment timeout remains the active expiration", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				<-For(tb).Done()
			})

			require.Equal(t, "test exceeded timeout of 10s", tb.error())
		})
	})

	t.Run("remains extendable, unlike an explicit WithTimeout", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			ctx := For(t)

			// TEMPORAL_TEST_TIMEOUT only raises the baseline; it must not pin
			// a hard ceiling the way WithTimeout does.
			EnsureRemaining(ctx, t, time.Minute)

			require.Same(t, ctx, For(t))
			time.Sleep(11 * time.Second) //nolint:forbidigo // advance past the environment timeout
			require.NoError(t, ctx.Err())
		})
	})
}

func TestEnsureRemaining(t *testing.T) {
	t.Parallel()

	t.Run("extends when remaining time is too short", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)
			ceiling, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(maxTimeout), ceiling)

			EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)

			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, ceiling, deadline)
			require.Same(t, ctx, For(t))

			time.Sleep(DefaultTimeout() + time.Second) //nolint:forbidigo // advance past the original active expiration
			require.NoError(t, ctx.Err())
		})
	})

	t.Run("caps ensured remaining time", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			tb := newRecordingTB()
			tb.run(func() {
				ctx := For(tb)
				ceiling, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, start.Add(maxTimeout), ceiling)

				EnsureRemaining(ctx, tb, 10*time.Minute)

				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, ceiling, deadline)
				<-ctx.Done()
			})

			require.Equal(t, fmt.Sprintf("test exceeded timeout of %v", maxTimeout), tb.error())
		})
	})

	t.Run("does not extend beyond an explicitly requested timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(100*time.Millisecond))

			EnsureRemaining(ctx, t, 10*time.Minute)

			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), deadline)
			require.Same(t, ctx, For(t), "context should not have been replaced")
		})
	})

	t.Run("keeps a caller-derived context's own deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)

			// The caller wrapped the test context with its own, tighter
			// deadline (e.g. context.WithTimeout(env.Context(), ...)).
			// Replacing it would silently discard that wrapping, so extension
			// must leave the derived context intact.
			derived, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			EnsureRemaining(derived, t, DefaultTimeout()+10*time.Second)

			deadline, ok := derived.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline, "the caller's tighter deadline still governs")
		})
	})

	t.Run("leaves a foreign context unchanged", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()
			tb.run(func() {
				For(tb, WithTimeout(5*time.Millisecond))

				withDeadline, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				originalDeadline, ok := withDeadline.Deadline()
				require.True(t, ok)

				// Neither context is derived from a test context - e.g. a
				// standalone RPC context built from context.Background().
				// Extending is an optimization, so it isn't in a position to
				// fail the call; leave the caller's own deadline and
				// cancellation intact.
				EnsureRemaining(withDeadline, tb, 10*time.Millisecond)
				deadline, ok := withDeadline.Deadline()
				require.True(t, ok)
				require.Equal(t, originalDeadline, deadline)
				EnsureRemaining(context.Background(), tb, 10*time.Millisecond)
			})

			require.Empty(t, tb.fatal())
		})
	})

	t.Run("accepts the testing context unchanged", func(t *testing.T) {
		t.Parallel()

		For(t)

		// t.Context() is the test context's parent, so it carries no owner
		// marker and has no deadline to extend.
		EnsureRemaining(t.Context(), t, DefaultTimeout())
	})

	t.Run("extends a context belonging to a different tb", func(t *testing.T) {
		t.Parallel()

		// Mirrors the dominant pattern in tests/: a suite's context is
		// handed to a subtest, whose own tb differs from the one the
		// context was created for. Ownership is resolved from ctx, not
		// tb, so this must extend the owning (here, parent) state rather
		// than failing or silently no-op'ing.
		synctest.Test(t, func(t *testing.T) {
			other := For(t)

			tb := newRecordingTB()
			tb.run(func() {
				For(tb) // tb has its own, unrelated state too

				EnsureRemaining(other, tb, DefaultTimeout()+10*time.Second)
			})

			require.Empty(t, tb.fatal())
			time.Sleep(DefaultTimeout() + time.Second) //nolint:forbidigo // advance past the original active expiration
			require.NoError(t, other.Err())

			// The extension is visible to the owning test too.
			require.Same(t, other, For(t))
		})
	})

	t.Run("fails for non-positive minimum remaining", func(t *testing.T) {
		t.Parallel()

		tb := newRecordingTB()
		tb.run(func() {
			ctx := For(tb, WithTimeout(5*time.Millisecond))

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
			ctx := For(t)

			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					EnsureRemaining(ctx, t, DefaultTimeout()+10*time.Second)
				})
			}
			wg.Wait()

			// All callers extend the same cached context.
			require.Same(t, ctx, For(t))
			time.Sleep(DefaultTimeout() + time.Second) //nolint:forbidigo // advance past the original active expiration
			require.NoError(t, ctx.Err())
		})
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
