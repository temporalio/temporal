package testcontext

import (
	"context"
	"fmt"
	"strings"
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

			extension := EnsureRemaining(t, 250*time.Millisecond)
			require.Equal(t, 150*time.Millisecond, extension.Granted)
			require.Equal(t, start.Add(250*time.Millisecond), extension.Deadline)

			refreshed := For(t)
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

			extension := EnsureRemaining(t, 10*time.Minute)
			require.Equal(t, maxTestTimeout()-100*time.Millisecond, extension.Granted)
			require.Equal(t, start.Add(maxTestTimeout()), extension.Deadline)
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

		extension := EnsureRemaining(t, time.Second)
		require.Positive(t, extension.Granted)

		refreshed := For(t)
		require.Equal(t, "decorated", refreshed.Value(key{}))
	})

	t.Run("preserves test name metadata", func(t *testing.T) {
		For(t, WithTimeout(100*time.Millisecond))
		extension := EnsureRemaining(t, time.Second)
		require.Positive(t, extension.Granted)

		refreshed := For(t)
		md, ok := metadata.FromOutgoingContext(refreshed)
		require.True(t, ok)
		require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
	})

	t.Run("preserves original configured timeout", func(t *testing.T) {
		For(t, WithTimeout(100*time.Millisecond))
		extension := EnsureRemaining(t, time.Second)
		require.Positive(t, extension.Granted)

		For(t, WithTimeout(100*time.Millisecond))
	})

	t.Run("records extension grants", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(5*time.Millisecond))
			extension1 := EnsureRemaining(t, 10*time.Millisecond)
			require.Equal(t, 5*time.Millisecond, extension1.Granted)
			extension2 := EnsureRemaining(t, 20*time.Millisecond)
			require.Equal(t, 10*time.Millisecond, extension2.Granted)

			require.Equal(t, []time.Duration{
				5 * time.Millisecond,
				10 * time.Millisecond,
			}, extensionGrants(t))
		})
	})

	t.Run("recognizes older context after repeated extensions", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			original := For(t, WithTimeout(5*time.Millisecond))

			extension := EnsureRemaining(t, 10*time.Millisecond)
			firstRefresh := extension.Context()
			require.True(t, extension.AppliesTo(original))
			require.True(t, extension.AppliesTo(firstRefresh))

			extension = EnsureRemaining(t, 20*time.Millisecond)
			require.True(t, extension.AppliesTo(original))
			require.True(t, extension.AppliesTo(firstRefresh))
			require.True(t, extension.AppliesTo(extension.Context()))
		})
	})

	t.Run("reports extension grant time below millisecond", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(5*time.Millisecond))
			timer := time.NewTimer(94 * time.Microsecond)
			<-timer.C
			extension := EnsureRemaining(t, 10*time.Millisecond)
			require.Positive(t, extension.Granted)

			require.Contains(t, extensionReport(t), "after 94µs")
		})
	})

	t.Run("records denied extensions", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(5*time.Second))
			extension := EnsureRemaining(t, 10*time.Minute)
			require.Positive(t, extension.Granted)
			extension = EnsureRemaining(t, 10*time.Minute)
			require.Zero(t, extension.Granted)
			extension = EnsureRemaining(t, 10*time.Minute)
			require.Zero(t, extension.Granted)

			require.Contains(t, extensionReport(t), "2 context extensions denied")
		})
	})

	t.Run("reports only denied extensions", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(maxTestTimeout()))
			extension := EnsureRemaining(t, 10*time.Minute)
			require.Zero(t, extension.Granted)

			require.Equal(t, "1 context extension denied", extensionReport(t))
		})
	})

	t.Run("does not record sufficient remaining time as denied", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			For(t, WithTimeout(20*time.Millisecond))
			extension := EnsureRemaining(t, 10*time.Millisecond)
			require.Zero(t, extension.Granted)

			require.Empty(t, extensionReport(t))
		})
	})

	t.Run("safe concurrent calls", func(t *testing.T) {
		For(t, WithTimeout(100*time.Millisecond))

		var wg sync.WaitGroup
		var negative atomic.Int32
		for range 8 {
			wg.Go(func() {
				extension := EnsureRemaining(t, 10*time.Millisecond)
				if extension.Granted < 0 {
					negative.Add(1)
				}
			})
		}
		wg.Wait()
		require.Zero(t, negative.Load())
	})
}

func TestTimeoutExceededCleanupMessage(t *testing.T) {
	t.Run("original timeout", func(t *testing.T) {
		tb := newRecordingTB()

		synctest.Test(t, func(t *testing.T) {
			ctx := For(tb, WithTimeout(5*time.Second))
			<-ctx.Done()
			tb.runCleanups()
		})

		timeout := 5 * time.Second * debug.TimeoutMultiplier
		require.Equal(t, fmt.Sprintf("context deadline exceeded: test exceeded timeout of %v", timeout), tb.errors())
	})

	t.Run("extended timeout", func(t *testing.T) {
		tb := newRecordingTB()

		synctest.Test(t, func(t *testing.T) {
			ctx := For(tb, WithTimeout(5*time.Second))
			extension := EnsureRemaining(tb, 10*time.Second)
			require.Equal(t, 5*time.Second, extension.Granted)

			refreshed := For(tb)
			<-refreshed.Done()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded, "old context keeps its original deadline")
			tb.runCleanups()
		})

		original := 5 * time.Second * debug.TimeoutMultiplier
		extended := 10 * time.Second
		require.Equal(t, strings.Join([]string{
			fmt.Sprintf("context deadline exceeded: test exceeded extended timeout of %v (originally %v)", extended, original),
			"ctx extensions   = 1 (+5s total)",
			"  1. +5s after 0µs",
		}, "\n"), tb.errors())
	})

	t.Run("extension cap", func(t *testing.T) {
		tb := newRecordingTB()

		synctest.Test(t, func(t *testing.T) {
			For(tb, WithTimeout(5*time.Second))
			extension := EnsureRemaining(tb, 10*time.Minute)
			require.Positive(t, extension.Granted)

			refreshed := For(tb)
			<-refreshed.Done()
			tb.runCleanups()
		})

		original := 5 * time.Second * debug.TimeoutMultiplier
		requested := 10*time.Minute - original
		require.Equal(t,
			strings.Join([]string{
				fmt.Sprintf("context deadline exceeded: test exceeded test context extension cap of %v (originally %v, extensions requested total %v)", maxTestTimeout(), original, requested),
				fmt.Sprintf("ctx extensions   = 1 (+%v total)", maxTestTimeout()-original),
				fmt.Sprintf("  1. +%v after 0µs", maxTestTimeout()-original),
			}, "\n"),
			tb.errors(),
		)
	})
}

func extensionGrants(tb testing.TB) []time.Duration {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return nil
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	grants := make([]time.Duration, 0, len(st.extensionGrants))
	for _, grant := range st.extensionGrants {
		grants = append(grants, grant.duration)
	}
	return grants
}

func extensionReport(tb testing.TB) string {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return ""
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	return st.extensionReportLocked()
}

type recordingTB struct {
	testing.TB
	mu            sync.Mutex
	errorMessages []string
	cleanups      []func()
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

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorMessages = append(r.errorMessages, fmt.Sprintf(format, args...))
}

func (r *recordingTB) runCleanups() {
	r.mu.Lock()
	cleanups := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func (r *recordingTB) errors() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.errorMessages, "\n")
}
