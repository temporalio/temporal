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
)

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := New(t, WithTimeout(time.Second))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, start.Add(time.Second), deadline)
	})
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

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := New(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(10*time.Second), deadline)
		})
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := New(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

func TestExtend(t *testing.T) {
	t.Run("extends deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := New(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			newDeadline, granted := Extend(t, 250*time.Millisecond)
			require.Equal(t, 250*time.Millisecond, granted)
			require.Equal(t, originalDeadline.Add(250*time.Millisecond), newDeadline)

			refreshed := New(t)
			refreshedDeadline, ok := refreshed.Deadline()
			require.True(t, ok)
			require.Equal(t, newDeadline, refreshedDeadline)
		})
	})

	t.Run("caps extension", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := New(t, WithTimeout(100*time.Millisecond))
			originalDeadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(100*time.Millisecond), originalDeadline)

			newDeadline, granted := Extend(t, 10*time.Minute)
			require.Equal(t, maxTestTimeout()-100*time.Millisecond, granted)
			require.Equal(t, start.Add(maxTestTimeout()), newDeadline)
		})
	})

	t.Run("replays decorators", func(t *testing.T) {
		type key struct{}

		ctx := New(t,
			WithTimeout(100*time.Millisecond),
			WithContextDecorator(key{}, func(ctx context.Context) context.Context {
				return context.WithValue(ctx, key{}, "decorated")
			}),
		)
		require.Equal(t, "decorated", ctx.Value(key{}))

		_, granted := Extend(t, time.Second)
		require.Positive(t, granted)

		refreshed := New(t)
		require.Equal(t, "decorated", refreshed.Value(key{}))
	})

	t.Run("preserves original configured timeout", func(t *testing.T) {
		ctx := New(t, WithTimeout(100*time.Millisecond))
		_, granted := Extend(t, time.Second)
		require.Positive(t, granted)

		refreshed := New(t, WithTimeout(100*time.Millisecond))
		require.NotNil(t, ctx)
		require.NotNil(t, refreshed)
	})

	t.Run("safe concurrently", func(t *testing.T) {
		New(t, WithTimeout(100*time.Millisecond))

		var wg sync.WaitGroup
		var negative atomic.Int32
		for range 8 {
			wg.Go(func() {
				_, granted := Extend(t, 10*time.Millisecond)
				if granted < 0 {
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
			ctx := New(tb, WithTimeout(5*time.Second))
			<-ctx.Done()
			tb.runCleanups()
		})

		timeout := 5 * time.Second * debug.TimeoutMultiplier
		require.Equal(t, fmt.Sprintf("Test exceeded timeout of %v", timeout), tb.errors())
	})

	t.Run("extended timeout", func(t *testing.T) {
		tb := newRecordingTB()

		synctest.Test(t, func(t *testing.T) {
			ctx := New(tb, WithTimeout(5*time.Second))
			_, granted := Extend(tb, 10*time.Second)
			require.Equal(t, 10*time.Second, granted)

			refreshed := New(tb)
			<-refreshed.Done()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded, "old context keeps its original deadline")
			tb.runCleanups()
		})

		original := 5 * time.Second * debug.TimeoutMultiplier
		extended := original + 10*time.Second
		require.Equal(t, fmt.Sprintf("Test exceeded extended timeout of %v (originally %v)", extended, original), tb.errors())
	})

	t.Run("extension cap", func(t *testing.T) {
		tb := newRecordingTB()

		synctest.Test(t, func(t *testing.T) {
			New(tb, WithTimeout(5*time.Second))
			_, granted := Extend(tb, 10*time.Minute)
			require.Positive(t, granted)

			refreshed := New(tb)
			<-refreshed.Done()
			tb.runCleanups()
		})

		original := 5 * time.Second * debug.TimeoutMultiplier
		require.Equal(t,
			fmt.Sprintf("Test exceeded %v cap (originally %v, extensions requested total %v)", maxTestTimeout(), original, 10*time.Minute),
			tb.errors(),
		)
	})
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
