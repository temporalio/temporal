package await_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
)

func TestRequire_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	attempts := 0

	await.Require(t.Context(), t, func(t *await.T) {
		attempts++
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, 1, attempts, "condition should be called exactly once")
}

func TestRequire_RetriesUntilAttemptPasses(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fail  func(*await.T, int32)
		stops bool
	}{
		{
			name: "Errorf",
			fail: func(t *await.T, attempt int32) {
				t.Errorf("not ready: %d", attempt)
			},
		},
		{
			name:  "FailNow",
			stops: true,
			fail: func(t *await.T, _ int32) {
				t.FailNow()
			},
		},
		{
			name:  "Fatal",
			stops: true,
			fail: func(t *await.T, _ int32) {
				t.Fatal("not ready")
			},
		},
		{
			name:  "Fatalf",
			stops: true,
			fail: func(t *await.T, attempt int32) {
				t.Fatalf("not ready: %d", attempt)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts atomic.Int32
			var continuedAfterFailure atomic.Bool
			await.Require(t.Context(), t, func(t *await.T) {
				attempt := attempts.Add(1)
				if attempt < 3 {
					tc.fail(t, attempt)
					continuedAfterFailure.Store(true)
				}
			}, time.Second, 100*time.Millisecond)

			require.Equal(t, int32(3), attempts.Load())
			require.Equal(t, !tc.stops, continuedAfterFailure.Load())
		})
	}
}

func TestRequire_PropagatesParentContextValues(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "value")

	var got any
	await.Require(ctx, t, func(t *await.T) {
		got = t.Context().Value(contextKey{})
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, "value", got)
}

func TestRequire_SetsTimeoutContextDeadline(t *testing.T) {
	t.Parallel()

	longCtx, cancel := context.WithTimeout(testcontext.For(t), time.Minute)
	defer cancel()
	longDeadline, ok := longCtx.Deadline()
	require.True(t, ok)

	shortTimeout := 1 * time.Second

	var shortCtx context.Context
	await.Require(longCtx, t, func(t *await.T) {
		shortCtx = t.Context()
	}, shortTimeout, 100*time.Millisecond)

	require.NotNil(t, shortCtx)
	require.NotSame(t, longCtx, shortCtx)

	shortDeadline, ok := shortCtx.Deadline()
	require.True(t, ok)
	require.True(t, shortDeadline.Before(longDeadline))
	require.LessOrEqual(t, time.Until(shortDeadline), shortTimeout)
	require.Greater(t, time.Until(shortDeadline), shortTimeout-200*time.Millisecond)
}

func TestRequire_PollIntervalStartsAfterAttemptFinishes(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var attemptStarts []time.Time
	var attemptEnds []time.Time
	attemptDuration := 60 * time.Millisecond
	pollInterval := 100 * time.Millisecond

	await.Require(t.Context(), t, func(t *await.T) {
		attemptStarts = append(attemptStarts, time.Now())
		defer func() { attemptEnds = append(attemptEnds, time.Now()) }()

		time.Sleep(attemptDuration) //nolint:forbidigo // simulate attempt work to distinguish poll-after-start vs poll-after-end

		if attempts.Add(1) < 3 {
			t.Error("not ready")
		}
	}, time.Second, pollInterval)

	require.Equal(t, int32(3), attempts.Load())
	require.Len(t, attemptStarts, 3)
	require.Len(t, attemptEnds, 3)
	for i := 1; i < len(attemptStarts); i++ {
		gap := attemptStarts[i].Sub(attemptEnds[i-1])
		require.GreaterOrEqual(t, gap, pollInterval,
			"poll interval should run after attempt finishes (gap=%v < %v)", gap, pollInterval)
	}
}

func TestRequire_FailureScenarios(t *testing.T) {
	t.Run("retries failed attempts until await timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.For(t)
			var attempts atomic.Int32

			tb := newRecordingTB()
			tb.run(func() {
				await.Require(ctx, tb, func(t *await.T) {
					n := attempts.Add(1)
					t.Errorf("attempt %d failed", n)
				}, time.Second, 100*time.Millisecond)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 1s",
				"details:",
				"  attempts         = 11",
				"  attempt duration = avg 0µs, max 0µs",
			}, "\n"), tb.fatals())
			require.Equal(t, strings.Join([]string{
				"attempt errors:",
				"",
				"  --- attempt 1 ---",
				"    attempt 1 failed",
				"  ... 7 attempts omitted ...",
				"",
				"  --- attempt 9 ---",
				"    attempt 9 failed",
				"",
				"  --- attempt 10 ---",
				"    attempt 10 failed",
				"",
				"  --- attempt 11 ---",
				"    attempt 11 failed",
			}, "\n"), tb.errors())
		})
	})

	t.Run("cancels running attempt at await timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.For(t)

			tb := newRecordingTB()
			tb.run(func() {
				await.Require(ctx, tb, func(t *await.T) {
					<-t.Context().Done()
					if t.Context().Err() != context.DeadlineExceeded {
						t.Errorf("context error = %v", t.Context().Err())
					}
				}, 2*time.Second, time.Second)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 2s",
				"details:",
				"  await timeout    = 2s",
				"  attempts         = 1",
				"  attempt duration = avg 2s, max 2s",
			}, "\n"), tb.fatals())
		})
	})

	t.Run("retries after attempt deadline expires", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			attemptTimeoutEnv := 50 * time.Millisecond
			attemptTimeout := attemptTimeoutEnv * debug.TimeoutMultiplier
			pollInterval := 100 * time.Millisecond
			t.Setenv("TEMPORAL_AWAIT_ATTEMPT_TIMEOUT", attemptTimeoutEnv.String())

			ctx := testcontext.For(t)
			var attempts atomic.Int32
			var firstAttemptRemaining time.Duration

			tb := newRecordingTB()
			tb.run(func() {
				await.Require(ctx, tb, func(t *await.T) {
					if attempts.Add(1) == 1 {
						deadline, _ := t.Context().Deadline()
						firstAttemptRemaining = time.Until(deadline)
					}
					<-t.Context().Done()
				}, attemptTimeout+2*pollInterval, pollInterval)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 250ms",
				"details:",
				"  attempts         = 3",
				"  attempt timeout  = 2 (configured as 50ms)",
				"  attempt duration = avg 33ms, max 50ms",
			}, "\n"), tb.fatals())
			require.Equal(t, attemptTimeout, firstAttemptRemaining)
			require.Equal(t, int32(3), attempts.Load())
		})
	})

	t.Run("does not start another attempt after await timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.For(t)
			var attempts atomic.Int32

			tb := newRecordingTB()
			tb.run(func() {
				await.Require(ctx, tb, func(t *await.T) {
					attempts.Add(1)
					<-t.Context().Done() // block until timeout
				}, time.Second, 100*time.Millisecond)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 1s",
				"details:",
				"  await timeout    = 1s",
				"  attempts         = 1",
				"  attempt duration = avg 1s, max 1s",
			}, "\n"), tb.fatals())
			require.Equal(t, int32(1), attempts.Load())
		})
	})

	t.Run("reports parent context deadline as await limit", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			parentCtx, cancel := context.WithTimeout(testcontext.For(t), time.Second)
			defer cancel()

			tb := newRecordingTB()
			tb.run(func() {
				await.Require(parentCtx, tb, func(t *await.T) {
					deadline, ok := t.Context().Deadline()
					if !ok {
						t.Error("missing deadline")
					}
					if time.Until(deadline) > time.Second {
						t.Errorf("deadline = %v", deadline)
					}
					<-t.Context().Done()
					if t.Context().Err() != context.DeadlineExceeded {
						t.Errorf("context error = %v", t.Context().Err())
					}
				}, 2*time.Second, time.Second)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 1s",
				"details:",
				"  await timeout    = 1s (configured 2s; limited by parent context deadline)",
				"  attempts         = 1",
				"  attempt duration = avg 1s, max 1s",
				"  last failure     = parent context deadline",
			}, "\n"), tb.fatals())
		})
	})

	t.Run("reports test context extension cap as await limit", func(t *testing.T) {
		t.Setenv("TEMPORAL_AWAIT_ATTEMPT_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			tb := newRecordingTB()

			ctx := testcontext.For(tb, testcontext.WithTimeout(5*time.Second))

			tb.run(func() {
				await.Require(ctx, tb, func(t *await.T) {
					<-t.Context().Done()
				}, 3*time.Minute, time.Second)
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 2m0s",
				"details:",
				"  await timeout    = 2m0s (configured 3m0s; limited by test context extension cap)",
				"  attempts         = 11",
				"  attempt timeout  = 10 (configured as 10s)",
				"  attempt duration = avg 10s, max 10s",
				"  ctx extensions   = 1 (+1m55s total)",
				"    1. +1m55s after 0µs",
			}, "\n"), tb.fatals())
		})
	})

	t.Run("stops after parent context cancellation", func(t *testing.T) {
		t.Parallel()

		parentCtx, cancel := context.WithCancel(testcontext.For(t))
		defer cancel()
		var attempts atomic.Int32

		tb := newRecordingTB()
		tb.run(func() {
			await.Require(parentCtx, tb, func(t *await.T) {
				attempts.Add(1)
				t.Error("not ready")
				cancel()
			}, time.Second, 100*time.Millisecond)
		})

		require.True(t, tb.Failed())
		require.Equal(t, "Require: context canceled before condition was satisfied: context canceled", tb.fatals())

		require.Equal(t, int32(1), attempts.Load(), "expected cancellation to stop polling")
	})

	t.Run("uses Requiref message on await timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.For(t)

			tb := newRecordingTB()
			tb.run(func() {
				await.Requiref(ctx, tb, func(t *await.T) {
					t.Error("not ready")
				}, time.Second, 100*time.Millisecond, "workflow %s not ready", "wf-123")
			})

			require.True(t, tb.Failed())
			require.Equal(t, strings.Join([]string{
				"Requiref: workflow wf-123 not ready (not satisfied after 1s)",
				"details:",
				"  attempts         = 11",
				"  attempt duration = avg 0µs, max 0µs",
			}, "\n"), tb.fatals())
		})
	})

	t.Run("propagates panic from attempt", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "unexpected nil pointer", func() {
			await.Require(t.Context(), t, func(_ *await.T) {
				panic("unexpected nil pointer")
			}, time.Second, 100*time.Millisecond)
		})
	})

	t.Run("detects real TB misuse", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name     string
			misuse   func(*recordingTB)
			expected string
		}{
			{
				name:   "Fatal stops real TB",
				misuse: func(tb *recordingTB) { tb.Fatal("wrong t used") },
				expected: strings.Join([]string{
					"wrong t used",
					"Require: the test was marked failed directly — use the *await.T passed to the callback, not s.T() or suite assertion methods",
				}, "\n"),
			},
			{
				name:     "Errorf marks real TB failed",
				misuse:   func(tb *recordingTB) { tb.Errorf("assert-style misuse") },
				expected: "Require: the test was marked failed directly — use the *await.T passed to the callback, not s.T() or suite assertion methods",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				ctx := testcontext.For(t)

				tb := newRecordingTB()
				tb.run(func() {
					await.Require(ctx, tb, func(_ *await.T) {
						tc.misuse(tb)
					}, time.Second, 100*time.Millisecond)
				})

				require.True(t, tb.Failed())
				require.Equal(t, tc.expected, tb.fatals())
			})
		}
	})

	t.Run("skips await after prior failure", func(t *testing.T) {
		t.Parallel()

		ctx := testcontext.For(t)
		conditionCalled := false

		tb := newRecordingTB()
		tb.run(func() {
			tb.Errorf("previous failure")
			await.Require(ctx, tb, func(_ *await.T) {
				conditionCalled = true
			}, time.Second, 100*time.Millisecond)
		})

		require.True(t, tb.Failed())
		require.Empty(t, tb.fatals())
		require.False(t, conditionCalled, "condition should not run when test already failed")
	})
}

func TestRequire_SoftDeadlockLogsAndCancels(t *testing.T) {
	// not using T.Parallel() so it can use t.Setenv to override the deadlock timeouts
	t.Setenv("TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT", "50ms")
	t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "5s")

	const awaitTimeout = 10 * time.Second

	ctx := testcontext.For(t)
	tb := newRecordingTB()
	start := time.Now()
	tb.run(func() {
		// Await timeout is long so parent-cancel doesn't beat the soft timer.
		await.Require(ctx, tb, func(t *await.T) {
			<-t.Context().Done() // exits as soon as soft cancel fires
		}, awaitTimeout, 100*time.Millisecond)
	})
	elapsed := time.Since(start)
	require.False(t, tb.Failed(), "soft deadlock + clean exit should succeed")
	require.Contains(t, tb.logs(), "soft deadlock")
	require.NotContains(t, tb.fatals(), "still running")
	require.Less(t, elapsed, awaitTimeout,
		"should return shortly after soft cancel, not wait the full await timeout (elapsed=%v)", elapsed)
}

func TestRequire_DeadlockDetected(t *testing.T) {
	// not using T.Parallel() so it can use t.Setenv to override the deadlock timeouts.
	// Await timeout is long enough that the soft timer fires before parent cancellation,
	// so the path is: soft fires → log + cancel → condition still running → hard fires.
	t.Setenv("TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT", "50ms")
	t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "100ms")

	const awaitTimeout = 10 * time.Second

	ctx := testcontext.For(t)
	tb := newRecordingTB()
	start := time.Now()
	tb.run(func() {
		await.Require(ctx, tb, func(*await.T) {
			select {} // ignores t.Context()
		}, awaitTimeout, 50*time.Millisecond)
	})
	elapsed := time.Since(start)
	require.True(t, tb.Failed())
	require.Contains(t, tb.logs(), "soft deadlock")
	require.Equal(t,
		"Require: condition still running 100ms past context cancellation — does it honor t.Context()? (1 attempts)",
		tb.fatals(),
	)
	require.Less(t, elapsed, awaitTimeout,
		"should fail at hard deadlock, not wait the full await timeout (elapsed=%v)", elapsed)
}

func TestRequire_WaitsForInFlightAttemptOnTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var finished atomic.Bool
		ctx := testcontext.For(t)
		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				<-t.Context().Done()
				finished.Store(true)
			}, time.Second, time.Second)
		})
		require.True(t, tb.Failed())
		require.Equal(t, strings.Join([]string{
			"Require: condition not satisfied after 1s",
			"details:",
			"  await timeout    = 1s",
			"  attempts         = 1",
			"  attempt duration = avg 1s, max 1s",
		}, "\n"), tb.fatals())
		require.True(t, finished.Load(), "Require returned before the running attempt exited")
	})
}

// recordingTB is a minimal testing.TB implementation for testing failure scenarios.
type recordingTB struct {
	testing.TB    // embed for interface satisfaction
	mu            sync.Mutex
	failed        atomic.Bool
	errorMessages []string
	fatalMessages []string
	logMessages   []string
	cleanups      []func()
}

func newRecordingTB() *recordingTB {
	return &recordingTB{}
}

func (r *recordingTB) Helper()      {}
func (r *recordingTB) Failed() bool { return r.failed.Load() }
func (r *recordingTB) Name() string { return "recordingTB" }
func (r *recordingTB) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logMessages = append(r.logMessages, fmt.Sprintf(format, args...))
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
	r.failed.Store(true)
	r.errorMessages = append(r.errorMessages, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed.Store(true)
	r.fatalMessages = append(r.fatalMessages, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Fatal(args ...any) {
	r.mu.Lock()
	r.failed.Store(true)
	r.fatalMessages = append(r.fatalMessages, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) FailNow() {
	r.failed.Store(true)
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

	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func (r *recordingTB) fatals() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.fatalMessages, "\n")
}

func (r *recordingTB) errors() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.errorMessages, "\n")
}

func (r *recordingTB) logs() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.logMessages, "\n")
}
