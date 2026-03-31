package await

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestT_Errorf(t *testing.T) {
	at := &T{TB: t}
	at.Errorf("error: %s", "details")
	require.Equal(t, []string{"error: details"}, at.errors)
}

func TestT_Error(t *testing.T) {
	at := &T{TB: t}
	at.Error("error:", "details")
	require.Equal(t, []string{"error: details"}, at.errors)
}

func TestT_Errorf_CollectsAll(t *testing.T) {
	at := &T{TB: t}
	at.Errorf("first: %d", 1)
	at.Errorf("second: %d", 2)
	require.Equal(t, []string{"first: 1", "second: 2"}, at.errors)
	require.True(t, at.Failed())
}

func TestT_Fail(t *testing.T) {
	at := &T{TB: t}
	at.Fail()
	require.True(t, at.Failed())
}

func TestT_Helper(t *testing.T) {
	at := &T{TB: t}
	// Just verify it doesn't panic
	at.Helper()
}

func TestRequire_ImmediateSuccess(t *testing.T) {
	called := 0
	Require(t, func(_ context.Context, t *T) {
		called++
		require.True(t, true) //nolint:testifylint
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, 1, called, "condition should be called exactly once")
}

func TestRequire_ScalesTimeoutContextDeadline(t *testing.T) {
	t.Setenv(awaitScaleEnv, "4")

	Require(t, func(ctx context.Context, t *T) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Greater(t, time.Until(deadline), 1500*time.Millisecond)
	}, 500*time.Millisecond, time.Millisecond)
}

func TestRequire_EventualSuccess(t *testing.T) {
	var counter atomic.Int32
	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		counter.Store(42)
	}()

	Require(t, func(_ context.Context, t *T) {
		require.Equal(t, int32(42), counter.Load())
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_MultipleAssertions(t *testing.T) {
	type state struct {
		count  atomic.Int32
		ready  atomic.Bool
		status atomic.Value
	}

	s := &state{}
	s.status.Store("initializing")

	go func() {
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
		s.count.Store(5)
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
		s.status.Store("ready")
		s.ready.Store(true)
	}()

	Require(t, func(_ context.Context, t *T) {
		require.True(t, s.ready.Load(), "should be ready")
		require.Equal(t, int32(5), s.count.Load(), "count should be 5")
		require.Equal(t, "ready", s.status.Load().(string), "status should be ready")
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_RetriesUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(_ context.Context, t *T) {
		attempts.Add(1)
		require.True(t, ready.Load())
	}, time.Second, 10*time.Millisecond)

	require.Greater(t, attempts.Load(), int32(1), "should have retried multiple times")
}

func TestRequire_FailNowStopsIteration(t *testing.T) {
	// When require.* fails, it calls FailNow which should stop the current
	// iteration but allow retry
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(_ context.Context, t *T) {
		attempts.Add(1)
		if !ready.Load() {
			require.Fail(t, "not ready yet")
			// This line should not execute after FailNow in require.Fail
			t.Error("should not reach here")
		}
	}, time.Second, 10*time.Millisecond)

	require.True(t, ready.Load())
	require.Greater(t, attempts.Load(), int32(1))
}

func TestRequire_CodeAfterFailureNotExecuted(t *testing.T) {
	var reachedAfterFailure atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(_ context.Context, t *T) {
		require.True(t, ready.Load(), "not ready")
		// This should only execute on the successful iteration
		reachedAfterFailure.Add(1)
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), reachedAfterFailure.Load(),
		"code after failing require should only run once (on success)")
}

func TestRequire_ErrorRetries(t *testing.T) {
	var attempts atomic.Int32

	Require(t, func(_ context.Context, t *T) {
		if attempts.Add(1) < 3 {
			t.Error("not ready")
		}
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequire_FailRetries(t *testing.T) {
	var attempts atomic.Int32

	Require(t, func(_ context.Context, t *T) {
		if attempts.Add(1) < 3 {
			t.Fail()
			return
		}
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequire_FatalRetries(t *testing.T) {
	var attempts atomic.Int32

	Require(t, func(_ context.Context, t *T) {
		if attempts.Add(1) < 3 {
			t.Fatal("not ready")
		}
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequire_FatalfRetries(t *testing.T) {
	var attempts atomic.Int32

	Require(t, func(_ context.Context, t *T) {
		if attempts.Add(1) < 3 {
			t.Fatalf("not ready: %d", attempts.Load())
		}
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequire_TickTimingRespected(t *testing.T) {
	// Verify that poll interval is actually respected — a slow condition
	// should not compress the interval between attempts.
	var attempts atomic.Int32
	start := time.Now()

	Require(t, func(_ context.Context, t *T) {
		n := attempts.Add(1)
		if n < 4 {
			require.Fail(t, "not yet")
		}
	}, time.Second, 25*time.Millisecond)

	elapsed := time.Since(start)
	// 3 failures × 25ms poll = at least 75ms before 4th attempt succeeds
	require.GreaterOrEqual(t, elapsed, 60*time.Millisecond, "should respect poll interval")
	require.Equal(t, int32(4), attempts.Load())
}

// fakeTB is a minimal testing.TB implementation for testing failure scenarios.
type fakeTB struct {
	testing.TB // embed for interface satisfaction
	mu         sync.Mutex
	failed     bool
	errors     []string
	fatals     []string
}

func (f *fakeTB) Helper()             {}
func (f *fakeTB) Name() string        { return "fake" }
func (f *fakeTB) Failed() bool        { f.mu.Lock(); defer f.mu.Unlock(); return f.failed }
func (f *fakeTB) Cleanup(fn func())   {}
func (f *fakeTB) Logf(string, ...any) {}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	f.failed = true
	f.fatals = append(f.fatals, fmt.Sprintf(format, args...))
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeTB) FailNow() {
	f.mu.Lock()
	f.failed = true
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeTB) hasFatal(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.fatals {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func (f *fakeTB) hasError(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.errors {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func runWithFakeTB(fn func(tb *fakeTB)) *fakeTB {
	tb := &fakeTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
	return tb
}

func TestScaledDurations_Default(t *testing.T) {
	timeout, pollInterval := scaledDurations(t, time.Second, 100*time.Millisecond)
	require.Equal(t, time.Second, timeout)
	require.Equal(t, 100*time.Millisecond, pollInterval)
}

func TestScaledDurations_FromEnv(t *testing.T) {
	t.Setenv(awaitScaleEnv, "1.5")

	timeout, pollInterval := scaledDurations(t, time.Second, 100*time.Millisecond)
	require.Equal(t, 1500*time.Millisecond, timeout)
	require.Equal(t, 150*time.Millisecond, pollInterval)
}

func TestScaledDurations_InvalidEnv(t *testing.T) {
	t.Setenv(awaitScaleEnv, "nope")

	tb := runWithFakeTB(func(tb *fakeTB) {
		scaledDurations(tb, time.Second, time.Millisecond)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("invalid "+awaitScaleEnv), "expected invalid scale fatal message")
}

func TestRequire_TimeoutFailsTest(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ context.Context, t *T) {
			require.True(t, false, "never succeeds") //nolint:testifylint
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("not satisfied after"), "expected timeout fatal message")
}

func TestRequire_CancelsContextOnTimeout(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(ctx context.Context, t *T) {
			<-ctx.Done()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		}, 50*time.Millisecond, time.Second)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("not satisfied after"), "expected timeout fatal message")
}

func TestRequire_TimeoutReportsAllErrors(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ context.Context, t *T) {
			require.Equal(t, "expected", "actual", "values must match")
			require.Equal(t, 1, 2, "numbers must match")
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	// The first require.Equal fails and calls FailNow, so only that error is
	// captured per attempt. But it should be reported.
	require.True(t, tb.hasError("last attempt errors"), "expected errors in output")
}

func TestRequiref_IncludesMessageOnTimeout(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Requiref(tb, func(_ context.Context, t *T) {
			require.True(t, false) //nolint:testifylint
		}, 50*time.Millisecond, 10*time.Millisecond, "workflow %s not ready", "wf-123")
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("workflow wf-123 not ready"), "expected custom message in fatal")
}

func TestRequire_PanicPropagated(t *testing.T) {
	// A panic in the condition should propagate immediately, not be
	// silently retried until timeout.
	require.PanicsWithValue(t, "unexpected nil pointer", func() {
		Require(t, func(_ context.Context, _ *T) {
			panic("unexpected nil pointer")
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
}

func TestRequire_DetectsMisuseOfRealT(t *testing.T) {
	// Simulate someone accidentally using require.X(realT, ...) inside the
	// callback. This calls realT.Errorf (marks failed) then realT.FailNow
	// (Goexit). Require should detect the misuse via tb.Failed().
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ context.Context, _ *T) {
			tb.Errorf("wrong t used")
			tb.FailNow() // simulates require.X calling FailNow on the real TB
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequire_DetectsMisuseOnSuccessPath(t *testing.T) {
	// When assert.X(s.T(), ...) is used inside the callback, Errorf is called
	// on the real test (marking it failed) but FailNow is NOT called, so the
	// condition appears to pass. Require should still detect this.
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ context.Context, _ *T) {
			// Simulates assert.Equal(t, ...) — marks failed, no FailNow.
			tb.Errorf("assert-style misuse")
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequire_SkipsWhenAlreadyFailed(t *testing.T) {
	conditionCalled := false
	tb := runWithFakeTB(func(tb *fakeTB) {
		tb.Errorf("previous failure") // mark as failed before Require
		Require(tb, func(_ context.Context, _ *T) {
			conditionCalled = true
		}, time.Second, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.False(t, conditionCalled, "condition should not run when test already failed")
}

func TestRequire_NoGoroutineLeak(t *testing.T) {
	// Run Require, then verify no leftover goroutines from the polling loop.
	before := runtime.NumGoroutine()

	var ready atomic.Bool
	go func() {
		time.Sleep(30 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(_ context.Context, t *T) {
		require.True(t, ready.Load())
	}, time.Second, 10*time.Millisecond)

	// Give goroutines a moment to clean up
	time.Sleep(50 * time.Millisecond) //nolint:forbidigo
	after := runtime.NumGoroutine()

	// Allow a small delta for background runtime goroutines
	require.InDelta(t, before, after, 2, "goroutine count should not grow: before=%d after=%d", before, after)
}

func TestRequireTrue_ImmediateSuccess(t *testing.T) {
	called := 0
	RequireTrue(t, func(_ context.Context, _ *T) bool {
		called++
		return true
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, 1, called, "condition should be called exactly once")
}

func TestRequireTrue_EventualSuccess(t *testing.T) {
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	RequireTrue(t, func(_ context.Context, _ *T) bool {
		attempts.Add(1)
		return ready.Load()
	}, time.Second, 10*time.Millisecond)

	require.True(t, ready.Load())
	require.Greater(t, attempts.Load(), int32(1))
}

func TestRequireTrue_TimeoutFailsTest(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(_ context.Context, _ *T) bool {
			return false
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("not satisfied after"), "expected timeout fatal message")
}

func TestRequireTrue_CancelsContextOnTimeout(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(ctx context.Context, _ *T) bool {
			<-ctx.Done()
			return true
		}, 50*time.Millisecond, time.Second)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("not satisfied after"), "expected timeout fatal message")
}

func TestRequireTruef_IncludesMessageOnTimeout(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTruef(tb, func(_ context.Context, _ *T) bool {
			return false
		}, 50*time.Millisecond, 10*time.Millisecond, "workflow %s not ready", "wf-123")
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("workflow wf-123 not ready"), "expected custom message in fatal")
}

func TestRequireTrue_PanicPropagated(t *testing.T) {
	require.PanicsWithValue(t, "unexpected nil pointer", func() {
		RequireTrue(t, func(_ context.Context, _ *T) bool {
			panic("unexpected nil pointer")
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
}

func TestRequireTrue_AssertionFailureRetries(t *testing.T) {
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	RequireTrue(t, func(_ context.Context, t *T) bool {
		attempts.Add(1)
		require.True(t, ready.Load())
		return true
	}, time.Second, 10*time.Millisecond)

	require.True(t, ready.Load())
	require.Greater(t, attempts.Load(), int32(1))
}

func TestRequireTrue_ErrorfRetriesEvenWhenConditionReturnsTrue(t *testing.T) {
	var attempts atomic.Int32

	RequireTrue(t, func(_ context.Context, t *T) bool {
		if attempts.Add(1) < 3 {
			t.Error("not ready")
			return true
		}
		return true
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequireTrue_FailRetriesEvenWhenConditionReturnsTrue(t *testing.T) {
	var attempts atomic.Int32

	RequireTrue(t, func(_ context.Context, t *T) bool {
		if attempts.Add(1) < 3 {
			t.Fail()
			return true
		}
		return true
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequireTrue_TimeoutReportsLastAssertionError(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(_ context.Context, t *T) bool {
			require.Equal(t, "expected", "actual", "values must match")
			return true
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasError("last attempt errors"), "expected errors in output")
}

func TestRequireTrue_DetectsAssertionMisuseOfRealT(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(_ context.Context, _ *T) bool {
			tb.Errorf("wrong t used")
			tb.FailNow()
			return false
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequireTrue_DetectsAssertionMisuseOnFalsePath(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(_ context.Context, _ *T) bool {
			tb.Errorf("assert-style misuse")
			return false
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequireTrue_DetectsAssertionMisuseOnSuccessPath(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		RequireTrue(tb, func(_ context.Context, _ *T) bool {
			tb.Errorf("assert-style misuse")
			return true
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequireTrue_SkipsWhenAlreadyFailed(t *testing.T) {
	conditionCalled := false
	tb := runWithFakeTB(func(tb *fakeTB) {
		tb.Errorf("previous failure")
		RequireTrue(tb, func(_ context.Context, _ *T) bool {
			conditionCalled = true
			return true
		}, time.Second, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.False(t, conditionCalled, "condition should not run when test already failed")
}
