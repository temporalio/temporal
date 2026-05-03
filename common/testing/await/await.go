// Package await provides polling-based test assertions as a replacement
// for testify's Eventually, EventuallyWithT, and their formatted variants.
//
// Improvements over testify's eventually functions:
//
//   - Misuse detection: accidentally using the real *testing.T (e.g. s.T() or
//     suite assertion methods) instead of the callback's collect T is a
//     common mistake. This package detects it and fails with a clear message.
//
//   - Retryable assertions in bool predicates: RequireTrue treats assertion
//     failures on the callback's *await.T the same as a false result, so
//     transient failures are retried and only reported after timeout.
//
//   - Timeout-aware callbacks: callbacks receive a context that is canceled
//     when the await timeout or test deadline is reached, so RPCs and blocking
//     waits can exit instead of continuing after the retry window has expired.
//
//   - Environment scaling: TEMPORAL_TEST_AWAIT_SCALE scales timeouts and poll
//     intervals together, so CI can use larger budgets without increasing
//     polling load.
//
//   - Panic propagation: if the condition panics (e.g. nil dereference), the
//     panic is propagated immediately rather than being silently swallowed
//     or retried until timeout.
//     See https://github.com/stretchr/testify/issues/1810
//
//   - No goroutine leaks: testify's Eventually may return on timeout while
//     the condition goroutine is still running, causing "panic: Fail in
//     goroutine after Test has completed" crashes and data races. This
//     package waits for each attempt to finish before starting the next.
//     See https://github.com/stretchr/testify/issues/1611
//
//   - Condition always runs: testify's Eventually can fail without ever
//     running the condition due to a timer/ticker race with short timeouts.
//     This package runs the condition immediately on the first iteration.
//     See https://github.com/stretchr/testify/issues/1652
package await

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const awaitScaleEnv = "TEMPORAL_TEST_AWAIT_SCALE"

// T is passed to the condition callback. It intercepts assertion failures
// so the polling loop can retry. Pass it to require.* functions — it
// satisfies testing.TB.
//
// Only use T for assertions (require.*, t.Errorf, t.FailNow). Methods like
// t.Cleanup, t.Skip, and t.Setenv delegate to the real test and should not
// be called inside polling conditions.
type T struct {
	testing.TB
	errors []string
	failed bool
}

// Errorf records an error message for reporting on timeout.
func (t *T) Errorf(format string, args ...any) {
	t.Fail()
	t.errors = append(t.errors, fmt.Sprintf(format, args...))
}

// Error records an error message for reporting on timeout.
func (t *T) Error(args ...any) {
	t.Fail()
	t.errors = append(t.errors, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}

// Fail records a failure for this attempt without marking the real test failed.
func (t *T) Fail() {
	t.failed = true
}

// FailNow is called by require.* on failure. It triggers runtime.Goexit()
// which terminates the goroutine and is detected by Require to retry.
// Unlike testing.TB.FailNow(), this does NOT mark the test as failed.
func (t *T) FailNow() {
	t.Fail()
	runtime.Goexit()
}

// Fatal records an error message and stops this attempt.
func (t *T) Fatal(args ...any) {
	t.Error(args...)
	t.FailNow()
}

// Fatalf records an error message and stops this attempt.
func (t *T) Fatalf(format string, args ...any) {
	t.Errorf(format, args...)
	t.FailNow()
}

// Failed reports whether this attempt has failed.
func (t *T) Failed() bool {
	return t.failed
}

// Require runs condition repeatedly until it completes without assertion
// failures, or until the timeout expires. Set TEMPORAL_TEST_AWAIT_SCALE to
// scale the timeout and poll interval together. The scaled timeout is capped at
// the test's deadline if one is set.
//
// The condition receives a context that is canceled on timeout, plus an
// *await.T for assertions. Pass *await.T to require.* functions. When
// assertions fail, Require catches the failure and retries.
//
// A goroutine is used per attempt so that runtime.Goexit (called by
// require.FailNow) terminates only the attempt, not the test.
//
// Example:
//
//	await.Require(t, func(ctx context.Context, t *await.T) {
//	    resp, err := client.GetStatus(ctx)
//	    require.NoError(t, err)
//	    require.Equal(t, "ready", resp.Status)
//	}, 5*time.Second, 200*time.Millisecond)
func Require(tb testing.TB, condition func(context.Context, *T), timeout, pollInterval time.Duration) {
	tb.Helper()
	run(tb, func(ctx context.Context, t *T) bool {
		condition(ctx, t)
		return true
	}, timeout, pollInterval, "", "await.Require")
}

// Requiref is like Require but accepts a format string that is included in the
// failure message when the condition is not satisfied before the timeout.
//
// Example:
//
//	await.Requiref(t, func(ctx context.Context, t *await.T) {
//	    require.Equal(t, "ready", status.Load())
//	}, 5*time.Second, 200*time.Millisecond, "workflow %s did not reach ready state", wfID)
func Requiref(tb testing.TB, condition func(context.Context, *T), timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(tb, func(ctx context.Context, t *T) bool {
		condition(ctx, t)
		return true
	}, timeout, pollInterval, fmt.Sprintf(msg, args...), "await.Require")
}

// RequireTrue runs condition repeatedly until it returns true and all
// assertions pass, or until the timeout expires. The timeout is capped at the
// test's deadline if one is set.
//
// The condition receives an *await.T for assertions. In this bool-returning
// variant, failed assertions are treated like returning false.
func RequireTrue(tb testing.TB, condition func(context.Context, *T) bool, timeout, pollInterval time.Duration) {
	tb.Helper()
	run(tb, condition, timeout, pollInterval, "", "await.RequireTrue")
}

// RequireTruef is like RequireTrue but accepts a format string that is included
// in the failure message when the condition is not satisfied before the timeout.
func RequireTruef(tb testing.TB, condition func(context.Context, *T) bool, timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(tb, condition, timeout, pollInterval, fmt.Sprintf(msg, args...), "await.RequireTrue")
}

type boolAttempt struct {
	ok     bool
	goexit bool
}

func run(
	tb testing.TB,
	condition func(context.Context, *T) bool,
	timeout,
	pollInterval time.Duration,
	msg string,
	name string,
) {
	tb.Helper()

	// Skip if the test already failed — no point polling.
	if tb.Failed() {
		tb.Logf("%s: skipping (test already failed)", name)
		return
	}

	timeout, pollInterval = scaledDurations(tb, timeout, pollInterval)
	deadline := time.Now().Add(timeout)

	// Cap at the test's deadline if one is set, so we don't sleep past it.
	if d, ok := tb.(interface{ Deadline() (time.Time, bool) }); ok {
		if testDeadline, hasDeadline := d.Deadline(); hasDeadline && testDeadline.Before(deadline) {
			deadline = testDeadline
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	polls := 0

	for {
		polls++
		t := &T{TB: tb}

		// Run condition in a goroutine so that runtime.Goexit (called by
		// require.FailNow) terminates only this goroutine, not the test.
		//
		// Channel protocol:
		//   boolAttempt → condition returned or assertion failed via Goexit
		//   panicVal    → condition panicked (propagated to caller)
		done := make(chan any, 1)
		go func() {
			defer func() {
				// Order matters: recover() returns nil during Goexit,
				// so a non-nil value means a real panic.
				if r := recover(); r != nil {
					done <- r // propagate panic
					return
				}
				// If we reach here via Goexit (from FailNow), send goexit.
				// If condition completed normally, a result was already sent.
				select {
				case done <- boolAttempt{goexit: true}:
				default:
				}
			}()
			done <- boolAttempt{ok: condition(ctx, t)}
		}()

		result := <-done
		switch v := result.(type) {
		case boolAttempt:
			if tb.Failed() {
				tb.Fatalf("%s: the test was marked failed directly — "+
					"use the *await.T passed to the callback, not s.T() or suite assertion methods", name)
				return
			}
			if v.ok && !v.goexit && !t.Failed() && !deadlineReached(deadline) {
				return
			}
		default:
			// Condition panicked — propagate immediately.
			panic(v)
		}

		// Detect misuse: require.NoError(s.T(), ...) inside the callback marks
		// the real test as failed via Errorf then calls FailNow (Goexit).
		if tb.Failed() {
			tb.Fatalf("%s: the test was marked failed directly — "+
				"use the *await.T passed to the callback, not s.T() or suite assertion methods", name)
			return
		}

		if deadlineReached(deadline) {
			if len(t.errors) > 0 {
				tb.Errorf("last attempt errors:\n%s", strings.Join(t.errors, "\n"))
			}
			if msg != "" {
				tb.Fatalf("%s: %s (not satisfied after %v, %d polls)", name, msg, timeout, polls)
			} else {
				tb.Fatalf("%s: condition not satisfied after %v (%d polls)", name, timeout, polls)
			}
			return
		}

		sleep(ctx, deadline, pollInterval)
	}
}

func sleep(ctx context.Context, deadline time.Time, pollInterval time.Duration) {
	remaining := time.Until(deadline)
	if remaining < pollInterval {
		pollInterval = remaining
	}
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func deadlineReached(deadline time.Time) bool {
	return !time.Now().Before(deadline)
}

func scaledDurations(tb testing.TB, timeout, pollInterval time.Duration) (scaledTimeout, scaledPollInterval time.Duration) {
	tb.Helper()
	scale := awaitScale(tb)
	if scale == 1 {
		return timeout, pollInterval
	}
	return scaleDuration(timeout, scale), scaleDuration(pollInterval, scale)
}

func awaitScale(tb testing.TB) float64 {
	tb.Helper()
	raw := os.Getenv(awaitScaleEnv)
	if raw == "" {
		return 1
	}
	scale, err := strconv.ParseFloat(raw, 64)
	if err != nil || scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		tb.Fatalf("await: invalid %s=%q; expected a positive finite number", awaitScaleEnv, raw)
		return 1
	}
	return scale
}

func scaleDuration(d time.Duration, scale float64) time.Duration {
	if d <= 0 {
		return d
	}
	scaled := float64(d) * scale
	if scaled < 1 {
		return 1
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if scaled > float64(maxDuration) {
		return maxDuration
	}
	return time.Duration(scaled)
}
