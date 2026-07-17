package await

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/util"
)

const requireMisuseHint = "use the *await.T passed to the callback, not s.T() or suite assertion methods"

// softDeadlockTimeoutEnvVar overrides the default soft-deadlock timeout.
// Parsed as a Go duration, e.g. "10s".
const softDeadlockTimeoutEnvVar = "TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT"

// defaultSoftDeadlockTimeout caps how long a single attempt can run before its
// context is cancelled (soft deadlock). Capped further by the overall await
// deadline. Each new attempt gets a fresh context with this same cap.
const defaultSoftDeadlockTimeout = 30 * time.Second

func softDeadlockTimeout() time.Duration {
	return effectiveEnvDuration(softDeadlockTimeoutEnvVar, defaultSoftDeadlockTimeout)
}

// hardDeadlockTimeoutEnvVar overrides the default hard-deadlock timeout.
// Parsed as a Go duration, e.g. "100ms".
const hardDeadlockTimeoutEnvVar = "TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT"

// defaultHardDeadlockTimeout is how long runAttempt waits AFTER cancelling the
// attempt context (soft deadlock) for the condition goroutine to honor the
// cancellation. If it doesn't terminate by then, the goroutine is declared
// hard-deadlocked and abandoned. Without it, a condition that ignores
// t.Context() would hang the test until go test -timeout fires.
const defaultHardDeadlockTimeout = 10 * time.Second

func hardDeadlockTimeout() time.Duration {
	return effectiveEnvDuration(hardDeadlockTimeoutEnvVar, defaultHardDeadlockTimeout)
}

// postAwaitTimeoutReserveEnvVar overrides the default post-await reserve.
// Parsed as a Go duration, e.g. "10s".
const postAwaitTimeoutReserveEnvVar = "TEMPORAL_AWAIT_POST_TIMEOUT_RESERVE"

// defaultPostAwaitTimeoutReserve is the minimum time for *after* Await returns.
const defaultPostAwaitTimeoutReserve = 10 * time.Second

func postAwaitTimeoutReserve() time.Duration {
	return effectiveEnvDuration(postAwaitTimeoutReserveEnvVar, defaultPostAwaitTimeoutReserve)
}

// Require polls condition until it returns without assertion failures, or
// until ctx is canceled or timeout expires (whichever is earliest).
//
// Pass the *await.T to require.*/assert.* — failures cause a retry, not a
// test failure. Use t.Context() inside the callback to honor the timeout.
func Require(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration) {
	tb.Helper()
	run(ctx, tb, condition, legacyConfig(timeout, pollInterval, ""), "Require", requireMisuseHint, true)
}

// Requiref is like [Require] but adds a formatted message to the timeout
// failure.
func Requiref(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(ctx, tb, condition, legacyConfig(timeout, pollInterval, fmt.Sprintf(msg, args...)), "Requiref", requireMisuseHint, true)
}

func run(
	ctx context.Context,
	tb testing.TB,
	condition func(*T),
	cfg config,
	funcName string,
	misuseHint string,
	cancellable bool,
) {
	tb.Helper()

	// Skip if the test already failed — no point polling.
	if tb.Failed() {
		tb.Logf("%s: skipping (test already failed)", funcName)
		return
	}
	// Guard: context.WithDeadline panics on a nil parent.
	if ctx == nil {
		tb.Fatalf("%s: nil context", funcName)
		return
	}

	// Ensure enough context time for the await itself plus post-await reserve.
	ctx = testcontext.EnsureRemaining(tb, ctx, cfg.totalTimeout+postAwaitTimeoutReserve())

	awaitDeadline := time.Now().Add(cfg.totalTimeout)
	if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline {
		// Cap await deadline at the ctx's deadline.
		awaitDeadline = util.MinTime(awaitDeadline, ctxDeadline)
	}

	awaitCtx, awaitCancel := context.WithDeadline(ctx, awaitDeadline)
	defer awaitCancel()

	report := timeoutReport{effectiveTimeout: max(0, time.Until(awaitDeadline))}

	for {
		// Parent context was canceled while we were sleeping (not our deadline).
		if err := awaitCtx.Err(); err != nil && !deadlineReached(awaitDeadline) {
			report.reportAttemptErrors(tb)
			tb.Fatalf("%s: context canceled before condition was satisfied: %v", funcName, err)
			return
		}

		report.nextPoll()

		// Per-attempt context: bounded by the configured attempt timeout and
		// further capped by the overall awaitCtx.
		attemptCtx, attemptCancel := context.WithTimeout(awaitCtx, cfg.attemptTimeout)
		t := &T{tb: tb, ctx: attemptCtx}

		// Run attempt.
		res := runAttempt(t, condition, attemptCancel, funcName, cancellable)
		attemptCancel()
		if res.panicVal != nil {
			panic(res.panicVal) // propagate to caller
		}
		if res.deadlocked {
			report.reportAttemptErrors(tb)
			if cancellable {
				tb.Fatalf("%s: condition still running %v past context cancellation — does it honor t.Context()? (%d attempts)",
					funcName, hardDeadlockTimeout(), report.attempts)
			} else {
				tb.Fatalf("%s: condition still running %v past deadline (%d attempts)",
					funcName, hardDeadlockTimeout(), report.attempts)
			}
			return
		}
		report.recordErrors(t.errors)

		// Attempt-timeout expiry: attemptCtx is done but awaitCtx is not.
		// An attempt timeout is retryable while the await is still active. Track
		// it separately so the final report identifies the responsible timeout.
		attemptTimedOut := attemptCtx.Err() == context.DeadlineExceeded && awaitCtx.Err() == nil
		if attemptTimedOut {
			report.recordAttemptTimeout()
		}

		// Check misuse where the real test failed instead of just the attempt.
		if tb.Failed() {
			tb.Fatalf("%s: the test was marked failed directly — %s", funcName, misuseHint)
			return
		}

		// Parent context was canceled during the attempt (not our deadline).
		if err := awaitCtx.Err(); err != nil && !deadlineReached(awaitDeadline) {
			report.reportAttemptErrors(tb)
			tb.Fatalf("%s: context canceled before condition was satisfied: %v", funcName, err)
			return
		}

		// Our deadline expired.
		if deadlineReached(awaitDeadline) {
			report.reportTimeout(tb, funcName, cfg.timeoutMsg)
			return
		}

		// Success: attempt completed without failures.
		if !res.stopped && !t.Failed() && !attemptTimedOut {
			return
		}

		// Wait for pollInterval, or context is canceled or deadline is reached.
		sleep(awaitCtx, awaitDeadline, cfg.pollInterval)
	}
}

// attemptResult describes how an attempt terminated. Exactly one of the
// following fields is set:
//   - panicVal != nil: condition panicked with a non-attemptFailed value;
//     caller should re-panic with panicVal.
//   - deadlocked: condition did not honor context cancellation within
//     [hardDeadlockTimeout]; the goroutine is abandoned and leaks until the
//     process exits.
//   - stopped: condition stopped via attemptFailed (FailNow on *T) or
//     runtime.Goexit (real-test FailNow misuse).
//   - none: condition returned normally.
type attemptResult struct {
	panicVal   any
	stopped    bool
	deadlocked bool
}

// runAttempt runs condition in a goroutine so that an accidental call to the
// real test's FailNow (runtime.Goexit) terminates only this goroutine.
//
// Termination is detected in two phases:
//   - Soft (cancellable only): if condition hasn't returned within
//     [softDeadlockTimeout], log a warning and cancel ctx. Skipped if the
//     parent ctx was already cancelled.
//   - Hard: if condition still hasn't returned within [hardDeadlockTimeout],
//     declare it deadlocked and abandon the goroutine.
func runAttempt(
	t *T,
	condition func(*T),
	cancel context.CancelFunc,
	funcName string,
	cancellable bool,
) attemptResult {
	done := make(chan attemptResult, 1)

	go func() {
		completed := false
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(attemptFailed); ok {
					done <- attemptResult{stopped: true}
					return
				}
				done <- attemptResult{panicVal: r}
				return
			}
			// recover returned nil: either normal return (completed=true) or
			// runtime.Goexit (completed=false; Goexit is not a panic).
			done <- attemptResult{stopped: !completed}
		}()
		condition(t)
		completed = true
	}()

	if cancellable {
		// Soft phase: wait for the condition, our soft timer, or parent cancel.
		softTimer := time.NewTimer(softDeadlockTimeout())
		defer softTimer.Stop()

		select {
		case r := <-done:
			return r
		case <-softTimer.C:
			// Soft deadlock: log a warning.
			t.tb.Logf("%s: soft deadlock — condition still running after %v; waiting %v before declaring hard deadlock",
				funcName, softDeadlockTimeout(), hardDeadlockTimeout())

			// Cancel so the condition can observe ctx.Done().
			cancel()
		case <-t.ctx.Done():
			// Parent cancelled (await deadline reached or upstream cancel).
			// Proceed to hard phase quietly.
		}
	}

	// Hard phase: wait for the condition or the hard timer.
	hardTimer := time.NewTimer(hardDeadlockTimeout())
	defer hardTimer.Stop()

	select {
	case r := <-done:
		return r
	case <-hardTimer.C:
		return attemptResult{deadlocked: true}
	}
}

func sleep(ctx context.Context, awaitDeadline time.Time, pollInterval time.Duration) {
	remaining := time.Until(awaitDeadline)
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

func deadlineReached(awaitDeadline time.Time) bool {
	return !time.Now().Before(awaitDeadline)
}
