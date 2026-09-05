package await

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
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
	return envDuration(softDeadlockTimeoutEnvVar, defaultSoftDeadlockTimeout)
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
	return envDuration(hardDeadlockTimeoutEnvVar, defaultHardDeadlockTimeout)
}

// postAwaitTimeoutReserve is the minimum time to keep for *after* Await returns.
const postAwaitTimeoutReserve = 10 * time.Second

// Require polls condition until it returns without assertion failures, or
// until ctx is canceled or the test context timeout expires (whichever is
// earliest).
//
// Pass the *await.T to require.*/assert.* — failures cause a retry, not a
// test failure. Use t.Context() inside the callback to honor the timeout.
// The timeout argument is retained for source compatibility and ignored. The
// poll interval is the base for exponential backoff capped at 2s.
func Require(ctx context.Context, tb testing.TB, condition func(*T), _, pollInterval time.Duration) {
	tb.Helper()
	run(ctx, tb, condition, legacyConfig(pollInterval, ""), "Require", requireMisuseHint, true)
}

// Requiref is like [Require] but adds a formatted message to the timeout
// failure. Its timeout argument is also ignored, and its poll interval is used
// as the base for exponential backoff.
func Requiref(ctx context.Context, tb testing.TB, condition func(*T), _, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(ctx, tb, condition, legacyConfig(pollInterval, fmt.Sprintf(msg, args...)), "Requiref", requireMisuseHint, true)
}

func run(
	parentCtx context.Context,
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
	// Guard: context.WithDeadline and testcontext.WithDeadline panic on a nil parent.
	if parentCtx == nil {
		tb.Fatalf("%s: nil context", funcName)
		return
	}

	start := time.Now()
	// Ensure enough context time for the await itself plus post-await reserve.
	// This only works for [testcontext]s; other contexts will be left unchanged.
	testcontext.EnsureRemaining(parentCtx, tb, cfg.totalTimeout+postAwaitTimeoutReserve)

	deadline := start.Add(cfg.totalTimeout)
	deadlineCause := ""
	if parentDeadline, hasDeadline := parentCtx.Deadline(); hasDeadline && parentDeadline.Before(deadline) {
		// Cap at the parent context's deadline if it's earlier than our timeout.
		deadline = parentDeadline
		deadlineCause = "parent context deadline"
	}

	// Cap at the test's deadline if it's earlier than our deadline.
	// Ideally, the parent context already accounts for the test's deadline - but we are being defensive.
	if testDeadline, hasDeadline := testcontext.GoTestDeadline(tb); hasDeadline && testDeadline.Before(deadline) {
		deadline = testDeadline
		deadlineCause = "go test timeout"
	}

	effectiveTimeout := max(0, time.Until(deadline))
	awaitCtx, awaitCancel := testcontext.WithDeadline(parentCtx, deadline)
	defer awaitCancel()

	report := timeoutReport{
		effectiveTimeout:  effectiveTimeout,
		configuredTimeout: cfg.totalTimeout,
		attemptTimeout:    cfg.attemptTimeout,
		testContext:       parentCtx,
		deadlineCause:     deadlineCause,
	}

	for {
		// Parent context may have been canceled while we were sleeping, or sleep
		// may have returned at the deadline. Do not start another attempt then.
		if failIfAwaitDone(awaitCtx, deadline, tb, report, funcName, cfg.timeoutMsg) {
			return
		}

		report.nextPoll()

		// Per-attempt context: bounded by the configured attempt timeout and
		// further capped by the overall awaitCtx.
		attemptCtx, attemptCancel := context.WithTimeout(awaitCtx, cfg.attemptTimeout)
		t := &T{tb: tb, ctx: attemptCtx}

		// Run attempt.
		attemptStart := time.Now()
		res := runAttempt(t, condition, attemptCancel, funcName, cancellable)
		report.recordAttemptDuration(time.Since(attemptStart))
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
		attemptHitOwnTimeout := attemptCtx.Err() == context.DeadlineExceeded && awaitCtx.Err() == nil
		if attemptHitOwnTimeout {
			report.recordAttemptTimeout()
		}

		// Check misuse where the real test failed instead of just the attempt.
		if tb.Failed() {
			tb.Fatalf("%s: the test was marked failed directly — %s", funcName, misuseHint)
			return
		}

		// Parent context may have been canceled during the attempt, or our
		// deadline may have expired.
		if failIfAwaitDone(awaitCtx, deadline, tb, report, funcName, cfg.timeoutMsg) {
			return
		}

		// Success: attempt completed without failures.
		if !res.stopped && !t.Failed() && !attemptHitOwnTimeout {
			return
		}

		// Wait for the next poll interval, or context is canceled or deadline is reached.
		pollInterval := min(
			nextPollInterval(cfg.pollInterval, report.attempts),
			max(time.Nanosecond, time.Until(deadline)/2),
		)
		sleep(awaitCtx, deadline, pollInterval)
	}
}

func failIfAwaitDone(
	ctx context.Context,
	deadline time.Time,
	tb testing.TB,
	report timeoutReport,
	funcName string,
	timeoutMsg string,
) bool {
	tb.Helper()
	err := ctx.Err()
	if err == nil {
		if !deadlineReached(deadline) {
			return false
		}
		// The wall clock can reach deadline just before the context timer
		// publishes its terminal error and cause. Wait for that state so the
		// reporter can classify the deadline by its actual owner.
		<-ctx.Done()
		err = ctx.Err()
	}

	if testcontext.FailIfDeadlineExceeded(
		ctx,
		tb,
		report.renderSupplementalDetails(funcName, timeoutMsg),
	) {
		return true
	}
	if err == context.DeadlineExceeded {
		report.reportTimeout(tb, funcName, timeoutMsg)
		return true
	}
	failContextCanceled(tb, report, funcName, err)
	return true
}

func failContextCanceled(tb testing.TB, report timeoutReport, funcName string, err error) {
	tb.Helper()
	report.reportAttemptErrors(tb)
	tb.Fatalf("%s: context canceled before condition was satisfied: %v", funcName, err)
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
