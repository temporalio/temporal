package await

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

// RequireHolds polls condition immediately and then after each pollInterval,
// failing on the first assertion failure. It succeeds only after the full
// duration has elapsed and every started attempt has passed. It cannot detect
// violations between polls.
//
// Pass the *await.T to assertions and use t.Context() for blocking operations.
// The duration stops new polls; it does not cancel an in-flight attempt. That
// attempt is allowed to finish under its own timeout, and its result is honored
// even after the duration has elapsed. Returning can therefore take up to an
// additional attempt timeout plus the hard-deadlock grace period.
// Parent cancellation, attempt timeout, and deadlock fail the test.
func RequireHolds(ctx context.Context, tb testing.TB, condition func(*T), duration, pollInterval time.Duration) {
	tb.Helper()
	runHolds(ctx, tb, condition, legacyConfig(duration, pollInterval, ""), "RequireHolds", requireMisuseHint, true)
}

// RequireHoldsf is like [RequireHolds] but adds a formatted failure message.
func RequireHoldsf(ctx context.Context, tb testing.TB, condition func(*T), duration, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	runHolds(ctx, tb, condition, legacyConfig(duration, pollInterval, fmt.Sprintf(msg, args...)), "RequireHoldsf", requireMisuseHint, true)
}

func runHolds(parentCtx context.Context, tb testing.TB, condition func(*T), cfg config, funcName, misuseHint string, cancellable bool) {
	tb.Helper()
	if tb.Failed() {
		tb.Logf("%s: skipping (test already failed)", funcName)
		return
	}
	if parentCtx == nil {
		tb.Fatalf("%s: nil context", funcName)
		return
	}
	if cfg.totalTimeout <= 0 || cfg.pollInterval < 0 {
		tb.Fatalf("%s: duration must be positive and poll interval must be non-negative", funcName)
		return
	}

	parentCtx = testcontext.EnsureRemaining(parentCtx, tb, cfg.totalTimeout+cfg.attemptTimeout+postAwaitTimeoutReserve)
	if deadline, ok := testcontext.GoTestDeadline(tb); ok {
		var cancel context.CancelFunc
		parentCtx, cancel = context.WithDeadline(parentCtx, deadline)
		defer cancel()
	}

	start := time.Now()
	observationEnd := start.Add(cfg.totalTimeout)
	report := timeoutReport{}
	fail := func(format string, args ...any) {
		tb.Helper()
		report.reportAttemptErrors(tb)
		message := fmt.Sprintf(format, args...)
		if cfg.timeoutMsg != "" {
			message = cfg.timeoutMsg + ": " + message
		}
		tb.Fatalf("%s: %s after %v (%d attempts; required duration %v)",
			funcName, message, time.Since(start), report.attempts, cfg.totalTimeout)
	}

	for {
		if err := parentCtx.Err(); err != nil {
			fail("context ended before observation completed: %v", err)
			return
		}
		if report.attempts > 0 && deadlineReached(observationEnd) {
			return
		}

		report.nextPoll()
		attemptCtx, attemptCancel := context.WithTimeout(parentCtx, cfg.attemptTimeout)
		t := &T{tb: tb, ctx: attemptCtx}
		res := runAttempt(t, condition, attemptCancel, funcName, cancellable)
		attemptErr := attemptCtx.Err()
		attemptCancel()
		if res.panicVal != nil {
			panic(res.panicVal)
		}
		if res.deadlocked {
			if cancellable {
				fail("condition still running %v past context cancellation — does it honor t.Context()?", hardDeadlockTimeout())
			} else {
				fail("condition still running after %v", hardDeadlockTimeout())
			}
			return
		}
		report.recordErrors(t.errors)
		if tb.Failed() {
			fail("the test was marked failed directly — %s", misuseHint)
			return
		}
		if err := parentCtx.Err(); err != nil {
			fail("context ended before observation completed: %v", err)
			return
		}
		if attemptErr != nil {
			fail("attempt context ended before condition completed: %v", attemptErr)
			return
		}
		if res.stopped || t.Failed() {
			fail("condition did not hold")
			return
		}

		sleep(parentCtx, observationEnd, cfg.pollInterval)
	}
}
