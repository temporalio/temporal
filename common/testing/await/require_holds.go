package await

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

var errObservationEnded = errors.New("observation duration elapsed")

// RequireHolds polls condition immediately and then after each pollInterval.
// It succeeds after the full duration if at least one poll completed successfully
// and every poll completed before the duration ended passed. It cannot detect
// violations between polls.
//
// Pass the *await.T to assertions and use t.Context() for blocking operations.
// At the end of the duration, the running attempt is canceled and drained;
// its assertion failures are ignored. It must exit within the hard-deadlock
// grace period. Parent cancellation, attempt timeout, and deadlock fail the test.
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

	parentCtx = testcontext.EnsureRemaining(parentCtx, tb, cfg.totalTimeout+postAwaitTimeoutReserve)
	if deadline, ok := testcontext.GoTestDeadline(tb); ok {
		var cancel context.CancelFunc
		parentCtx, cancel = context.WithDeadline(parentCtx, deadline)
		defer cancel()
	}

	start := time.Now()
	observationEnd := start.Add(cfg.totalTimeout)
	observationCtx, cancel := context.WithDeadlineCause(parentCtx, observationEnd, errObservationEnded)
	defer cancel()
	passed := false
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
		if deadlineReached(observationEnd) {
			if !passed {
				fail("no poll completed successfully before observation ended")
			}
			return
		}

		report.nextPoll()
		attemptCtx, attemptCancel := context.WithTimeout(observationCtx, cfg.attemptTimeout)
		t := &T{tb: tb, ctx: attemptCtx}
		var attemptCause error
		var completedAt time.Time
		res := runAttempt(t, func(t *T) {
			defer func() {
				attemptCause = context.Cause(attemptCtx)
				completedAt = time.Now()
			}()
			condition(t)
		}, attemptCancel, funcName, cancellable)
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
		interrupted := attemptCause == errObservationEnded || (attemptCause == nil && !completedAt.Before(observationEnd))
		if !interrupted {
			report.recordErrors(t.errors)
		}
		if tb.Failed() {
			fail("the test was marked failed directly — %s", misuseHint)
			return
		}
		if err := parentCtx.Err(); err != nil {
			fail("context ended before observation completed: %v", err)
			return
		}
		if interrupted {
			continue
		}
		if attemptCause != nil {
			fail("attempt context ended before condition completed: %v", attemptCause)
			return
		}
		if res.stopped || t.Failed() {
			fail("condition did not hold")
			return
		}

		passed = true
		sleep(observationCtx, observationEnd, cfg.pollInterval)
	}
}
