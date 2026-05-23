package await

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
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
	if s := os.Getenv(softDeadlockTimeoutEnvVar); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return defaultSoftDeadlockTimeout
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
	if s := os.Getenv(hardDeadlockTimeoutEnvVar); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return defaultHardDeadlockTimeout
}

// Require polls condition until it returns without assertion failures, or
// until ctx is canceled or timeout expires (whichever is earliest).
//
// Pass the *await.T to require.*/assert.* — failures cause a retry, not a
// test failure. Use t.Context() inside the callback to honor the timeout.
func Require(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration) {
	tb.Helper()
	run(ctx, tb, condition, timeout, pollInterval, "", "Require", requireMisuseHint, true)
}

// Requiref is like [Require] but adds a formatted message to the timeout
// failure.
func Requiref(ctx context.Context, tb testing.TB, condition func(*T), timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(ctx, tb, condition, timeout, pollInterval, fmt.Sprintf(msg, args...), "Requiref", requireMisuseHint, true)
}

func run(
	parentCtx context.Context,
	tb testing.TB,
	condition func(*T),
	timeout,
	pollInterval time.Duration,
	timeoutMsg string,
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
	if parentCtx == nil {
		tb.Fatalf("%s: nil context", funcName)
		return
	}

	deadline := time.Now().Add(timeout)

	// Cap at the parent context's deadline if it's earlier than our timeout.
	if parentDeadline, hasDeadline := parentCtx.Deadline(); hasDeadline && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}

	// Cap at the test's deadline if it's earlier than our deadline.
	// Ideally, the parent context already accounts for the test's deadline - but we are being defensive.
	if d, ok := tb.(interface{ Deadline() (time.Time, bool) }); ok {
		if testDeadline, hasDeadline := d.Deadline(); hasDeadline && testDeadline.Before(deadline) {
			deadline = testDeadline
		}
	}

	effectiveTimeout := max(0, time.Until(deadline))
	awaitCtx, awaitCancel := context.WithDeadline(parentCtx, deadline)
	defer awaitCancel()

	var failures []attemptFailure
	polls := 0

	for {
		// Parent context was canceled while we were sleeping (not our deadline).
		if err := awaitCtx.Err(); err != nil && !deadlineReached(deadline) {
			reportAttemptErrors(tb, failures)
			tb.Fatalf("%s: context canceled before condition was satisfied: %v", funcName, err)
			return
		}

		polls++

		// Fresh context per attempt, scoped to the run-level ctx. runAttempt
		// owns the soft timeout and the corresponding cancel.
		attemptCtx, attemptCancel := context.WithCancel(awaitCtx)
		t := &T{tb: tb, ctx: attemptCtx}

		// Run attempt.
		res := runAttempt(t, condition, attemptCancel, funcName, cancellable)
		attemptCancel()
		if res.panicVal != nil {
			panic(res.panicVal) // propagate to caller
		}
		if res.deadlocked {
			reportAttemptErrors(tb, failures)
			if cancellable {
				tb.Fatalf("%s: condition still running %v past context cancellation — does it honor t.Context()? (%d polls)",
					funcName, hardDeadlockTimeout(), polls)
			} else {
				tb.Fatalf("%s: condition still running %v past deadline (%d polls)",
					funcName, hardDeadlockTimeout(), polls)
			}
			return
		}
		if len(t.errors) > 0 {
			failures = append(failures, attemptFailure{attempt: polls, errors: t.errors})
		}

		// Check misuse where the real test failed instead of just the attempt.
		if tb.Failed() {
			tb.Fatalf("%s: the test was marked failed directly — %s", funcName, misuseHint)
			return
		}

		// Parent context was canceled during the attempt (not our deadline).
		if err := awaitCtx.Err(); err != nil && !deadlineReached(deadline) {
			reportAttemptErrors(tb, failures)
			tb.Fatalf("%s: context canceled before condition was satisfied: %v", funcName, err)
			return
		}

		// Our deadline expired.
		if deadlineReached(deadline) {
			reportTimeout(tb, failures, funcName, timeoutMsg, effectiveTimeout, polls)
			return
		}

		// Success: attempt completed without failures.
		if !res.stopped && !t.Failed() {
			return
		}

		// Wait for pollInterval, or context is canceled or deadline is reached.
		sleep(awaitCtx, deadline, pollInterval)
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
