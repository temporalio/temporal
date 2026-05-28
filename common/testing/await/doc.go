// Package await provides polling-based test assertions as a replacement
// for testify's Eventually, EventuallyWithT, and their formatted variants.
//
// The legacy API is [Require], [Requiref], [RequireTrue], and [RequireTruef],
// which accept positional timeout and poll interval arguments. The option-based
// API is [Require2] and [RequireTrue2]; use [WithTimeout],
// [WithMinPollInterval], [WithMaxPollInterval], [WithAttemptTimeout], and
// [WithMessagef] to configure it.
//
// By default, the option-based API waits up to 30s total, caps each attempt at
// 10s, starts polling at 500ms, and exponentially backs off to 2s. Set
// TEMPORAL_TEST_TIMEOUT, TEMPORAL_AWAIT_ATTEMPT_TIMEOUT,
// TEMPORAL_AWAIT_MIN_POLL_INTERVAL, or TEMPORAL_AWAIT_MAX_POLL_INTERVAL to
// override the package defaults for a test environment, or pass per-call
// options.
//
// Await also asks testcontext to extend the test-scoped context deadline by the
// await timeout, subject to testcontext's cap. Existing contexts obtained before
// the extension keep their original deadline; callers that need the extended
// deadline should obtain a fresh context from testcontext.
//
// Improvements over testify's eventually functions:
//
//   - Misuse detection: accidentally using the real *testing.T (e.g. s.T() or
//     suite assertion methods) instead of the callback's collect T is a
//     common mistake. This package detects it and fails with a clear message.
//
//   - Safer bool predicates: unlike testify's Eventually, [RequireTrue] only
//     accepts func() bool, so returning false is the sole retry signal. If the
//     predicate accidentally marks the real test failed, it reports that
//     immediately instead of polling until timeout.
//
//   - Timeout-aware callbacks: callbacks receive a context derived from the
//     parent context and canceled when the await timeout or test deadline is
//     reached, so RPCs and blocking waits can exit instead of continuing after
//     the retry window has expired.
//
//   - Panic propagation: if the condition panics (e.g. nil dereference), the
//     panic is propagated immediately rather than being silently swallowed
//     or retried until timeout.
//     See https://github.com/stretchr/testify/issues/1810
//
//   - Bounded goroutine lifetime: each attempt completes before the next
//     starts, avoiding the overlapping-attempt data races and "panic: Fail
//     in goroutine after Test has completed" crashes seen with testify's
//     Eventually.
//     See https://github.com/stretchr/testify/issues/1611
//
//   - Deadlock detection: a condition that ignores t.Context() is abandoned
//     after a grace period, producing a clear "does it honor t.Context()?"
//     failure instead of hanging until go test -timeout.
//
//   - Condition always runs: testify's Eventually can fail without ever
//     running the condition due to a timer/ticker race with short timeouts.
//     This package runs the condition immediately on the first iteration.
//     See https://github.com/stretchr/testify/issues/1652
package await
