// Package await provides polling-based test assertions as a replacement
// for testify's Eventually, EventuallyWithT, and their formatted variants.
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
