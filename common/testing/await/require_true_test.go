package await_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

// RequireTrue is a thin bool-predicate adapter over the same polling runner
// covered by require_ctx_test.go, so these tests focus on adapter behavior.

func TestRequireTrue_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	attempts := 0

	await.RequireTrue(t, func() bool {
		attempts++
		return true
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, 1, attempts, "condition should be called exactly once")
}

func TestRequireTrue_RetriesFalseUntilTrue(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	await.RequireTrue(t, func() bool {
		return attempts.Add(1) >= 3
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, int32(3), attempts.Load())
}

func TestRequireTrue_FailureScenarios(t *testing.T) {
	t.Parallel()

	t.Run("reports timeout", func(t *testing.T) {
		t.Parallel()

		tb := newRecordingTB()
		tb.run(func() {
			await.RequireTrue(tb, func() bool {
				return false
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
	})

	t.Run("RequireTruef includes message on timeout", func(t *testing.T) {
		t.Parallel()

		tb := newRecordingTB()
		tb.run(func() {
			await.RequireTruef(tb, func() bool {
				return false
			}, time.Second, 100*time.Millisecond, "workflow %s not ready", "wf-123")
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "workflow wf-123 not ready")
	})

	t.Run("reports real TB misuse", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name   string
			misuse func(*recordingTB)
		}{
			{"Fatal stops real TB", func(tb *recordingTB) { tb.Fatal("wrong t used") }},
			{"Errorf marks real TB failed", func(tb *recordingTB) { tb.Errorf("assert-style misuse") }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				tb := newRecordingTB()
				tb.run(func() {
					await.RequireTrue(tb, func() bool {
						tc.misuse(tb)
						return true
					}, time.Second, 100*time.Millisecond)
				})
				require.True(t, tb.Failed())
				require.Contains(t, tb.fatals(), "do not use test assertions")
			})
		}
	})

	t.Run("does not poll after prior failure", func(t *testing.T) {
		t.Parallel()

		conditionCalled := false

		tb := newRecordingTB()
		tb.run(func() {
			tb.Errorf("previous failure")
			await.RequireTrue(tb, func() bool {
				conditionCalled = true
				return true
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Empty(t, tb.fatals())
		require.False(t, conditionCalled, "condition should not run when test already failed")
	})
}

func TestRequireTrue_DeadlockDetected(t *testing.T) {
	// not using T.Parallel() so it can use t.Setenv to override the deadlock timeouts
	t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "100ms")

	const awaitTimeout = 10 * time.Second

	tb := newRecordingTB()
	start := time.Now()
	tb.run(func() {
		await.RequireTrue(tb, func() bool {
			select {} // never returns; predicate has no way to honor cancellation
		}, awaitTimeout, 50*time.Millisecond)
	})
	elapsed := time.Since(start)
	require.True(t, tb.Failed())
	require.Contains(t, tb.fatals(), "still running")
	require.Contains(t, tb.fatals(), "past deadline")
	require.Less(t, elapsed, awaitTimeout,
		"should fail at hard deadlock, not wait the full await timeout (elapsed=%v)", elapsed)
}
