package await_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestRequireHoldsTrue(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		failAt       int
		wantAttempts int
		wantDuration time.Duration
	}{
		{"holds", 0, 3, time.Second},
		{"immediate false", 1, 1, 0},
		{"later false", 3, 3, 800 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				tb := newRecordingTB()
				start := time.Now()
				attempts := 0
				tb.run(func() {
					await.RequireHoldsTrue(tb, func() bool {
						attempts++
						return attempts != tc.failAt
					}, time.Second, 400*time.Millisecond)
				})
				require.Equal(t, tc.failAt != 0, tb.Failed(), tb.fatals())
				require.Equal(t, tc.wantAttempts, attempts)
				require.Equal(t, tc.wantDuration, time.Since(start))
			})
		})
	}
}

func TestRequireHoldsTruefMisuse(t *testing.T) {
	t.Parallel()
	tb := newRecordingTB()
	tb.run(func() {
		await.RequireHoldsTruef(tb, func() bool {
			tb.Fatal("wrong target")
			return true
		}, time.Second, time.Millisecond, "counter %s", "unchanged")
	})
	require.True(t, tb.Failed())
	require.Contains(t, tb.fatals(), "counter unchanged")
	require.Contains(t, tb.fatals(), "return false to fail")
}
