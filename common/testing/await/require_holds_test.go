package await_test

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestRequireHoldsFullDuration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var starts []time.Duration
		await.RequireHolds(t.Context(), t, func(c *await.T) {
			starts = append(starts, time.Since(start))
			require.NoError(c, c.Context().Err())
		}, time.Second, 400*time.Millisecond)
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, []time.Duration{0, 400 * time.Millisecond, 800 * time.Millisecond}, starts)
	})
}

func TestRequireHoldsStopsOnFailure(t *testing.T) {
	t.Parallel()
	for _, failAt := range []int{1, 3} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				tb := newRecordingTB()
				attempts := 0
				tb.run(func() {
					await.RequireHoldsf(t.Context(), tb, func(c *await.T) {
						attempts++
						require.NotEqual(c, failAt, attempts, "invariant violated")
					}, time.Second, 100*time.Millisecond, "activity %s", "scheduled")
				})
				require.True(t, tb.Failed())
				require.Equal(t, failAt, attempts)
				require.Contains(t, tb.errors(), "invariant violated")
				require.Contains(t, tb.fatals(), "activity scheduled")
			})
		})
	}
}

func TestRequireHoldsDrainsFinalAttempt(t *testing.T) {
	t.Parallel()
	for _, lateFailure := range []bool{false, true} {
		name := "pass"
		if lateFailure {
			name = "fail"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				tb := newRecordingTB()
				start := time.Now()
				attempts := 0
				var attemptCtx context.Context
				tb.run(func() {
					await.RequireHolds(t.Context(), tb, func(c *await.T) {
						attempts++
						attemptCtx = c.Context()
						time.Sleep(2 * time.Second) //nolint:forbidigo // Advance the synctest clock beyond the observation window.
						require.NoError(c, c.Context().Err())
						if lateFailure {
							c.Fatal("late violation")
						}
					}, time.Second, time.Millisecond)
				})
				require.Equal(t, lateFailure, tb.Failed(), tb.fatals())
				require.Equal(t, 1, attempts)
				require.Equal(t, 2*time.Second, time.Since(start))
				require.ErrorIs(t, attemptCtx.Err(), context.Canceled)
			})
		})
	}
}

func TestRequireHoldsCancellation(t *testing.T) {
	t.Parallel()
	for _, duringAttempt := range []bool{false, true} {
		name := "sleep"
		if duringAttempt {
			name = "attempt"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				tb := newRecordingTB()
				attempts := 0
				tb.run(func() {
					await.RequireHolds(ctx, tb, func(c *await.T) {
						attempts++
						if duringAttempt {
							<-c.Context().Done()
						}
					}, time.Second, 200*time.Millisecond)
				})
				require.True(t, tb.Failed())
				require.Equal(t, 1, attempts)
				require.Contains(t, tb.fatals(), "context")
			})
		})
	}
}

func TestRequireHoldsAttemptTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t.Setenv("TEMPORAL_AWAIT_ATTEMPT_TIMEOUT", "50ms")
		tb := newRecordingTB()
		attempts := 0
		tb.run(func() {
			await.RequireHolds(t.Context(), tb, func(c *await.T) {
				attempts++
				<-c.Context().Done()
			}, time.Hour, time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Equal(t, 1, attempts)
		require.Contains(t, tb.fatals(), "attempt context")
	})
}

func TestRequireHoldsSoftDeadlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t.Setenv("TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT", "10ms")
		tb := newRecordingTB()
		tb.run(func() {
			await.RequireHolds(t.Context(), tb, func(c *await.T) {
				<-c.Context().Done()
			}, time.Second, time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "attempt context")
	})
}

func TestRequireHoldsHardDeadlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t.Setenv("TEMPORAL_AWAIT_ATTEMPT_TIMEOUT", "10ms")
		t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "10ms")
		release := make(chan struct{})
		exited := make(chan struct{})
		tb := newRecordingTB()
		tb.run(func() {
			await.RequireHolds(t.Context(), tb, func(*await.T) {
				defer close(exited)
				<-release
			}, time.Second, time.Millisecond)
		})
		close(release)
		<-exited
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "does it honor t.Context()?")
	})
}

func TestRequireHoldsAbnormalTermination(t *testing.T) {
	t.Parallel()
	t.Run("panic", func(t *testing.T) {
		t.Parallel()
		require.PanicsWithValue(t, "boom", func() {
			await.RequireHolds(t.Context(), t, func(*await.T) { panic("boom") }, time.Second, time.Millisecond)
		})
	})
	for _, tc := range []struct {
		name      string
		condition func(*recordingTB, *await.T)
		message   string
	}{
		{"misuse", func(tb *recordingTB, _ *await.T) { tb.Fatal("wrong target") }, "marked failed directly"},
		{"goexit", func(_ *recordingTB, _ *await.T) { runtime.Goexit() }, "condition did not hold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tb := newRecordingTB()
			tb.run(func() {
				await.RequireHolds(t.Context(), tb, func(c *await.T) { tc.condition(tb, c) }, time.Second, time.Millisecond)
			})
			require.True(t, tb.Failed())
			require.Contains(t, tb.fatals(), tc.message)
		})
	}
}

func TestRequireHoldsNonfatalFailure(t *testing.T) {
	t.Parallel()
	tb := newRecordingTB()
	attempts := 0
	tb.run(func() {
		await.RequireHolds(t.Context(), tb, func(c *await.T) {
			attempts++
			c.Error("invariant violated")
		}, time.Second, time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.Equal(t, 1, attempts)
	require.Contains(t, tb.errors(), "invariant violated")
}

type deadlineTB struct {
	*recordingTB
	deadline time.Time
}

func (t *deadlineTB) Deadline() (time.Time, bool) {
	return t.deadline, true
}

func TestRequireHoldsTestDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tb := &deadlineTB{recordingTB: newRecordingTB(), deadline: time.Now().Add(100 * time.Millisecond)}
		attempts := 0
		tb.run(func() {
			await.RequireHolds(t.Context(), tb, func(*await.T) {
				attempts++
			}, time.Second, 200*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Equal(t, 1, attempts)
		require.Contains(t, tb.fatals(), "context")
	})
}

func TestRequireHoldsCanceledDuringDrain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			time.Sleep(1500 * time.Millisecond) //nolint:forbidigo // Cancel after the observation window while its last attempt is running.
			cancel()
		}()
		tb := newRecordingTB()
		start := time.Now()
		tb.run(func() {
			await.RequireHolds(ctx, tb, func(c *await.T) {
				<-c.Context().Done()
			}, time.Second, time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Equal(t, 1500*time.Millisecond, time.Since(start))
		require.Contains(t, tb.fatals(), "context canceled")
	})
}

func TestRequireHoldsInvalidInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		duration time.Duration
		interval time.Duration
		message  string
	}{
		{"nil context", nil, time.Second, time.Millisecond, "nil context"},
		{"zero duration", t.Context(), 0, time.Millisecond, "duration must be positive"},
		{"negative duration", t.Context(), -time.Second, time.Millisecond, "duration must be positive"},
		{"negative interval", t.Context(), time.Second, -time.Millisecond, "poll interval must be non-negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tb := newRecordingTB()
			attempts := 0
			tb.run(func() {
				await.RequireHolds(tc.ctx, tb, func(*await.T) { attempts++ }, tc.duration, tc.interval)
			})
			require.True(t, tb.Failed())
			require.Zero(t, attempts)
			require.Contains(t, tb.fatals(), tc.message)
		})
	}
}
