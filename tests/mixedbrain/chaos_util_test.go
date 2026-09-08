package mixedbrain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeRestartTarget struct {
	restarts int
	err      error
}

func (t *fakeRestartTarget) Restart(context.Context) error {
	t.restarts++
	return t.err
}

func TestConfiguredProcessChaosInterval(t *testing.T) {
	t.Setenv(processChaosIntervalEnv, "")
	interval, err := configuredProcessChaosInterval()
	require.NoError(t, err)
	require.Equal(t, time.Minute, interval)

	for _, value := range []string{"0", "-1s", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(processChaosIntervalEnv, value)
			_, err := configuredProcessChaosInterval()
			require.Error(t, err)
		})
	}

	t.Setenv(processChaosIntervalEnv, "250ms")
	interval, err = configuredProcessChaosInterval()
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, interval)
}

func TestProcessChaosSelectsTargetAndRecordsEvent(t *testing.T) {
	first := &fakeRestartTarget{}
	second := &fakeRestartTarget{}
	events := &chaosEvents{}
	ticks := make(chan time.Time, 1)
	startedAt := time.Now().Add(-time.Second)
	ticks <- startedAt
	close(ticks)

	err := runProcessChaosTicks(t.Context(), ticks, func(int) int { return 1 }, func(context.Context) error { return nil }, events,
		processChaosTarget{name: "current", target: first},
		processChaosTarget{name: "release", target: second},
	)
	require.NoError(t, err)
	require.Zero(t, first.restarts)
	require.Equal(t, 1, second.restarts)
	require.Equal(t, []processChaosEvent{{
		Target:      "release",
		StartedAt:   startedAt,
		RestartedAt: events.snapshot()[0].RestartedAt,
		ReformedAt:  events.snapshot()[0].ReformedAt,
	}}, events.snapshot())
	require.False(t, events.snapshot()[0].RestartedAt.IsZero())
	require.False(t, events.snapshot()[0].ReformedAt.IsZero())
}

func TestProcessChaosCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, runProcessChaosTicks(ctx, make(chan time.Time), func(int) int { return 0 }, func(context.Context) error { return nil }, &chaosEvents{},
		processChaosTarget{name: "current", target: &fakeRestartTarget{}},
	))
}

func TestProcessChaosPropagatesRestartError(t *testing.T) {
	wantErr := errors.New("restart failed")
	target := &fakeRestartTarget{err: wantErr}
	events := &chaosEvents{}
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()

	err := runProcessChaosTicks(t.Context(), ticks, func(int) int { return 0 }, func(context.Context) error { return nil }, events,
		processChaosTarget{name: "current", target: target},
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, wantErr.Error(), events.snapshot()[0].Err)
}

func TestProcessChaosPropagatesClusterReformationFailure(t *testing.T) {
	wantErr := errors.New("cluster did not reform")
	events := &chaosEvents{}
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()

	err := runProcessChaosTicks(t.Context(), ticks, func(int) int { return 0 }, func(context.Context) error { return wantErr }, events,
		processChaosTarget{name: "release", target: &fakeRestartTarget{}},
	)
	require.ErrorIs(t, err, wantErr)
	event := events.snapshot()[0]
	require.False(t, event.RestartedAt.IsZero())
	require.True(t, event.ReformedAt.IsZero())
	require.Equal(t, wantErr.Error(), event.Err)
}
