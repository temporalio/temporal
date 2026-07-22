package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestSchedulerGeneratorDeadlineBoundaryProperty(t *testing.T) {
	tests := []struct {
		name   string
		delta  time.Duration
		starts int
	}{
		{name: "before", delta: -time.Nanosecond, starts: 0},
		{name: "exact", starts: 1},
		{name: "after", delta: time.Nanosecond, starts: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSchedulerPropertyEnv(t, false)
			env.drain(t, schedulerConformanceDrainLimit)
			env.timeSource.Update(schedulerPropertyStartTime.Add(defaultInterval).Add(test.delta))
			env.drain(t, schedulerConformanceDrainLimit)
			require.Len(t, env.services.StartCalls(), test.starts)
		})
	}
}

func TestSchedulerBoundedBackfillProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	env.backfill(t, schedulerPropertyStartTime.Add(-3*defaultInterval), schedulerPropertyStartTime, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	env.drain(t, schedulerConformanceDrainLimit)
	require.Len(t, env.services.StartCalls(), 4)
	require.Equal(t, int64(4), env.describe(t).GetInfo().GetActionCount())
}
