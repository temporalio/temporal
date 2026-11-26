package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestSchedulerCanTakeScheduledActionDoesNotMutateState(t *testing.T) {
	tests := []struct {
		name             string
		paused           bool
		limitedActions   bool
		remainingActions int64
		want             bool
	}{
		{
			name:   "paused",
			paused: true,
		},
		{
			name:             "unlimited",
			remainingActions: 0,
			want:             true,
		},
		{
			name:           "limited with no actions",
			limitedActions: true,
		},
		{
			name:             "limited with remaining actions",
			limitedActions:   true,
			remainingActions: 2,
			want:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := newActionCapacityTestScheduler(tt.paused, tt.limitedActions, tt.remainingActions)
			initialToken := scheduler.ConflictToken

			require.Equal(t, tt.want, scheduler.canTakeScheduledAction())
			require.Equal(t, tt.remainingActions, scheduler.Schedule.State.RemainingActions)
			require.Equal(t, initialToken, scheduler.ConflictToken)
		})
	}
}

func TestSchedulerConsumeScheduledActionCounterAndTokenDeltas(t *testing.T) {
	tests := []struct {
		name         string
		consumptions int
	}{
		{name: "zero"},
		{name: "one", consumptions: 1},
		{name: "multiple", consumptions: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialActions := int64(tt.consumptions)
			scheduler := newActionCapacityTestScheduler(false, true, initialActions)
			initialToken := scheduler.ConflictToken

			for range tt.consumptions {
				require.True(t, scheduler.consumeScheduledAction())
			}
			require.False(t, scheduler.consumeScheduledAction())

			require.Zero(t, scheduler.Schedule.State.RemainingActions)
			require.Equal(t, initialToken+int64(tt.consumptions), scheduler.ConflictToken)
		})
	}
}

func TestSchedulerConsumeScheduledActionUnlimitedDoesNotMutateState(t *testing.T) {
	scheduler := newActionCapacityTestScheduler(false, false, 0)
	initialToken := scheduler.ConflictToken

	require.True(t, scheduler.consumeScheduledAction())
	require.Zero(t, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialToken, scheduler.ConflictToken)
}

func newActionCapacityTestScheduler(paused, limitedActions bool, remainingActions int64) *Scheduler {
	return &Scheduler{
		SchedulerState: &schedulerpb.SchedulerState{
			Schedule: &schedulepb.Schedule{
				State: &schedulepb.ScheduleState{
					Paused:           paused,
					LimitedActions:   limitedActions,
					RemainingActions: remainingActions,
				},
			},
			ConflictToken: 41,
		},
	}
}
