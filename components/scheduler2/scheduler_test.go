package scheduler2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apischedule "go.temporal.io/api/schedule/v1"
	srvschedule "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/components/scheduler2"
	v1scheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestUseScheduledActionPeek(t *testing.T) {
	const initialRemainingActions int64 = 10
	const initialConflictToken int64 = 0
	decrement := false

	// Starts paused, with unlimited actions.
	scheduler := scheduler2.Scheduler{
		SchedulerInternal: &srvschedule.SchedulerInternal{
			ConflictToken: initialConflictToken,
			Schedule: &apischedule.Schedule{
				State: &apischedule.ScheduleState{
					Paused:           true,
					LimitedActions:   false,
					RemainingActions: initialRemainingActions,
				},
			},
		},
	}

	// Paused schedules should deny and never mutate.
	require.False(t, scheduler.UseScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// Running schedules with unlimited actions should allow and never mutate.
	scheduler.Schedule.State.Paused = false
	require.True(t, scheduler.UseScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// Limit the schedule's actions, and check that we can peek the value.
	scheduler.Schedule.State.LimitedActions = true
	decrement = false
	require.True(t, scheduler.UseScheduledAction(decrement))
	require.Equal(t, initialRemainingActions, scheduler.Schedule.State.RemainingActions)
	require.Equal(t, initialConflictToken, scheduler.ConflictToken)

	// When not peeking, we should mutate RemainingActions and ConflictToken.
	decrement = true
	require.True(t, scheduler.UseScheduledAction(decrement))
	require.Equal(t, initialRemainingActions-1, scheduler.Schedule.State.RemainingActions)
	require.NotEqual(t, initialConflictToken, scheduler.ConflictToken)

	// False when out of remaining actions.
	scheduler.Schedule.State.RemainingActions = 0
	oldConflictToken := scheduler.ConflictToken
	require.False(t, scheduler.UseScheduledAction(decrement))
	require.Equal(t, oldConflictToken, scheduler.ConflictToken)
}

func TestCompiledSpec(t *testing.T) {
	const initialConflictToken int64 = 0
	initialInterval := apischedule.IntervalSpec{
		Interval: durationpb.New(time.Minute),
		Phase:    durationpb.New(0),
	}

	scheduler := scheduler2.Scheduler{
		SchedulerInternal: &srvschedule.SchedulerInternal{
			ConflictToken: initialConflictToken,
			Schedule: &apischedule.Schedule{
				Spec: &apischedule.ScheduleSpec{
					Interval: []*apischedule.IntervalSpec{&initialInterval},
				},
			},
		},
	}
	specBuilder := v1scheduler.NewSpecBuilder()

	// Test that the same compiled spec is returned between invocations.
	spec, err := scheduler.CompiledSpec(specBuilder)
	require.NoError(t, err)

	spec2, err := scheduler.CompiledSpec(specBuilder)
	require.NoError(t, err)
	require.Equal(t, spec, spec2)

	// Test that an update to conflict token will recompile the spec.
	scheduler.UpdateConflictToken()
	spec3, err := scheduler.CompiledSpec(specBuilder)
	require.NoError(t, err)
	require.NotSame(t, spec, spec3)
}
