package activity

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

func TestValidTransitions(t *testing.T) {
	validTransitions := []struct {
		from       activitypb.ActivityExecutionStatus
		transition chasm.Transition[activitypb.ActivityExecutionStatus, *Activity, any]
	}{
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
			transition: TransitionScheduled,
		},
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			transition: TransitionStarted,
		},
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			transition: TransitionCompleted,
		},
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			transition: TransitionFailed,
		},
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			transition: TransitionTerminated,
		},
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			transition: TransitionTimedOut,
		},
	}

	for _, transition := range validTransitions {
		t.Run(fmt.Sprintf("%s->%s", transition.from, transition.transition.Destination), func(t *testing.T) {
			activity := &Activity{ActivityState: &activitypb.ActivityState{Status: transition.from}}
			require.True(t, transition.transition.Possible(activity))
		})
	}
}

func TestInvalidTransitions(t *testing.T) {
	invalidTransitions := []struct {
		from       activitypb.ActivityExecutionStatus
		transition chasm.Transition[activitypb.ActivityExecutionStatus, *Activity, any]
	}{
		{
			from:       activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			transition: TransitionCompleted,
		},
	}

	for _, transition := range invalidTransitions {
		t.Run(fmt.Sprintf("%s->%s", transition.from, transition.transition.Destination), func(t *testing.T) {
			activity := &Activity{ActivityState: &activitypb.ActivityState{Status: transition.from}}
			require.False(t, transition.transition.Possible(activity))
		})
	}
}
