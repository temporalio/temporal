package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/scheduler/model"
)

func TestTransitionOverlapPolicies(t *testing.T) {
	tests := []struct {
		name       string
		policy     enumspb.ScheduleOverlapPolicy
		effectType any
		skipped    int64
		deferred   bool
	}{
		{name: "allow all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, effectType: model.StartWorkflow{}},
		{name: "skip", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, skipped: 1},
		{name: "buffer one", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, deferred: true},
		{name: "buffer all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, deferred: true},
		{name: "cancel other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, effectType: model.CancelWorkflow{}, deferred: true},
		{name: "terminate other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, effectType: model.TerminateWorkflow{}, deferred: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			config.OverlapPolicy = test.policy
			state := transition(t, config, model.State{}, model.Create{}).State
			state = transition(t, config, state, model.Trigger{ID: "first"}).State
			state = transition(t, config, state, model.StartSucceeded{ActionID: "first", RunID: "run-1"}).State

			second := transition(t, config, state, model.Trigger{ID: "second"})
			require.Equal(t, test.skipped, second.State.OverlapSkipped)
			if test.effectType == nil {
				require.Empty(t, second.Effects)
			} else {
				require.IsType(t, test.effectType, second.Effects[0])
			}
			if test.policy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
				require.Empty(t, second.State.Pending)
				return
			}
			if test.deferred {
				require.False(t, second.State.Pending[0].Dispatched)
				released := transition(t, config, second.State, model.CompleteWorkflow{ActionID: "first"})
				require.IsType(t, model.StartWorkflow{}, released.Effects[0])
				require.True(t, released.State.Pending[0].Dispatched)
			}
		})
	}
}

func TestTransitionStartFailureClasses(t *testing.T) {
	config := testConfig()
	config.Interval = time.Hour
	state := transition(t, config, model.State{}, model.Create{}).State
	state = transition(t, config, state, model.Trigger{ID: "retry"}).State
	retryAt := state.Now.Add(time.Second)

	failed := transition(t, config, state, model.StartFailed{
		ActionID: "retry", Class: model.StartFailureRetryable, RetryAt: retryAt,
	})
	require.Equal(t, []model.Effect{model.ScheduleWakeup{At: retryAt}}, failed.Effects)
	require.Equal(t, retryAt, failed.State.Pending[0].RetryAt)

	retried := transition(t, config, failed.State, model.AdvanceTime{Time: retryAt})
	require.IsType(t, model.StartWorkflow{}, retried.Effects[0])
	require.Equal(t, 2, retried.State.Pending[0].Attempt)
	require.True(t, retried.State.Pending[0].RetryAt.IsZero())

	dropped := transition(t, config, retried.State, model.StartFailed{
		ActionID: "retry", Class: model.StartFailureNonRetryable,
	})
	require.Empty(t, dropped.State.Pending)
	require.Empty(t, dropped.Effects)
}

func TestTransitionLimitedActionsDoNotLimitManualTrigger(t *testing.T) {
	config := testConfig()
	config.LimitedActions = true
	config.RemainingActions = 2
	state := transition(t, config, model.State{}, model.Create{}).State

	advanced := transition(t, config, state, model.AdvanceTime{Time: state.Now.Add(3 * time.Minute)})
	require.Zero(t, advanced.State.RemainingActions)
	require.Len(t, advanced.State.Pending, 2)

	manual := transition(t, config, advanced.State, model.Trigger{ID: "manual"})
	require.Len(t, manual.State.Pending, 3)
	require.IsType(t, model.StartWorkflow{}, manual.Effects[0])
}

func TestTransitionBackfillIncludesBoundsAfterDeferral(t *testing.T) {
	config := testConfig()
	state := transition(t, config, model.State{}, model.Create{}).State
	start := testStart.Add(-3 * time.Minute)
	outcome := transition(t, config, state, model.Backfill{ID: "history", Start: start, End: testStart})
	require.Len(t, outcome.Effects, 4)
	require.Equal(t, start, outcome.State.Pending[0].NominalTime)
}

func TestTransitionUpdate(t *testing.T) {
	config := testConfig()
	state := transition(t, config, model.State{}, model.Create{}).State
	outcome := transition(t, config, state, model.Update{
		Paused: true, Notes: "updated", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
	})
	require.True(t, outcome.State.Paused)
	require.Equal(t, "updated", outcome.State.Notes)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, outcome.State.OverlapPolicy)
	require.Equal(t, state.ConflictToken+1, outcome.State.ConflictToken)
}
