package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"pgregory.net/rapid"
)

func TestSchedulerCancelTerminateRetryProfileProperty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		policy enumspb.ScheduleOverlapPolicy
		queue  func(schedulerRPCProfiles, *schedulerPropertyEnv)
		calls  func(*schedulerPropertyEnv) int
	}{
		{
			name:   "cancel",
			policy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
			queue: func(profiles schedulerRPCProfiles, env *schedulerPropertyEnv) {
				profiles.cancelRetryable().Expect(&env.services.Cancel, cancelWorkflowMethod, "retry", func(*historyservice.RequestCancelWorkflowExecutionRequest) bool { return true })
			},
			calls: func(env *schedulerPropertyEnv) int { return env.services.CancelCallCount() },
		},
		{
			name:   "terminate",
			policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
			queue: func(profiles schedulerRPCProfiles, env *schedulerPropertyEnv) {
				profiles.terminateRetryable().Expect(&env.services.Terminate, terminateWorkflowMethod, "retry", func(*historyservice.TerminateWorkflowExecutionRequest) bool { return true })
			},
			calls: func(env *schedulerPropertyEnv) int { return env.services.TerminateCallCount() },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				env := newSchedulerPropertyEnvWithPolicy(t, false, test.policy)
				env.drain(t, schedulerConformanceDrainLimit)
				env.trigger(t)
				env.drain(t, schedulerConformanceDrainLimit)

				test.queue(schedulerRPCProfiles{}, env)
				env.trigger(t)
				env.drain(t, schedulerConformanceDrainLimit)
				require.Equal(t, 2, test.calls(env))
			})
		})
	}
}
