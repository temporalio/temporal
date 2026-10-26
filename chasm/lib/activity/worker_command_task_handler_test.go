package activity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
)

func TestWorkerCommandDispatchTaskHandler_Validate(t *testing.T) {
	maxAttempts := 3
	handler := &workerCommandDispatchTaskHandler{
		config: &configs.Config{
			WorkerCommandsMaxAttempts: dynamicconfig.GetIntPropertyFn(maxAttempts),
		},
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewNoopLogger(),
	}

	cancelTask := &activitypb.WorkerCommandDispatchTask{
		Command: &activitypb.WorkerCommandDispatchTask_Cancel{},
	}

	testCases := []struct {
		name     string
		status   activitypb.ActivityExecutionStatus
		task     *activitypb.WorkerCommandDispatchTask
		attempt  int
		expected bool
	}{
		{
			name:     "cancel: cancel requested",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			task:     cancelTask,
			attempt:  1,
			expected: true,
		},
		{
			name:     "cancel: terminated",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
			task:     cancelTask,
			attempt:  1,
			expected: true,
		},
		{
			name:     "cancel: scheduled",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			task:     cancelTask,
			attempt:  1,
			expected: false,
		},
		{
			name:     "cancel: started",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			task:     cancelTask,
			attempt:  1,
			expected: false,
		},
		{
			name:     "cancel: completed",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			task:     cancelTask,
			attempt:  1,
			expected: false,
		},
		{
			name:     "cancel: at max attempts",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			task:     cancelTask,
			attempt:  maxAttempts,
			expected: true,
		},
		{
			name:     "cancel: exceeds max attempts",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			task:     cancelTask,
			attempt:  maxAttempts + 1,
			expected: false,
		},
		{
			name:     "unknown command type",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			task:     &activitypb.WorkerCommandDispatchTask{},
			attempt:  1,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status: tc.status,
				},
			}
			ctx := &chasm.MockContext{
				HandleExecutionKey: func() chasm.ExecutionKey {
					return chasm.ExecutionKey{
						NamespaceID: "test-ns-id",
						BusinessID:  "test-activity-id",
					}
				},
			}
			invocation := chasm.TaskInvocation{Attempt: tc.attempt}
			valid, err := handler.Validate(ctx, activity, invocation, tc.task)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}
