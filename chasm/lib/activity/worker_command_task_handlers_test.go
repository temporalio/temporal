package activity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/configs"
)

func TestCancelCommandDispatchTaskHandler_Validate(t *testing.T) {
	maxAttempts := 3
	handler := &cancelCommandDispatchTaskHandler{
		config: &configs.Config{
			WorkerCommandsMaxAttempts: dynamicconfig.GetIntPropertyFn(maxAttempts),
		},
		logger: log.NewNoopLogger(),
	}

	testCases := []struct {
		name     string
		status   activitypb.ActivityExecutionStatus
		attempt  int
		expected bool
	}{
		{
			name:     "cancel requested",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:  1,
			expected: true,
		},
		{
			name:     "terminated",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
			attempt:  1,
			expected: true,
		},
		{
			name:     "scheduled",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			attempt:  1,
			expected: false,
		},
		{
			name:     "started",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attempt:  1,
			expected: false,
		},
		{
			name:     "completed",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			attempt:  1,
			expected: false,
		},
		{
			name:     "cancel requested at max attempts",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:  maxAttempts,
			expected: true,
		},
		{
			name:     "cancel requested exceeds max attempts",
			status:   activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:  maxAttempts + 1,
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
			valid, err := handler.Validate(ctx, activity, invocation, nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}
