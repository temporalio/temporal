package activity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/workercommands"
)

func TestCancelCommandDispatchTaskHandler_Validate(t *testing.T) {
	h := &cancelCommandDispatchTaskHandler{}

	newActivity := func(status activitypb.ActivityExecutionStatus) *Activity {
		return &Activity{ActivityState: &activitypb.ActivityState{Status: status}}
	}

	testCases := []struct {
		name      string
		status    activitypb.ActivityExecutionStatus
		attempt   int
		wantValid bool
		wantErr   error
	}{
		{
			name:      "cancel requested within attempt budget is valid",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:   1,
			wantValid: true,
		},
		{
			name:      "terminated at the attempt limit is still valid",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
			attempt:   workercommands.MaxTaskAttempts,
			wantValid: true,
		},
		{
			name:      "other status is not valid",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attempt:   1,
			wantValid: false,
		},
		{
			// Attempt is 0 when validated outside of task processing (e.g. transaction close);
			// the give-up check must not trip there.
			name:      "attempt zero does not trigger give-up",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:   0,
			wantValid: true,
		},
		{
			name:      "exceeding max attempts gives up regardless of status",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attempt:   workercommands.MaxTaskAttempts + 1,
			wantValid: false,
			wantErr:   chasm.ErrTaskAttemptsExhausted,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := h.Validate(
				nil,
				newActivity(tc.status),
				chasm.TaskAttributes{Attempt: tc.attempt},
				&activitypb.CancelCommandDispatchTask{},
			)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantValid, valid)
		})
	}
}
