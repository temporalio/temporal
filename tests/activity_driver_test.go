package tests

import (
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
)

// TestSAADriverRecognizesTerminalScheduleTimeouts verifies that a timeout-elapse event is realized
// when the terminal timeout type is reported in DescribeActivityExecution's Outcome rather than in
// ActivityExecutionInfo.LastFailure.
func (s *activityParityTestSuite) TestSAADriverRecognizesTerminalScheduleTimeouts() {
	env := newActivityParityEnv(s.T())
	tests := []struct {
		name  string
		trace []model.Event
		want  enumspb.TimeoutType
	}{
		{
			name:  "ScheduleToStart",
			trace: []model.Event{{Type: model.ScheduleToStartElapsesType}},
			want:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		},
		{
			name:  "ScheduleToClose",
			trace: []model.Event{model.Poll, {Type: model.ScheduleToCloseElapsesType}},
			want:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func(s *activityParityTestSuite) {
			t := s.T()
			activity := newSAADriver(t, env, activityConfig{MaxAttempts: 1}).driveTrace(t, test.trace)
			response := activity.describe(t)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, response.GetInfo().GetStatus())
			require.Equal(t, test.want,
				response.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
		})
	}
}
