package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/retrypolicy"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestScheduleToCloseTimeoutTaskValidateStamp(t *testing.T) {
	handler := newScheduleToCloseTimeoutTaskHandler()

	newActivity := func(stamp int32) *Activity {
		return &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToCloseStamp:   stamp,
			},
		}
	}

	testCases := []struct {
		name          string
		activityStamp int32
		taskStamp     int32
		wantValid     bool
	}{
		// A legacy activity (created before ScheduleToCloseStamp existed) has stamp 0 on both
		// the state and the timer task. Its timer is valid only while the activity stamp is
		// still 0; once an options update bumps the stamp, the zero-stamp timer must be rejected.
		{"legacy task valid while activity stamp still zero", 0, 0, true},
		{"legacy zero-stamp task invalid after stamp bumped", 5, 0, false},
		{"matching stamp valid", 5, 5, true},
		{"stale non-zero stamp invalid", 5, 3, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := handler.Validate(
				nil,
				newActivity(tc.activityStamp),
				chasm.TaskInvocation{},
				&activitypb.ScheduleToCloseTimeoutTask{Stamp: tc.taskStamp},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantValid, valid)
		})
	}
}

func TestTimeoutTaskTerminalFailure(t *testing.T) {
	testCases := []struct {
		name                string
		timeoutType         enumspb.TimeoutType
		startStatus         activitypb.ActivityExecutionStatus
		maximumAttempts     int32
		scheduleToClose     time.Duration
		nonRetryable        bool
		expectedTimeoutType enumspb.TimeoutType
		expectedRetryState  enumspb.RetryState
		expectedMessage     string
	}{
		{
			name:                "start to close deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "heartbeat deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "start to close maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "heartbeat maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "start to close non-retryable timeout",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     time.Minute,
			nonRetryable:        true,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		},
		{
			name:                "start to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
		{
			name:                "schedule to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime.Add(1500 * time.Millisecond) },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					HandleNamespaceEntry: testNamespaceEntry,
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false),
						},
					}),
				},
			}
			retryPolicy := &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1,
				MaximumAttempts:    tc.maximumAttempts,
			}
			if tc.nonRetryable {
				retryPolicy.NonRetryableErrorTypes = []string{
					retrypolicy.TimeoutFailureTypePrefix + tc.timeoutType.String(),
				}
			}
			startStatus := tc.startStatus
			if startStatus == activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED {
				startStatus = activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
			}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            retryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToClose),
					Status:                 startStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       1,
					StartedTime: timestamppb.New(defaultTime),
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
				LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("heartbeat details")}}},
				}),
			}

			var err error
			switch tc.timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = newScheduleToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.ScheduleToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				err = newStartToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.StartToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				err = newHeartbeatTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.HeartbeatTimeoutTask{})
			default:
				t.Fatalf("unexpected timeout type: %v", tc.timeoutType)
			}
			require.NoError(t, err)

			terminalFailure := activity.outcome(ctx).GetFailure()
			require.Equal(t, tc.expectedTimeoutType, terminalFailure.GetTimeoutFailureInfo().GetTimeoutType())
			require.Equal(t, tc.expectedRetryState, activity.outcome(ctx).GetRetryState())
			if tc.expectedMessage != "" {
				require.Equal(t, tc.expectedMessage, terminalFailure.GetMessage())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE || tc.expectedMessage != "" {
				require.NotNil(t, activity.Outcome.Get(ctx).GetFailed())
			} else {
				require.Nil(t, activity.Outcome.Get(ctx).GetVariant())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE {
				require.Nil(t, activity.LastAttempt.Get(ctx).GetLastFailureDetails())
			} else {
				lastFailure := activity.LastAttempt.Get(ctx).GetLastFailureDetails().GetFailure()
				require.Equal(t, tc.timeoutType, lastFailure.GetTimeoutFailureInfo().GetTimeoutType())
				require.Equal(t, lastFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), terminalFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
			}

			for _, metricName := range []string{metrics.ActivityTimeout.Name(), metrics.ActivityTaskTimeout.Name()} {
				recordings := capture.Snapshot()[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.timeoutType.String(), recordings[0].Tags["timeout_type"])
			}
		})
	}
}
