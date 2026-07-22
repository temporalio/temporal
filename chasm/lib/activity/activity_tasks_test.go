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
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
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

func TestAttemptTimeoutTaskTerminalFailureType(t *testing.T) {
	testCases := []struct {
		name                string
		timeoutType         enumspb.TimeoutType
		maximumAttempts     int32
		scheduleToClose     time.Duration
		expectedTimeoutType enumspb.TimeoutType
		expectedMessage     string
	}{
		{
			name:                "start to close deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "heartbeat deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "start to close maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                "heartbeat maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-namespace"), nil)

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime.Add(1500 * time.Millisecond) },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false),
						},
						namespaceRegistry: nsRegistry,
					}),
				},
			}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    durationpb.New(time.Second),
						BackoffCoefficient: 1,
						MaximumAttempts:    tc.maximumAttempts,
					},
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToClose),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
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
			if tc.expectedMessage != "" {
				require.Equal(t, tc.expectedMessage, terminalFailure.GetMessage())
				require.NotNil(t, activity.Outcome.Get(ctx).GetFailed())
			} else {
				require.Nil(t, activity.Outcome.Get(ctx).GetVariant())
			}
			lastFailure := activity.LastAttempt.Get(ctx).GetLastFailureDetails().GetFailure()
			require.Equal(t, tc.timeoutType, lastFailure.GetTimeoutFailureInfo().GetTimeoutType())
			require.Equal(t, lastFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), terminalFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())

			for _, metricName := range []string{metrics.ActivityTimeout.Name(), metrics.ActivityTaskTimeout.Name()} {
				recordings := capture.Snapshot()[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.timeoutType.String(), recordings[0].Tags["timeout_type"])
			}
		})
	}
}
