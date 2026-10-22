package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/stream_batcher"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testConcurrencyServiceClient struct {
	fcpb.ConcurrencyServiceClient
	batch func(context.Context, *fcpb.ConcurrencyBatchRequest, ...grpc.CallOption) (*fcpb.ConcurrencyBatchResponse, error)
}

func (c *testConcurrencyServiceClient) Batch(
	ctx context.Context,
	request *fcpb.ConcurrencyBatchRequest,
	opts ...grpc.CallOption,
) (*fcpb.ConcurrencyBatchResponse, error) {
	return c.batch(ctx, request, opts...)
}

func TestReleaseLimiterTaskExecute(t *testing.T) {
	releaseErr := errors.New("release failed")
	var requests []*fcpb.ConcurrencyBatchRequest
	handler := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{
		batch: func(
			_ context.Context,
			request *fcpb.ConcurrencyBatchRequest,
			_ ...grpc.CallOption,
		) (*fcpb.ConcurrencyBatchResponse, error) {
			requests = append(requests, request)
			if request.GetReleaseSlots()[0] == "bad-slot" {
				return nil, releaseErr
			}
			return &fcpb.ConcurrencyBatchResponse{}, nil
		},
	}, &Config{FlowControlClientBatcherOptions: dynamicconfig.GetTypedPropertyFn(stream_batcher.BatcherOptions{
		MaxItems: 1,
		IdleTime: time.Minute,
	})}, metrics.NoopMetricsHandler)
	task := &activitypb.ReleaseLimiterTask{Limiters: []*taskqueuespb.LimiterRef{
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "first-key", SlotId: "bad-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_UNSPECIFIED, Key: "ignored-key", SlotId: "ignored-slot"},
		{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "second-key", SlotId: "good-slot"},
	}}

	err := handler.Execute(t.Context(), chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
	}}, chasm.TaskAttributes{}, task)

	require.ErrorIs(t, err, releaseErr)
	require.Equal(t, []*fcpb.ConcurrencyBatchRequest{
		{NamespaceId: "namespace-id", Key: "first-key", ReleaseSlots: []string{"bad-slot"}},
		{NamespaceId: "namespace-id", Key: "second-key", ReleaseSlots: []string{"good-slot"}},
	}, requests)
}

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

func TestReleaseLatencyPreservesSchedulingTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		timestamp *timestamppb.Timestamp
		failed    bool
		want      bool
	}{
		{name: "success", timestamp: timestamppb.New(defaultTime), want: true},
		{name: "failed release", timestamp: timestamppb.New(defaultTime), failed: true},
		{name: "legacy task"},
		{name: "future timestamp", timestamp: timestamppb.New(defaultTime.Add(time.Second))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)
			clockSource := clock.NewEventTimeSource().Update(defaultTime.Add(150 * time.Millisecond))
			releaseErr := errors.New("release failed")
			h := newReleaseLimiterTaskHandler(&testConcurrencyServiceClient{batch: func(context.Context, *fcpb.ConcurrencyBatchRequest, ...grpc.CallOption) (*fcpb.ConcurrencyBatchResponse, error) {
				if tc.failed {
					return nil, releaseErr
				}
				return &fcpb.ConcurrencyBatchResponse{}, nil
			}}, &Config{FlowControlClientBatcherOptions: dynamicconfig.GetTypedPropertyFn(stream_batcher.BatcherOptions{MaxItems: 1, IdleTime: time.Minute})}, captureHandler)
			h.timeSource = clockSource
			task := &activitypb.ReleaseLimiterTask{ScheduledAt: tc.timestamp, Limiters: []*taskqueuespb.LimiterRef{{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "wholequeue/test/queue/2/0", SlotId: "slot"}}}
			err := h.Execute(t.Context(), chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{NamespaceID: "namespace-id"}}, chasm.TaskAttributes{}, task)
			if tc.failed {
				require.ErrorIs(t, err, releaseErr)
			} else {
				require.NoError(t, err)
			}
			records := capture.Snapshot()["flowcontrol_concurrency_release_latency"]
			if !tc.want {
				require.Empty(t, records)
				return
			}
			require.Len(t, records, 1)
			require.Equal(t, 150*time.Millisecond, records[0].Value)
			require.Equal(t, "namespace-id", records[0].Tags["namespace_id"])
			require.Equal(t, "test/queue", records[0].Tags["taskqueue"])
		})
	}
}

func TestReleaseLimiterTaskSchedulingTimestamp(t *testing.T) {
	t.Parallel()
	cctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return defaultTime }}}
	limiter := &taskqueuespb.LimiterRef{LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: "wholequeue/queue/2/0", SlotId: "slot"}
	a := &Activity{LastAttempt: chasm.NewDataField(cctx, &activitypb.ActivityAttemptState{Limiters: []*taskqueuespb.LimiterRef{limiter}})}
	a.releaseAttemptLimiters(cctx)
	require.Len(t, cctx.Tasks, 1)
	payload, ok := cctx.Tasks[0].Payload.(*activitypb.ReleaseLimiterTask)
	require.True(t, ok)
	require.Equal(t, defaultTime, payload.GetScheduledAt().AsTime())
	require.Equal(t, []*taskqueuespb.LimiterRef{limiter}, payload.GetLimiters())
}
