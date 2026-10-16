package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTryRescheduleRetryStatePrecedence(t *testing.T) {
	testCases := []struct {
		name             string
		failureRetryable bool
		status           activitypb.ActivityExecutionStatus
		retryPolicy      *commonpb.RetryPolicy
		expected         enumspb.RetryState
		finalStatus      activitypb.ActivityExecutionStatus
	}{
		{
			name:        "retry policy not set",
			status:      activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			expected:    enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			finalStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			name:        "retry policy not set takes precedence over cancellation",
			status:      activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			expected:    enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			finalStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:        "cancellation requested",
			status:      activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			retryPolicy: defaultRetryPolicy,
			expected:    enumspb.RETRY_STATE_CANCEL_REQUESTED,
			finalStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:             "reset requested takes precedence over retry policy not set",
			failureRetryable: true,
			status:           activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expected:         enumspb.RETRY_STATE_IN_PROGRESS,
			finalStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:        "non-retryable reset without retry policy",
			status:      activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expected:    enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			finalStatus: activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
		},
		{
			name:             "pause does not override missing retry policy",
			status:           activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			failureRetryable: true,
			expected:         enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			finalStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time {
						return defaultTime.Add(2 * time.Second)
					},
				},
			}
			injectActivityContext(t, ctx)
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:                 tc.status,
					RetryPolicy:            tc.retryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(time.Second),
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
			}

			failure := &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: !tc.failureRetryable},
			}}
			retryState, err := activity.tryReschedule(ctx, tc.failureRetryable, 0, failure)

			require.NoError(t, err)
			require.Equal(t, tc.expected, retryState)
			require.Equal(t, tc.finalStatus, activity.GetStatus())
		})
	}
}

func TestTryReschedulePendingOperatorIntent(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		status           activitypb.ActivityExecutionStatus
		retryable        bool
		maximumAttempts  int32
		keepPaused       bool
		workerDelay      time.Duration
		wantRetryState   enumspb.RetryState
		wantStatus       activitypb.ActivityExecutionStatus
		wantAttemptCount int32
	}{
		{"pause overrides exhausted budget", activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED, true, 3, false, 0,
			enumspb.RETRY_STATE_IN_PROGRESS, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED, 4},
		{"pause overrides insufficient retry window", activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED, true, 0, false, 2 * time.Hour,
			enumspb.RETRY_STATE_IN_PROGRESS, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED, 4},
		{"pause does not override non-retryability", activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED, false, 3, false, 0,
			enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED, 3},
		{"ordinary failure respects exhausted budget", activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, true, 3, false, 0,
			enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, 3},
		{"retryable reset overrides exhausted budget", activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED, true, 3, false, 0,
			enumspb.RETRY_STATE_IN_PROGRESS, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, 1},
		{"retryable reset preserves pause", activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED, true, 3, true, 0,
			enumspb.RETRY_STATE_IN_PROGRESS, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED, 1},
		{"non-retryable reset leaves deferred mutations unapplied", activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED, false, 3, true, 0,
			enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
				HandleNow: func(chasm.Component) time.Time { return defaultTime.Add(time.Second) },
			}}
			injectActivityContext(t, ctx)
			policy := common.CloneProto(defaultRetryPolicy)
			policy.MaximumAttempts = tc.maximumAttempts
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status: tc.status, RetryPolicy: policy,
					ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:           &taskqueuepb.TaskQueue{Name: "current-task-queue"},
					StartToCloseTimeout: durationpb.New(30 * time.Second),
					ScheduleTime:        timestamppb.New(defaultTime), ScheduleToCloseTimeout: durationpb.New(time.Hour),
					ScheduleToCloseStamp: 5, ResetShouldPause: tc.keepPaused,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 3, Stamp: 7}),
				LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("checkpoint")}}},
				}),
			}
			if tc.status == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
				activity.ResetRestoreOptions = true
				activity.ResetShouldClearHeartbeat = true
				activity.OriginalOptions = &apiactivitypb.ActivityOptions{
					TaskQueue:   &taskqueuepb.TaskQueue{Name: "original-task-queue"},
					RetryPolicy: common.CloneProto(policy), ScheduleToCloseTimeout: durationpb.New(2 * time.Hour),
					StartToCloseTimeout: durationpb.New(time.Minute),
				}
			}
			stateBefore := common.CloneProto(activity.ActivityState)
			attemptBefore := common.CloneProto(activity.LastAttempt.Get(ctx))
			heartbeatBefore := common.CloneProto(activity.LastHeartbeat.Get(ctx))
			failure := &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: !tc.retryable},
			}}

			retryState, err := activity.tryReschedule(ctx, tc.retryable, tc.workerDelay, failure)

			require.NoError(t, err)
			require.Equal(t, tc.wantRetryState, retryState)
			require.Equal(t, tc.wantStatus, activity.GetStatus())
			attempt := activity.LastAttempt.Get(ctx)
			require.Equal(t, tc.wantAttemptCount, attempt.GetCount())
			if tc.wantRetryState != enumspb.RETRY_STATE_IN_PROGRESS {
				protorequire.ProtoEqual(t, stateBefore, activity.ActivityState)
				protorequire.ProtoEqual(t, attemptBefore, attempt)
				protorequire.ProtoEqual(t, heartbeatBefore, activity.LastHeartbeat.Get(ctx))
				require.Empty(t, ctx.Tasks)
				return
			}
			require.EqualValues(t, 8, attempt.GetStamp())
			protorequire.ProtoEqual(t, failure, attempt.GetLastFailureDetails().GetFailure())
			if tc.status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED {
				require.Empty(t, ctx.Tasks)
				require.EqualValues(t, 5, activity.GetScheduleToCloseStamp())
				if tc.workerDelay == 0 {
					// Attempt 3 under the 1s initial interval and coefficient 2 policy backs off for 4s.
					require.Equal(t, 4*time.Second, attempt.GetCurrentRetryInterval().AsDuration())
					require.Equal(t, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY, attempt.GetCurrentRetryIntervalSource())
				} else {
					require.Equal(t, tc.workerDelay, attempt.GetCurrentRetryInterval().AsDuration())
					require.Equal(t, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE, attempt.GetCurrentRetryIntervalSource())
				}
			}
		})
	}
}

func TestStartDelayBucket(t *testing.T) {
	testCases := []struct {
		name     string
		delay    time.Duration
		expected activitypb.StartDelayBucket
	}{
		{name: "negative", delay: -time.Second, expected: activitypb.START_DELAY_BUCKET_NONE},
		{name: "zero", delay: 0, expected: activitypb.START_DELAY_BUCKET_NONE},
		{name: "less than one minute", delay: time.Minute - time.Nanosecond, expected: activitypb.START_DELAY_BUCKET_LT_1M},
		{name: "one minute", delay: time.Minute, expected: activitypb.START_DELAY_BUCKET_1M_10M},
		{name: "ten minutes", delay: 10 * time.Minute, expected: activitypb.START_DELAY_BUCKET_10M_1H},
		{name: "one hour", delay: time.Hour, expected: activitypb.START_DELAY_BUCKET_1H_6H},
		{name: "six hours", delay: 6 * time.Hour, expected: activitypb.START_DELAY_BUCKET_6H_1D},
		{name: "one day", delay: 24 * time.Hour, expected: activitypb.START_DELAY_BUCKET_1D_7D},
		{name: "seven days", delay: 7 * 24 * time.Hour, expected: activitypb.START_DELAY_BUCKET_7D_30D},
		{name: "thirty days", delay: 30 * 24 * time.Hour, expected: activitypb.START_DELAY_BUCKET_7D_30D},
		{name: "more than thirty days", delay: 30*24*time.Hour + time.Nanosecond, expected: activitypb.START_DELAY_BUCKET_GT_30D},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, startDelayBucket(tc.delay))
		})
	}
}

func TestNewActivityDispatchTask(t *testing.T) {
	testCases := []struct {
		name                    string
		attemptCount            int32
		startDelay              time.Duration
		firstAttemptStartedTime *timestamppb.Timestamp
		expectedReason          activitypb.DispatchReason
		expectedBucket          activitypb.StartDelayBucket
	}{
		{
			name:           "immediate",
			attemptCount:   1,
			expectedReason: activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:           "start delay",
			attemptCount:   1,
			startDelay:     time.Hour,
			expectedReason: activitypb.DISPATCH_REASON_START_DELAY,
			expectedBucket: activitypb.START_DELAY_BUCKET_1H_6H,
		},
		{
			name:           "attempt number is authoritative for retry",
			attemptCount:   2,
			expectedReason: activitypb.DISPATCH_REASON_RETRY,
			expectedBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:                    "retry preserves configured start delay bucket",
			attemptCount:            2,
			startDelay:              time.Hour,
			firstAttemptStartedTime: timestamppb.Now(),
			expectedReason:          activitypb.DISPATCH_REASON_RETRY,
			expectedBucket:          activitypb.START_DELAY_BUCKET_1H_6H,
		},
		{
			name:                    "attempt 1 after a prior worker start with configured start delay",
			attemptCount:            1,
			startDelay:              time.Hour,
			firstAttemptStartedTime: timestamppb.Now(),
			expectedReason:          activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedBucket:          activitypb.START_DELAY_BUCKET_1H_6H,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					StartDelay:              durationpb.New(tc.startDelay),
					FirstAttemptStartedTime: tc.firstAttemptStartedTime,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count: tc.attemptCount,
					Stamp: 1,
				}),
			}

			task := activity.newActivityDispatchTask(ctx)
			require.Equal(t, int32(1), task.GetStamp())
			require.Equal(t, tc.expectedReason, task.GetDispatchReason())
			require.Equal(t, tc.expectedBucket, task.GetStartDelayBucket())
		})
	}
}

// oversizedActivityFailure returns an application failure whose details push it past the retained
// failure size limit, mirroring the failure used by the workflow activity truncation tests.
func oversizedActivityFailure(t *testing.T) *failurepb.Failure {
	activityFailure := &failurepb.Failure{
		Message: "activity failure with large details",
		Source:  "application",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "application-failure-type",
			NonRetryable: false,
			Details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					{
						Data: make([]byte, defaultFailureSizeLimit*2),
					},
				},
			},
		}},
	}
	require.Greater(t, activityFailure.Size(), defaultFailureSizeLimit)
	return activityFailure
}

// withinLimitActivityFailure returns an application failure small enough to be retained whole.
func withinLimitActivityFailure(t *testing.T) *failurepb.Failure {
	activityFailure := &failurepb.Failure{
		Message: "activity failure",
		Source:  "application",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type: "application-failure-type",
		}},
	}
	require.LessOrEqual(t, activityFailure.Size(), defaultFailureSizeLimit)
	return activityFailure
}

// recordFailedAttempt truncates the retryable failure, and not the final failure.
func TestRecordFailedAttempt_FailureTruncation(t *testing.T) {
	requireTruncated := func(t *testing.T, sent, retained *failurepb.Failure) {
		require.LessOrEqual(t, retained.Size(), defaultFailureSizeLimit)
		require.Equal(t, common.FailureReasonFailureExceedsLimit, retained.GetMessage())
		require.NotNil(t, retained.GetServerFailureInfo())
		cause := retained.GetCause()
		require.Equal(t, sent.GetMessage(), cause.GetMessage())
		require.Equal(t, "application-failure-type", cause.GetApplicationFailureInfo().GetType())
		require.Nil(t, cause.GetApplicationFailureInfo().GetDetails())
	}
	requireKeptWhole := func(t *testing.T, sent, retained *failurepb.Failure) {
		protorequire.ProtoEqual(t, sent, retained)
	}

	testCases := []struct {
		name          string
		failure       func(*testing.T) *failurepb.Failure
		noRetriesLeft bool
		requireResult func(t *testing.T, sent, retained *failurepb.Failure)
	}{
		{
			name:          "OversizedRetryableFailureIsTruncated",
			failure:       oversizedActivityFailure,
			requireResult: requireTruncated,
		},
		{
			name:          "RetryableFailureWithinLimitIsKeptWhole",
			failure:       withinLimitActivityFailure,
			requireResult: requireKeptWhole,
		},
		{
			name:          "OversizedTerminalFailureIsKeptWhole",
			failure:       oversizedActivityFailure,
			noRetriesLeft: true,
			requireResult: requireKeptWhole,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			injectActivityContext(t, ctx)

			attemptState := &activitypb.ActivityAttemptState{Count: 1}
			activity := &Activity{
				LastAttempt: chasm.NewDataField(ctx, attemptState),
			}
			activityFailure := tc.failure(t)

			retryInterval, retryIntervalSource := time.Second, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
			if tc.noRetriesLeft {
				retryInterval, retryIntervalSource = 0, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED
			}

			err := activity.recordFailedAttempt(
				ctx,
				retryInterval,
				retryIntervalSource,
				activityFailure,
				defaultTime,
				tc.noRetriesLeft,
			)
			require.NoError(t, err)

			tc.requireResult(t, activityFailure, attemptState.GetLastFailureDetails().GetFailure())
		})
	}
}
