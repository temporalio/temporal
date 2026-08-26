package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTryRescheduleRetryStatePrecedence(t *testing.T) {
	testCases := []struct {
		name        string
		status      activitypb.ActivityExecutionStatus
		retryPolicy *commonpb.RetryPolicy
		expected    enumspb.RetryState
		finalStatus activitypb.ActivityExecutionStatus
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
			name:        "reset requested takes precedence over retry policy not set",
			status:      activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expected:    enumspb.RETRY_STATE_IN_PROGRESS,
			finalStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
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
			injectActivityContext(t, ctx) // needed for last test case
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:                 tc.status,
					RetryPolicy:            tc.retryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(time.Second),
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
			}

			retryState, err := activity.tryReschedule(ctx, false, 0, &failurepb.Failure{})

			require.NoError(t, err)
			require.Equal(t, tc.expected, retryState)
			require.Equal(t, tc.finalStatus, activity.GetStatus())
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
		startDelay              time.Duration
		firstAttemptStartedTime *timestamppb.Timestamp
		expectedReason          activitypb.DispatchReason
		expectedBucket          activitypb.StartDelayBucket
	}{
		{
			name:           "immediate",
			expectedReason: activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:           "start delay",
			startDelay:     time.Hour,
			expectedReason: activitypb.DISPATCH_REASON_START_DELAY,
			expectedBucket: activitypb.START_DELAY_BUCKET_1H_6H,
		},
		{
			name:                    "retry without configured start delay",
			firstAttemptStartedTime: timestamppb.Now(),
			expectedReason:          activitypb.DISPATCH_REASON_RETRY,
			expectedBucket:          activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:                    "retry preserves configured start delay bucket",
			startDelay:              time.Hour,
			firstAttemptStartedTime: timestamppb.Now(),
			expectedReason:          activitypb.DISPATCH_REASON_RETRY,
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
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Stamp: 1}),
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
