package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
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

			retryState, err := activity.tryReschedule(ctx, retryEvent{failure: &failurepb.Failure{}})

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

// Both policies deliberately have different curves and counters: selecting the wrong one must
// change the observed retry state, delay or counter, rather than merely scheduling another task.
func TestHandoffRetryPolicySelection(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                  string
		handedOff             bool
		timer                 enumspb.TimeoutType
		failure               *failurepb.Failure
		mainNil               bool
		retryIntervalOverride time.Duration
		expectedType          activitypb.ActivityRetryType
		expectedRetryInterval time.Duration
	}{
		{
			name:                  "submission failure",
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second, // 7s initial × 3^(handoff attempt 2 − 1).
		},
		{
			name:                  "external failure",
			handedOff:             true,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second, // 5s initial × 2^(activity attempt 3 − 1).
		},
		{
			name:                  "submission timeout",
			timer:                 enumspb.TIMEOUT_TYPE_START_TO_HANDOFF,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second,
		},
		{
			name:                  "completion timeout",
			handedOff:             true,
			timer:                 enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second,
		},
		{
			name:                  "submission heartbeat",
			timer:                 enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second,
		},
		{
			name:                  "completion heartbeat",
			handedOff:             true,
			timer:                 enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second,
		},
		{
			name:                  "start-to-close before handoff",
			timer:                 enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second,
		},
		{
			name:                  "start-to-close after handoff",
			handedOff:             true,
			timer:                 enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second,
		},
		{
			name:                  "reported start to close before handoff",
			failure:               createActivityTimeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second,
		},
		{
			name:                  "reported completion timeout before handoff",
			failure:               createActivityTimeoutFailure(enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE),
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second,
		},
		{
			name:                  "reported submission timeout after handoff",
			handedOff:             true,
			failure:               createActivityTimeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_HANDOFF),
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY,
			expectedRetryInterval: 20 * time.Second,
		},
		{
			name:                  "main nil with valid submission policy",
			mainNil:               true,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 21 * time.Second,
		},
		{
			name:                  "worker overrides submission backoff",
			retryIntervalOverride: 37 * time.Second,
			expectedType:          activitypb.ACTIVITY_RETRY_TYPE_HANDOFF,
			expectedRetryInterval: 37 * time.Second,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			attempt := a.LastAttempt.Get(ctx)
			a.AwaitingCompletion = testCase.handedOff
			if testCase.mainNil {
				a.RetryPolicy = nil
			}

			event := retryEvent{
				serverTimeoutType:       testCase.timer,
				overridingRetryInterval: testCase.retryIntervalOverride,
				failure:                 testCase.failure,
			}
			if testCase.timer != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				event.failure = createActivityTimeoutFailure(testCase.timer)
			}

			before := common.CloneProto(attempt)
			now := ctx.Now(a)
			scheduleToStartTimeout := a.GetScheduleToStartTimeout().AsDuration()

			state, err := a.tryReschedule(ctx, event)
			require.NoError(t, err)
			require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, state)

			expected := common.CloneProto(before)
			expected.Stamp++
			expected.PendingRetryType = testCase.expectedType
			expected.CompleteTime = timestamppb.New(now)
			expected.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{Failure: event.failure, Time: expected.CompleteTime}
			if testCase.expectedType == activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY {
				expected.Count++
				expected.AsyncHandoffState.Count = 1
				expected.AsyncHandoffState.StartToCloseStartedTime = nil
				expected.AsyncHandoffState.StartToCloseStamp++
			} else {
				expected.AsyncHandoffState.Count++
			}

			expected.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
			if testCase.retryIntervalOverride > 0 {
				expected.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE
			}

			expected.CurrentRetryInterval = durationpb.New(testCase.expectedRetryInterval)
			expected.DispatchTime = timestamppb.New(now.Add(testCase.expectedRetryInterval))
			protorequire.ProtoEqual(t, expected, attempt)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, a.GetStatus())
			require.False(t, a.GetAwaitingCompletion())
			require.Equal(t, []chasm.MockTask{
				{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: expected.DispatchTime.AsTime().Add(scheduleToStartTimeout)}, Payload: &activitypb.ScheduleToStartTimeoutTask{Stamp: expected.Stamp}},
				{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: expected.DispatchTime.AsTime()}, Payload: &activitypb.ActivityDispatchTask{Stamp: expected.Stamp, DispatchReason: activitypb.DISPATCH_REASON_RETRY, StartDelayBucket: activitypb.START_DELAY_BUCKET_NONE}},
			}, ctx.Tasks)
		})
	}
}

func TestHandoffRetryEligibility(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                     string
		handedOff                bool
		timer                    enumspb.TimeoutType
		failure                  *failurepb.Failure
		mainNil, handoffNil      bool
		excluded                 bool
		maximumAttempts          int32
		scheduleToCloseRemaining time.Duration
		expectedType             activitypb.ActivityRetryType
		expectedState            enumspb.RetryState
	}{
		{
			name: "handoff retry reaches schedule-to-close", scheduleToCloseRemaining: 21 * time.Second,
			expectedType: activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, expectedState: enumspb.RETRY_STATE_TIMEOUT,
		},
		{
			name: "activity retry reaches schedule-to-close", handedOff: true, scheduleToCloseRemaining: 20 * time.Second,
			expectedType: activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, expectedState: enumspb.RETRY_STATE_TIMEOUT,
		},
		{
			name: "submission application exclusion", excluded: true,
			failure:      &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "application-error"}}},
			expectedType: activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, expectedState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		},
		{
			name: "external application exclusion", handedOff: true, excluded: true,
			failure:      &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "application-error"}}},
			expectedType: activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, expectedState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		},
		{name: "handoff nil with valid main policy", handoffNil: true, expectedType: activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, expectedState: enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET},
		{name: "selected main nil", mainNil: true, handedOff: true, expectedType: activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, expectedState: enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET},
		{name: "handoff exclusions", timer: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF, excluded: true, expectedType: activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, expectedState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE},
		{name: "main exclusions", handedOff: true, timer: enumspb.TIMEOUT_TYPE_HEARTBEAT, excluded: true, expectedType: activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, expectedState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE},
		{name: "handoff maximum attempts", maximumAttempts: 2, expectedType: activitypb.ACTIVITY_RETRY_TYPE_HANDOFF, expectedState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED},
		{name: "main maximum attempts", handedOff: true, maximumAttempts: 3, expectedType: activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, expectedState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			attempt := a.LastAttempt.Get(ctx)
			a.AwaitingCompletion = testCase.handedOff
			if testCase.scheduleToCloseRemaining > 0 {
				a.ScheduleToCloseTimeout = durationpb.New(ctx.Now(a).Sub(a.GetScheduleTime().AsTime()) + testCase.scheduleToCloseRemaining)
			}

			if testCase.mainNil {
				a.RetryPolicy = nil
			}

			if testCase.handoffNil {
				a.AsyncHandoffOptions.RetryPolicy = nil
			}

			policy := a.GetAsyncHandoffOptions().GetRetryPolicy()
			if testCase.expectedType == activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY {
				policy = a.GetRetryPolicy()
			}

			if testCase.excluded {
				if testCase.timer == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
					policy.NonRetryableErrorTypes = []string{"application-error"}
				} else {
					policy.NonRetryableErrorTypes = []string{retrypolicy.TimeoutFailureTypePrefix + testCase.timer.String()}
				}
			}

			if testCase.maximumAttempts > 0 {
				policy.MaximumAttempts = testCase.maximumAttempts
			}

			event := retryEvent{failure: testCase.failure, serverTimeoutType: testCase.timer}
			if testCase.timer != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				event.failure = createActivityTimeoutFailure(testCase.timer)
			}

			before := common.CloneProto(attempt)

			state, err := a.tryReschedule(ctx, event)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedState, state)
			protorequire.ProtoEqual(t, before, attempt)
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestHandoffRetryOperatorPrecedence(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name               string
		optionsPresent     bool
		handedOff          bool
		requestedStatus    activitypb.ActivityExecutionStatus
		nonRetryable       bool
		expectedRetryState enumspb.RetryState
		expectedStatus     activitypb.ActivityExecutionStatus
	}{
		{
			name:               "pause before handoff without options",
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		},
		{
			name: "reset before handoff without options", nonRetryable: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_IN_PROGRESS,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name: "pause after handoff without options", handedOff: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		},
		{
			name: "reset after handoff without options", handedOff: true, nonRetryable: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_IN_PROGRESS,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name: "pause before handoff with options", optionsPresent: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		},
		{
			name: "reset before handoff with options", optionsPresent: true, nonRetryable: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_IN_PROGRESS,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name: "pause after handoff with options", optionsPresent: true, handedOff: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
		},
		{
			name: "reset after handoff with options", optionsPresent: true, handedOff: true, nonRetryable: true,
			requestedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			expectedRetryState: enumspb.RETRY_STATE_IN_PROGRESS,
			expectedStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			a.AwaitingCompletion = testCase.handedOff
			if !testCase.optionsPresent {
				a.AsyncHandoffOptions = nil
			}

			a.Status = testCase.requestedStatus
			policy, _ := a.retryPolicyAndCount(ctx, a.retryTypeForFailure())
			policy.MaximumAttempts = 1
			failure := &failurepb.Failure{FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: testCase.nonRetryable}}}

			state, err := a.tryReschedule(ctx, retryEvent{failure: failure})
			require.NoError(t, err)
			require.Equal(t, testCase.expectedRetryState, state)
			require.Equal(t, testCase.expectedStatus, a.GetStatus())

			if testCase.expectedStatus == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
				// A pending reset still applies to a non-retryable failure.
				require.EqualValues(t, 1, a.LastAttempt.Get(ctx).GetCount())
				require.Nil(t, a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetStartToCloseStartedTime())
				require.NotEmpty(t, ctx.Tasks)
			} else {
				// Exhausting retries leaves the pause request pending.
				require.Empty(t, ctx.Tasks)
			}
		})
	}
}

func newHandoffRetryTestActivity(t *testing.T) (*Activity, *chasm.MockMutableContext) {
	t.Helper()

	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return defaultTime.Add(10 * time.Second) }}}
	injectActivityContext(t, ctx)
	activityContextFromChasm(ctx).config.BreakdownMetricsByTaskQueue = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true)
	a := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			ScheduleTime:           timestamppb.New(defaultTime),
			ActivityType:           &commonpb.ActivityType{Name: "test-activity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-queue"},
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
			ScheduleToStartTimeout: durationpb.New(40 * time.Second),
			StartToCloseTimeout:    durationpb.New(time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(5 * time.Second),
				MaximumInterval:    durationpb.New(time.Minute),
				BackoffCoefficient: 2,
				MaximumAttempts:    5,
			},
			AsyncHandoffOptions: &apiactivitypb.AsyncHandoffOptions{
				StartToHandoffTimeout: durationpb.New(30 * time.Second),
				HandoffToCloseTimeout: durationpb.New(50 * time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(7 * time.Second),
					MaximumInterval:    durationpb.New(time.Minute),
					BackoffCoefficient: 3,
					MaximumAttempts:    4,
				},
			},
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:        3,
			Stamp:        7,
			StartedStamp: 7,
			StartedTime:  timestamppb.New(defaultTime),
			AsyncHandoffState: &activitypb.ActivityAttemptState_AsyncHandoffState{
				Count:                   2,
				StartToCloseStartedTime: timestamppb.New(defaultTime),
				StartToCloseStamp:       4,
			},
		}),
		Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{}),
	}

	return a, ctx
}

func TestTryRescheduleRejectsInvalidTimeout(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name           string
		withoutOptions bool
		timeout        enumspb.TimeoutType
	}{
		{name: "submission timeout without options", withoutOptions: true, timeout: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF},
		{name: "completion timeout without options", withoutOptions: true, timeout: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE},
		{name: "unknown timeout", timeout: enumspb.TimeoutType(100)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			if testCase.withoutOptions {
				a.AsyncHandoffOptions = nil
			}

			beforeState := common.CloneProto(a.ActivityState)
			beforeAttempt := common.CloneProto(a.LastAttempt.Get(ctx))

			_, err := a.tryReschedule(ctx, retryEvent{
				serverTimeoutType: testCase.timeout,
				failure:           createActivityTimeoutFailure(testCase.timeout),
			})
			require.ErrorAs(t, err, new(*serviceerror.Internal))
			protorequire.ProtoEqual(t, beforeState, a.ActivityState)
			protorequire.ProtoEqual(t, beforeAttempt, a.LastAttempt.Get(ctx))
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestCalculateHandoffDeadlines(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name     string
		input    handoffDeadlineInput
		expected handoffDeadlines
		invalid  bool
	}{
		{
			name: "worker phase",
			input: handoffDeadlineInput{DeliveryActive: true, TaskStartedAt: start.Add(10 * time.Second), StartToCloseStartedAt: start,
				HandoffAt: start, LastHeartbeatAt: start.Add(5 * time.Second),
				StartToCloseTimeout: time.Minute, StartToHandoffTimeout: 30 * time.Second, HandoffToCloseTimeout: time.Minute, PreHandoffHeartbeatTimeout: 20 * time.Second, PostHandoffHeartbeatTimeout: time.Minute},
			expected: handoffDeadlines{StartToClose: start.Add(time.Minute), StartToHandoff: start.Add(40 * time.Second), Heartbeat: start.Add(30 * time.Second)},
		},
		{
			name: "completion phase ignores past submission deadline",
			input: handoffDeadlineInput{DeliveryActive: true, AwaitingCompletion: true, TaskStartedAt: start, StartToCloseStartedAt: start, HandoffAt: start.Add(20 * time.Second), LastHeartbeatAt: start.Add(15 * time.Second),
				StartToCloseTimeout: 2 * time.Hour, StartToHandoffTimeout: 10 * time.Second, HandoffToCloseTimeout: time.Hour, PostHandoffHeartbeatTimeout: 30 * time.Second},
			expected: handoffDeadlines{StartToClose: start.Add(2 * time.Hour), HandoffToClose: start.Add(time.Hour + 20*time.Second), Heartbeat: start.Add(50 * time.Second)},
		},
		{
			name: "heartbeat after handoff",
			input: handoffDeadlineInput{DeliveryActive: true, AwaitingCompletion: true, TaskStartedAt: start, StartToCloseStartedAt: start, HandoffAt: start.Add(20 * time.Second),
				LastHeartbeatAt: start.Add(40 * time.Second), PostHandoffHeartbeatTimeout: 30 * time.Second},
			expected: handoffDeadlines{Heartbeat: start.Add(70 * time.Second)},
		},
		{
			name:     "carried budget without delivery",
			input:    handoffDeadlineInput{StartToCloseStartedAt: start, StartToCloseTimeout: time.Minute, StartToHandoffTimeout: time.Second, HandoffToCloseTimeout: time.Hour, HandoffAt: start},
			expected: handoffDeadlines{StartToClose: start.Add(time.Minute)},
		},
		{
			name:  "inactive historical anchors",
			input: handoffDeadlineInput{TaskStartedAt: start, HandoffAt: start, StartToCloseTimeout: time.Minute, StartToHandoffTimeout: time.Second},
		},
		{
			name:  "pre handoff heartbeat disabled independently",
			input: handoffDeadlineInput{DeliveryActive: true, TaskStartedAt: start, StartToCloseStartedAt: start, PostHandoffHeartbeatTimeout: time.Second},
		},
		{
			name: "post handoff heartbeat disabled independently",
			input: handoffDeadlineInput{DeliveryActive: true, AwaitingCompletion: true, TaskStartedAt: start, StartToCloseStartedAt: start,
				HandoffAt: start.Add(20 * time.Second), PreHandoffHeartbeatTimeout: time.Second},
		},
		{
			name:    "missing pickup",
			input:   handoffDeadlineInput{DeliveryActive: true, StartToCloseStartedAt: start},
			invalid: true,
		},
		{
			name:  "start to close disabled during backoff",
			input: handoffDeadlineInput{StartToCloseStartedAt: start},
		},
		{
			name:    "missing start to close start with disabled timeout",
			input:   handoffDeadlineInput{DeliveryActive: true, TaskStartedAt: start},
			invalid: true,
		},
		{
			name:    "missing start to close start with enabled timeout",
			input:   handoffDeadlineInput{DeliveryActive: true, TaskStartedAt: start, StartToCloseTimeout: time.Minute},
			invalid: true,
		},
		{
			name:    "missing handoff",
			input:   handoffDeadlineInput{DeliveryActive: true, AwaitingCompletion: true, TaskStartedAt: start, StartToCloseStartedAt: start},
			invalid: true,
		},
		{
			name:    "completion without delivery",
			input:   handoffDeadlineInput{AwaitingCompletion: true, HandoffAt: start},
			invalid: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := calculateHandoffDeadlines(testCase.input)
			if testCase.invalid {
				require.ErrorAs(t, err, new(*serviceerror.Internal))
				require.Equal(t, handoffDeadlines{}, actual)
				return
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expected, actual)
		})
	}
}
