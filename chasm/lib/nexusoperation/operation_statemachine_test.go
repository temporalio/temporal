package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	defaultTime                   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultScheduleToCloseTimeout = 10 * time.Minute
	defaultScheduleToStartTimeout = 5 * time.Minute
)

func newTestOperation() *Operation {
	ctx := &chasm.MockMutableContext{}
	op := &Operation{
		OperationState: &nexusoperationpb.OperationState{
			Status:                 nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
			EndpointId:             "endpoint-id",
			Endpoint:               "test-endpoint",
			Service:                "test-service",
			Operation:              "test-operation",
			ScheduledTime:          timestamppb.New(defaultTime),
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			RequestId:              "request-id",
			Attempt:                0,
		},
	}
	op.Outcome = chasm.NewDataField(ctx, &nexusoperationpb.OperationOutcome{})
	return op
}

func TestTransitionScheduled(t *testing.T) {
	testCases := []struct {
		name                   string
		scheduleToCloseTimeout time.Duration
		scheduleToStartTimeout time.Duration
		expectedTasks          []chasm.MockTask
	}{
		{
			name:                   "schedules invocation and schedule-to-close timeout tasks",
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToCloseTimeout)},
					Payload:    &nexusoperationpb.ScheduleToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                   "schedules invocation and schedule-to-start timeout tasks",
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToStartTimeout)},
					Payload:    &nexusoperationpb.ScheduleToStartTimeoutTask{},
				},
			},
		},
		{
			name:                   "schedules invocation and both timeout tasks",
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToStartTimeout)},
					Payload:    &nexusoperationpb.ScheduleToStartTimeoutTask{},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToCloseTimeout)},
					Payload:    &nexusoperationpb.ScheduleToCloseTimeoutTask{},
				},
			},
		},
		{
			name: "schedules only invocation task when no timeouts set",
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.ScheduleToCloseTimeout = durationpb.New(tc.scheduleToCloseTimeout)
			operation.ScheduleToStartTimeout = durationpb.New(tc.scheduleToStartTimeout)

			err := TransitionScheduled.Apply(operation, ctx, EventScheduled{})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, operation.Status)

			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]
				require.Equal(t, expectedTask.Attributes, actualTask.Attributes)
				protorequire.ProtoEqual(t, expectedTask.Payload.(proto.Message), actualTask.Payload.(proto.Message))
			}
		})
	}
}

func TestTransitionAttemptFailed(t *testing.T) {
	testCases := []struct {
		name                 string
		startingAttemptCount int32
		expectedAttempt      int32
		minRetryInterval     time.Duration
		maxRetryInterval     time.Duration
		retryPolicy          backoff.RetryPolicy
	}{
		{
			name:                 "first retry",
			startingAttemptCount: 1,
			expectedAttempt:      1,
			minRetryInterval:     500 * time.Millisecond,  // With jitter, minimum is ~50% of base
			maxRetryInterval:     1500 * time.Millisecond, // With jitter, maximum is ~150% of base
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "second retry",
			startingAttemptCount: 2,
			expectedAttempt:      2,
			minRetryInterval:     1 * time.Second,
			maxRetryInterval:     3 * time.Second,
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "third retry",
			startingAttemptCount: 3,
			expectedAttempt:      3,
			minRetryInterval:     2 * time.Second,
			maxRetryInterval:     6 * time.Second,
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Attempt = tc.startingAttemptCount
			operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

			failure := &failurepb.Failure{
				Message: "test failure",
			}

			event := EventAttemptFailed{
				Failure:     failure,
				RetryPolicy: tc.retryPolicy,
			}

			err := transitionAttemptFailed.Apply(operation, ctx, event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, operation.Status)
			require.Equal(t, tc.expectedAttempt, operation.Attempt)
			require.Equal(t, defaultTime, operation.LastAttemptCompleteTime.AsTime())
			require.Equal(t, failure, operation.LastAttemptFailure)
			require.NotNil(t, operation.NextAttemptScheduleTime)
			require.True(t, operation.NextAttemptScheduleTime.AsTime().After(defaultTime))

			// Verify retry interval is within expected range (due to jitter)
			actualInterval := operation.NextAttemptScheduleTime.AsTime().Sub(defaultTime)
			require.GreaterOrEqual(t, actualInterval, tc.minRetryInterval, "retry interval %v should be >= %v", actualInterval, tc.minRetryInterval)
			require.LessOrEqual(t, actualInterval, tc.maxRetryInterval, "retry interval %v should be <= %v", actualInterval, tc.maxRetryInterval)

			// Verify backoff task
			require.Len(t, ctx.Tasks, 1)
			backoffTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationBackoffTask)
			require.True(t, ok, "expected InvocationBackoffTask")
			require.Equal(t, tc.expectedAttempt, backoffTask.Attempt)
			require.Equal(t, operation.NextAttemptScheduleTime.AsTime(), ctx.Tasks[0].Attributes.ScheduledTime)
		})
	}
}

func TestTransitionRescheduled(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	operation.Attempt = 2
	operation.NextAttemptScheduleTime = timestamppb.New(defaultTime.Add(time.Minute))

	event := EventRescheduled{}

	err := transitionRescheduled.Apply(operation, ctx, event)
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, operation.Status)
	require.Equal(t, int32(3), operation.Attempt)

	// Verify NextAttemptScheduleTime was cleared
	require.Nil(t, operation.NextAttemptScheduleTime)

	// Verify invocation task
	require.Len(t, ctx.Tasks, 1)
	invTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
	require.Equal(t, int32(3), invTask.Attempt)
}

func TestTransitionStarted(t *testing.T) {
	defaultStartToCloseTimeout := 5 * time.Minute
	customStartTime := defaultTime.Add(time.Minute)

	testCases := []struct {
		name                string
		startToCloseTimeout time.Duration
		startTime           *time.Time
		pendingCancellation bool
		expectedTasks       []chasm.MockTask
	}{
		{
			name:                "emits start-to-close timeout task",
			startToCloseTimeout: defaultStartToCloseTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultStartToCloseTimeout)},
					Payload:    &nexusoperationpb.StartToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                "start-to-close timeout uses event StartTime",
			startToCloseTimeout: defaultStartToCloseTimeout,
			startTime:           &customStartTime,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: customStartTime.Add(defaultStartToCloseTimeout)},
					Payload:    &nexusoperationpb.StartToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                "schedules pending cancellation",
			pendingCancellation: true,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.CancellationTask{Attempt: 1},
				},
			},
		},
		{
			name: "no tasks when timeout not set",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED
			operation.Attempt = 1
			operation.StartToCloseTimeout = durationpb.New(tc.startToCloseTimeout)
			if tc.pendingCancellation {
				cancellation := newCancellation(
					&nexusoperationpb.CancellationState{
						Status:        nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
						RequestedTime: timestamppb.New(defaultTime),
					},
				)
				cancellation.Operation = chasm.NewMockParentPtr[*Operation](operation)
				operation.Cancellation = chasm.NewComponentField[*Cancellation](nil, cancellation)
			}

			err := TransitionStarted.Apply(operation, ctx, EventStarted{
				OperationToken: "test-token",
				StartTime:      tc.startTime,
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, operation.Status)

			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]
				require.Equal(t, expectedTask.Attributes, actualTask.Attributes)
				protorequire.ProtoEqual(t, expectedTask.Payload.(proto.Message), actualTask.Payload.(proto.Message))
			}
		})
	}
}

func TestTransitionSucceeded(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)

	testCases := []struct {
		name               string
		completeTime       *time.Time
		result             *commonpb.Payload
		expectedClosedTime time.Time
	}{
		{
			name:               "uses default time",
			result:             mustToPayload(t, "result"),
			expectedClosedTime: defaultTime,
		},
		{
			name:               "uses event CompleteTime",
			completeTime:       &customCompleteTime,
			result:             mustToPayload(t, "result"),
			expectedClosedTime: customCompleteTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

			err := TransitionSucceeded.Apply(operation, ctx, EventSucceeded{
				CompleteTime: tc.completeTime,
				Result:       tc.result,
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, operation.Status)
			require.Equal(t, tc.expectedClosedTime, operation.ClosedTime.AsTime())

			outcome, ok := operation.Outcome.TryGet(ctx)
			require.True(t, ok)
			require.NotNil(t, outcome.GetSuccessful())
			protorequire.ProtoEqual(t, tc.result, outcome.GetSuccessful().GetResult())
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionFailed(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)
	failure := &failurepb.Failure{Message: "test failure"}

	for _, tc := range []struct {
		name       string
		fromStatus nexusoperationpb.OperationStatus
		event      EventFailed
		prepare    func(*Operation)
		assert     func(*testing.T, *chasm.MockMutableContext, *Operation)
	}{
		{
			name:       "from scheduled records last attempt failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			event:      EventFailed{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, failure, operation.LastAttemptFailure)
				require.Nil(t, operation.Outcome.Get(ctx).GetVariant())
			},
		},
		{
			name:       "from non-scheduled stores outcome failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventFailed{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, failure, operation.Outcome.Get(ctx).GetFailed().GetFailure())
				require.Nil(t, operation.LastAttemptFailure)
			},
		},
		{
			name:       "uses default time",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventFailed{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, defaultTime, operation.ClosedTime.AsTime())
			},
		},
		{
			name:       "uses event CompleteTime",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventFailed{Failure: failure, CompleteTime: &customCompleteTime},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, customCompleteTime, operation.ClosedTime.AsTime())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = tc.fromStatus
			if tc.prepare != nil {
				tc.prepare(operation)
			}

			err := TransitionFailed.Apply(operation, ctx, tc.event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, operation.Status)
			require.Nil(t, operation.NextAttemptScheduleTime)
			require.Empty(t, ctx.Tasks)
			tc.assert(t, ctx, operation)
		})
	}
}

func TestTransitionCanceled(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)
	failure := &failurepb.Failure{Message: "canceled"}

	for _, tc := range []struct {
		name       string
		fromStatus nexusoperationpb.OperationStatus
		event      EventCanceled
		prepare    func(*Operation)
		assert     func(*testing.T, *chasm.MockMutableContext, *Operation)
	}{
		{
			name:       "from scheduled records last attempt failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			event:      EventCanceled{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, failure, operation.LastAttemptFailure)
				require.Nil(t, operation.Outcome.Get(ctx).GetVariant())
			},
		},
		{
			name:       "from non-scheduled stores outcome failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventCanceled{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, failure, operation.Outcome.Get(ctx).GetFailed().GetFailure())
				require.Nil(t, operation.LastAttemptFailure)
			},
		},
		{
			name:       "uses default time",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventCanceled{Failure: failure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, defaultTime, operation.ClosedTime.AsTime())
			},
		},
		{
			name:       "uses event CompleteTime",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventCanceled{Failure: failure, CompleteTime: &customCompleteTime},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, customCompleteTime, operation.ClosedTime.AsTime())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = tc.fromStatus
			if tc.prepare != nil {
				tc.prepare(operation)
			}

			err := TransitionCanceled.Apply(operation, ctx, tc.event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, operation.Status)
			require.Nil(t, operation.NextAttemptScheduleTime)
			require.Empty(t, ctx.Tasks)
			tc.assert(t, ctx, operation)
		})
	}
}

func TestTransitionTimedOut(t *testing.T) {
	timeoutFailure := &failurepb.Failure{Message: "operation timed out"}
	attemptFailure := &failurepb.Failure{Message: "attempt timed out"}

	for _, tc := range []struct {
		name       string
		fromStatus nexusoperationpb.OperationStatus
		event      EventTimedOut
		prepare    func(*Operation)
		assert     func(*testing.T, *chasm.MockMutableContext, *Operation)
	}{
		{
			name:       "when not from attempt stores outcome failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventTimedOut{Failure: timeoutFailure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, timeoutFailure, operation.Outcome.Get(ctx).GetFailed().GetFailure())
				require.Nil(t, operation.LastAttemptFailure)
				require.Nil(t, operation.LastAttemptCompleteTime)
			},
		},
		{
			name:       "when from attempt records last attempt failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			event:      EventTimedOut{Failure: attemptFailure, FromAttempt: true},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, attemptFailure, operation.LastAttemptFailure)
				require.Nil(t, operation.Outcome.Get(ctx).GetFailed())
			},
		},
		{
			name:       "uses default time",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			event:      EventTimedOut{Failure: timeoutFailure},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, defaultTime, operation.ClosedTime.AsTime())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = tc.fromStatus
			if tc.prepare != nil {
				tc.prepare(operation)
			}

			err := TransitionTimedOut.Apply(operation, ctx, tc.event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, operation.Status)
			require.Nil(t, operation.NextAttemptScheduleTime)
			require.Empty(t, ctx.Tasks)
			tc.assert(t, ctx, operation)
		})
	}
}

func TestTransitionTerminated(t *testing.T) {
	event := EventTerminated{TerminateComponentRequest: chasm.TerminateComponentRequest{
		RequestID: "terminate-request-id",
		Reason:    "test reason",
		Identity:  "test-identity",
	}}

	for _, tc := range []struct {
		name       string
		fromStatus nexusoperationpb.OperationStatus
		prepare    func(*Operation)
		assert     func(*testing.T, *chasm.MockMutableContext, *Operation)
	}{
		{
			name:       "without prior last attempt failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Nil(t, operation.LastAttemptFailure)
			},
		},
		{
			name:       "preserves prior last attempt failure",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			prepare: func(operation *Operation) {
				operation.LastAttemptFailure = &failurepb.Failure{Message: "prior attempt failure"}
			},
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				protorequire.ProtoEqual(t, &failurepb.Failure{Message: "prior attempt failure"}, operation.LastAttemptFailure)
			},
		},
		{
			name:       "uses default time",
			fromStatus: nexusoperationpb.OPERATION_STATUS_STARTED,
			assert: func(t *testing.T, ctx *chasm.MockMutableContext, operation *Operation) {
				require.Equal(t, defaultTime, operation.ClosedTime.AsTime())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}
			operation := newTestOperation()
			operation.Status = tc.fromStatus
			if tc.prepare != nil {
				tc.prepare(operation)
			}

			err := TransitionTerminated.Apply(operation, ctx, event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_TERMINATED, operation.Status)
			protorequire.ProtoEqual(t, &nexusoperationpb.NexusOperationTerminateState{
				RequestId: "terminate-request-id",
			}, operation.TerminateState)
			tc.assert(t, ctx, operation)

			protorequire.ProtoEqual(t, &nexusoperationpb.OperationOutcome{
				Variant: &nexusoperationpb.OperationOutcome_Failed_{
					Failed: &nexusoperationpb.OperationOutcome_Failed{
						Failure: &failurepb.Failure{
							Message: "test reason",
							FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
								TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
									Identity: "test-identity",
								},
							},
						},
					},
				},
			}, operation.Outcome.Get(ctx))
			require.Empty(t, ctx.Tasks)
		})
	}
}
