package nexusoperation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newCallbackTestContext builds a MockMutableContext with the plumbing the terminal transitions need
// (clock, namespace, metrics handler, and the operation context used for metric tags).
func newCallbackTestContext() *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewNamespaceForTest(
					&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0,
				)
			},
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{BusinessID: "operation-id", RunID: "run-id"}
			},
			HandleMetricsHandler: func() metrics.Handler { return metricstest.NewCaptureHandler() },
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MetricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
			}),
		},
	}
}

// attachStandbyCallbacks attaches n STANDBY completion callbacks to the operation. Each callback carries
// a valid Nexus variant so it can be scheduled (its invocation task destination is derived from the URL).
func attachStandbyCallbacks(ctx chasm.MutableContext, op *Operation, n int) {
	op.Callbacks = make(chasm.Map[string, *callback.Callback], n)
	for i := range n {
		chasmCB := &callbackspb.Callback{
			Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url: chasm.NexusCompletionHandlerURL,
				},
			},
		}
		cb := callback.NewCallback(
			fmt.Sprintf("request-id-%d", i),
			timestamppb.New(defaultTime),
			&callbackspb.CallbackState{},
			chasmCB,
		)
		op.Callbacks[fmt.Sprintf("cb-%d", i)] = chasm.NewComponentField(ctx, cb)
	}
}

// requireCallbacksScheduled asserts that every attached callback moved to SCHEDULED and that one
// callback invocation task was emitted per callback.
func requireCallbacksScheduled(t *testing.T, ctx *chasm.MockMutableContext, op *Operation) {
	t.Helper()
	for id, field := range op.Callbacks {
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, field.Get(ctx).Status,
			"callback %q should be scheduled", id)
	}

	require.Len(t, ctx.Tasks, len(op.Callbacks), "expected one invocation task per callback")
	for _, task := range ctx.Tasks {
		_, ok := task.Payload.(*callbackspb.InvocationTask)
		require.True(t, ok, "expected a callback InvocationTask, got %T", task.Payload)
		// The destination is derived from the callback URL (scheme://host).
		require.Equal(t, chasm.NexusCompletionHandlerURL, task.Attributes.Destination)
	}
}

// TestCompletionCallbacksScheduledOnTerminalState verifies that completion callbacks attached to an
// operation are scheduled (STANDBY -> SCHEDULED) whenever the operation reaches any terminal state.
func TestCompletionCallbacksScheduledOnTerminalState(t *testing.T) {
	timeoutFailure := &failurepb.Failure{
		Message: "timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	}

	testCases := []struct {
		name           string
		expectedStatus nexusoperationpb.OperationStatus
		apply          func(op *Operation, ctx chasm.MutableContext) error
	}{
		{
			name:           "succeeded",
			expectedStatus: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			apply: func(op *Operation, ctx chasm.MutableContext) error {
				return TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: mustToPayload(t, "result")})
			},
		},
		{
			name:           "failed",
			expectedStatus: nexusoperationpb.OPERATION_STATUS_FAILED,
			apply: func(op *Operation, ctx chasm.MutableContext) error {
				return TransitionFailed.Apply(op, ctx, EventFailed{Failure: &failurepb.Failure{Message: "boom"}})
			},
		},
		{
			name:           "canceled",
			expectedStatus: nexusoperationpb.OPERATION_STATUS_CANCELED,
			apply: func(op *Operation, ctx chasm.MutableContext) error {
				return TransitionCanceled.Apply(op, ctx, EventCanceled{Failure: &failurepb.Failure{Message: "canceled"}})
			},
		},
		{
			name:           "timed out",
			expectedStatus: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			apply: func(op *Operation, ctx chasm.MutableContext) error {
				return TransitionTimedOut.Apply(op, ctx, EventTimedOut{Failure: timeoutFailure})
			},
		},
		{
			name:           "terminated",
			expectedStatus: nexusoperationpb.OPERATION_STATUS_TERMINATED,
			apply: func(op *Operation, ctx chasm.MutableContext) error {
				return TransitionTerminated.Apply(op, ctx, EventTerminated{
					TerminateComponentRequest: chasm.TerminateComponentRequest{
						RequestID: "terminate-request-id",
						Identity:  "tester",
						Reason:    "because",
					},
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCallbackTestContext()

			op := newTestOperation()
			op.Status = nexusoperationpb.OPERATION_STATUS_STARTED
			op.StartedTime = timestamppb.New(op.ScheduledTime.AsTime().Add(time.Second))
			attachStandbyCallbacks(ctx, op, 2)

			err := tc.apply(op, ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedStatus, op.Status)
			require.True(t, op.isClosed(), "operation should be in a terminal state")

			requireCallbacksScheduled(t, ctx, op)
		})
	}
}

// TestCompletionCallbacksNoopWhenNoneAttached verifies that reaching a terminal state without any
// attached callbacks does not emit callback tasks.
func TestCompletionCallbacksNoopWhenNoneAttached(t *testing.T) {
	ctx := newCallbackTestContext()

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	err := TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: mustToPayload(t, "result")})
	require.NoError(t, err)
	require.Empty(t, ctx.Tasks)
}

// TestCompletionCallbacksScheduledOnce verifies scheduling is idempotent: callbacks already moved out of
// STANDBY are not rescheduled if scheduleCompletionCallbacks runs again.
func TestCompletionCallbacksScheduledOnce(t *testing.T) {
	ctx := newCallbackTestContext()

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_STARTED
	attachStandbyCallbacks(ctx, op, 3)

	err := TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: mustToPayload(t, "result")})
	require.NoError(t, err)
	require.Len(t, ctx.Tasks, 3)

	// A second scheduling pass must be a no-op since the callbacks are no longer STANDBY.
	require.NoError(t, op.scheduleCompletionCallbacks(ctx))
	require.Len(t, ctx.Tasks, 3, "callbacks should not be rescheduled")
}

// TestGetNexusCompletion verifies that GetNexusCompletion surfaces the operation's terminal outcome in
// the shape the callback invocation expects.
func TestGetNexusCompletion(t *testing.T) {
	successResult := mustToPayload(t, "the-result")

	t.Run("returns success result", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_STARTED
		op.StartedTime = timestamppb.New(op.ScheduledTime.AsTime().Add(time.Second))
		op.OperationToken = "op-token"

		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: successResult}))

		completion, err := op.GetNexusCompletion(ctx, "request-id")
		require.NoError(t, err)
		require.Nil(t, completion.Error)
		require.Equal(t, successResult, completion.Result)
		require.Equal(t, "op-token", completion.OperationToken)
		require.Equal(t, op.StartedTime.AsTime(), completion.StartTime)
		require.Equal(t, op.ClosedTime.AsTime(), completion.CloseTime)
		require.NotEmpty(t, completion.Links)
	})

	t.Run("returns failure error", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

		require.NoError(t, TransitionFailed.Apply(op, ctx, EventFailed{Failure: &failurepb.Failure{Message: "boom"}}))

		completion, err := op.GetNexusCompletion(ctx, "request-id")
		require.NoError(t, err)
		require.Nil(t, completion.Result)
		require.NotNil(t, completion.Error)
		require.Equal(t, nexus.OperationStateFailed, completion.Error.State)
	})

	t.Run("returns canceled error", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

		require.NoError(t, TransitionCanceled.Apply(op, ctx, EventCanceled{Failure: &failurepb.Failure{Message: "canceled"}}))

		completion, err := op.GetNexusCompletion(ctx, "request-id")
		require.NoError(t, err)
		require.NotNil(t, completion.Error)
		require.Equal(t, nexus.OperationStateCanceled, completion.Error.State)
	})

	t.Run("errors when operation is not closed", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

		_, err := op.GetNexusCompletion(ctx, "request-id")
		require.Error(t, err)
	})
}
