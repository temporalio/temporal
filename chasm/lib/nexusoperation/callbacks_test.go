package nexusoperation

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testCallbackURL = "http://localhost:8080/cb"

// newCallbackTestContext builds a context with everything the terminal transitions need: a clock, a
// namespace entry and metrics tag config (both used when emitting close metrics), plus the callback
// limit that maxCallbacksFromContext reads.
func newCallbackTestContext() *chasm.MockMutableContext {
	return newCallbackTestContextWithMax(10)
}

func newCallbackTestContextWithMax(maxCallbacks int) *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "ns-id", BusinessID: "op-id", RunID: "run-id"}
			},
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
			},
			HandleExecutionInfo: func() chasm.ExecutionInfo {
				return chasm.ExecutionInfo{CloseTime: defaultTime}
			},
			GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
				MetricTagConfig:          dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
				MaxCallbacksPerExecution: func(string) int { return maxCallbacks },
			}),
		},
	}
}

func TestMaxCallbacksFromContext(t *testing.T) {
	t.Parallel()

	t.Run("ReadsThePerNamespaceLimit", func(t *testing.T) {
		require.Equal(t, 7, maxCallbacksFromContext(newCallbackTestContextWithMax(7)))
	})

	t.Run("FailsClosedWithoutAnOperationContext", func(t *testing.T) {
		// A missing OperationContext can only be a library registration bug. Returning 0 rejects every
		// callback rather than silently accepting an unbounded number.
		require.Equal(t, 0, maxCallbacksFromContext(&chasm.MockMutableContext{}))
	})
}

// TestNewStandaloneOperationAttachesCompletionCallbacks covers the start path end to end: the request's
// callbacks land on the component, and the limit is resolved from the component context values rather
// than being threaded in by the caller.
func TestNewStandaloneOperationAttachesCompletionCallbacks(t *testing.T) {
	t.Parallel()

	newReq := func(cbs ...*commonpb.Callback) *nexusoperationpb.StartNexusOperationRequest {
		return &nexusoperationpb.StartNexusOperationRequest{
			EndpointId: "endpoint-id",
			FrontendRequest: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace:           "ns-name",
				OperationId:         "op-id",
				RequestId:           "req-id",
				Endpoint:            "test-endpoint",
				Service:             "test-service",
				Operation:           "test-operation",
				CompletionCallbacks: cbs,
			},
		}
	}

	t.Run("WithCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()

		op, err := newStandaloneOperation(ctx, newReq(testNexusCallback(testCallbackURL)))
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)

		require.Len(t, op.Callbacks, 1)
		cb := op.Callbacks["req-id-0"].Get(ctx)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.Status)
		require.Equal(t, testCallbackURL, cb.GetCallback().GetNexus().GetUrl())
	})

	t.Run("WithoutCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()

		op, err := newStandaloneOperation(ctx, newReq())
		require.NoError(t, err)
		require.Nil(t, op.Callbacks)
	})

	t.Run("PropagatesTheLimitFromTheContext", func(t *testing.T) {
		ctx := newCallbackTestContextWithMax(1)

		_, err := newStandaloneOperation(ctx, newReq(
			testNexusCallback(testCallbackURL),
			testNexusCallback("http://localhost:8080/cb2"),
		))
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Contains(t, err.Error(), "cannot attach more than 1 callbacks")
	})
}

func TestAddCompletionCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("AttachesCallbacksInStandby", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		cbs := []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:    testCallbackURL,
						Header: map[string]string{"key": "value"},
					},
				},
				Links: []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns-name", WorkflowId: "wf-id"},
				}}},
			},
			testNexusCallback("http://localhost:8080/cb2"),
		}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.Len(t, op.Callbacks, 2)

		// Keyed by request ID plus position within the request.
		first, ok := op.Callbacks["req-id-0"]
		require.True(t, ok)
		cb := first.Get(ctx)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.Status)
		require.Equal(t, "req-id", cb.RequestId)
		require.Equal(t, defaultTime, cb.RegistrationTime.AsTime())
		require.Equal(t, testCallbackURL, cb.GetCallback().GetNexus().GetUrl())
		require.Equal(t, map[string]string{"key": "value"}, cb.GetCallback().GetNexus().GetHeader())
		require.Len(t, cb.GetCallback().GetLinks(), 1)

		second, ok := op.Callbacks["req-id-1"]
		require.True(t, ok)
		require.Equal(t, "http://localhost:8080/cb2", second.Get(ctx).GetCallback().GetNexus().GetUrl())

		// STANDBY means no invocation task yet; only the scheduled transition's tasks are present.
		for _, task := range ctx.Tasks {
			_, isInvocation := task.Payload.(*callbackspb.InvocationTask)
			require.False(t, isInvocation, "callbacks must not be invoked while in STANDBY")
		}
	})

	t.Run("EmptyListIsANoOp", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", nil, 10))
		require.Nil(t, op.Callbacks)
	})

	t.Run("ReAttachingTheSameRequestIsIdempotent", func(t *testing.T) {
		// A retried start (or a retried on_conflict_options attach) must not duplicate callbacks.
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{testNexusCallback(testCallbackURL)}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.Len(t, op.Callbacks, 1)
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{testNexusCallback(testCallbackURL)}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-1", cbs, 10))
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-2", cbs, 10))
		require.Len(t, op.Callbacks, 2)
	})

	t.Run("RejectsExceedingTheLimit", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{testNexusCallback(testCallbackURL), testNexusCallback(testCallbackURL)}

		err := op.addCompletionCallbacks(ctx, "req-id", cbs, 1)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Contains(t, err.Error(), "cannot attach more than 1 callbacks")
		require.Empty(t, op.Callbacks)
	})

	t.Run("RejectsExceedingTheLimitWithAlreadyAttachedCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-1", []*commonpb.Callback{
			testNexusCallback(testCallbackURL),
		}, 2))

		err := op.addCompletionCallbacks(ctx, "req-2", []*commonpb.Callback{
			testNexusCallback(testCallbackURL),
			testNexusCallback(testCallbackURL),
		}, 2)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Contains(t, err.Error(), "1 callbacks already attached")
		require.Len(t, op.Callbacks, 1)
	})

	t.Run("RejectsAClosedOperation", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))

		err := op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
			testNexusCallback(testCallbackURL),
		}, 10)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Contains(t, err.Error(), "cannot attach callbacks to a closed nexus operation")
		require.Empty(t, op.Callbacks)
	})

	t.Run("RejectsAnUnsupportedVariant", func(t *testing.T) {
		// The frontend rejects these first; this is the belt-and-braces check in the component, since the
		// CHASM callback component has no representation for a non-Nexus variant.
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		err := op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{{
			Variant: &commonpb.Callback_Worker_{Worker: &commonpb.Callback_Worker{}},
		}}, 10)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "unsupported callback variant")
	})
}

// TestScheduleCompletionCallbacksOnTerminalTransition proves that every way an operation can close
// releases its STANDBY callbacks for delivery.
func TestScheduleCompletionCallbacksOnTerminalTransition(t *testing.T) {
	t.Parallel()

	timeoutFailure := &failurepb.Failure{
		Message: "timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	}

	for _, tc := range []struct {
		name           string
		fromStatus     nexusoperationpb.OperationStatus
		apply          func(*Operation, *chasm.MockMutableContext) error
		expectedStatus nexusoperationpb.OperationStatus
	}{
		{
			name: "Succeeded",
			apply: func(o *Operation, ctx *chasm.MockMutableContext) error {
				return TransitionSucceeded.Apply(o, ctx, EventSucceeded{})
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
		},
		{
			name: "Failed",
			apply: func(o *Operation, ctx *chasm.MockMutableContext) error {
				return TransitionFailed.Apply(o, ctx, EventFailed{
					Failure: &failurepb.Failure{Message: "boom"},
				})
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_FAILED,
		},
		{
			name: "Canceled",
			apply: func(o *Operation, ctx *chasm.MockMutableContext) error {
				return TransitionCanceled.Apply(o, ctx, EventCanceled{
					Failure: &failurepb.Failure{Message: "canceled"},
				})
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_CANCELED,
		},
		{
			name: "TimedOut",
			apply: func(o *Operation, ctx *chasm.MockMutableContext) error {
				return TransitionTimedOut.Apply(o, ctx, EventTimedOut{Failure: timeoutFailure})
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
		},
		{
			name: "Terminated",
			apply: func(o *Operation, ctx *chasm.MockMutableContext) error {
				_, err := o.Terminate(ctx, chasm.TerminateComponentRequest{
					RequestID: "terminate-req-id",
					Reason:    "because",
				})
				return err
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_TERMINATED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCallbackTestContext()
			op := newScheduledTestOperation(t, ctx)
			require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
				testNexusCallback(testCallbackURL),
			}, 10))

			tasksBefore := len(ctx.Tasks)
			require.NoError(t, tc.apply(op, ctx))
			require.Equal(t, tc.expectedStatus, op.Status)

			cb := op.Callbacks["req-id-0"].Get(ctx)
			require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, cb.Status)

			// Closing must emit exactly one callback invocation task, routed to the callback's host.
			newTasks := ctx.Tasks[tasksBefore:]
			require.Len(t, newTasks, 1)
			require.IsType(t, &callbackspb.InvocationTask{}, newTasks[0].Payload)
			require.Equal(t, "http://localhost:8080", newTasks[0].Attributes.Destination)
		})
	}

	t.Run("AlreadyScheduledCallbacksAreNotRescheduled", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
			testNexusCallback(testCallbackURL),
		}, 10))
		op.Callbacks["req-id-0"].Get(ctx).SetStateMachineState(callbackspb.CALLBACK_STATUS_SUCCEEDED)

		tasksBefore := len(ctx.Tasks)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))
		require.Len(t, ctx.Tasks, tasksBefore)
	})

	t.Run("NoCallbacksIsANoOp", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		tasksBefore := len(ctx.Tasks)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))
		require.Len(t, ctx.Tasks, tasksBefore)
	})
}

func TestBuildCompletionCallbackInfos(t *testing.T) {
	t.Parallel()

	t.Run("NoCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()

		infos, err := op.buildCompletionCallbackInfos(ctx)
		require.NoError(t, err)
		require.Nil(t, infos)
	})

	t.Run("ReportsStateAndOutcomePerCallback", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newTestOperation()

		newCB := func(url string, status callbackspb.CallbackStatus) *callback.Callback {
			cb := callback.NewCallback(
				"req-id",
				timestamppb.New(defaultTime),
				&callbackspb.CallbackState{},
				&callbackspb.Callback{
					Variant: &callbackspb.Callback_Nexus_{
						Nexus: &callbackspb.Callback_Nexus{Url: url},
					},
				},
			)
			cb.SetStateMachineState(status)
			return cb
		}

		failed := newCB("http://localhost:8080/failed", callbackspb.CALLBACK_STATUS_FAILED)
		failed.TerminalFailure = chasm.NewDataField(ctx, &failurepb.Failure{Message: "boom"})
		failed.Attempt = 3

		op.Callbacks = chasm.Map[string, *callback.Callback]{
			"req-id-0": chasm.NewComponentField(ctx, newCB("http://localhost:8080/standby", callbackspb.CALLBACK_STATUS_STANDBY)),
			"req-id-1": chasm.NewComponentField(ctx, newCB("http://localhost:8080/succeeded", callbackspb.CALLBACK_STATUS_SUCCEEDED)),
			"req-id-2": chasm.NewComponentField(ctx, failed),
		}

		infos, err := op.buildCompletionCallbackInfos(ctx)
		require.NoError(t, err)
		require.Len(t, infos, 3)

		// Ordering follows the sorted callback IDs, not the (randomized) map iteration order.
		require.Equal(t, "http://localhost:8080/standby", infos[0].GetInfo().GetCallback().GetNexus().GetUrl())
		require.Equal(t, enumspb.CALLBACK_STATE_STANDBY, infos[0].GetInfo().GetState())
		require.Nil(t, infos[0].GetInfo().GetOutcome())
		require.Equal(t, defaultTime, infos[0].GetInfo().GetRegistrationTime().AsTime())
		// Every callback on a standalone operation is triggered by the operation completing.
		require.NotNil(t, infos[0].GetTrigger().GetOperationCompleted())

		require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, infos[1].GetInfo().GetState())
		require.NotNil(t, infos[1].GetInfo().GetOutcome().GetSuccess())

		require.Equal(t, enumspb.CALLBACK_STATE_FAILED, infos[2].GetInfo().GetState())
		require.Equal(t, int32(3), infos[2].GetInfo().GetAttempt())
		protorequire.ProtoEqual(t,
			&failurepb.Failure{Message: "boom"},
			infos[2].GetInfo().GetOutcome().GetFailure(),
		)
	})

	t.Run("ResponseProtosAreIsolatedFromTheComponent", func(t *testing.T) {
		// The describe response is marshalled after the CHASM read lease is released, so it must not
		// alias protos owned by the live component.
		ctx := newCallbackTestContext()
		op := newTestOperation()
		registrationTime := timestamppb.New(defaultTime)
		op.Callbacks = chasm.Map[string, *callback.Callback]{
			"req-id-0": chasm.NewComponentField(ctx, callback.NewCallback(
				"req-id",
				registrationTime,
				&callbackspb.CallbackState{},
				&callbackspb.Callback{
					Variant: &callbackspb.Callback_Nexus_{
						Nexus: &callbackspb.Callback_Nexus{
							Url:    testCallbackURL,
							Header: map[string]string{"key": "value"},
						},
					},
				},
			)),
		}

		infos, err := op.buildCompletionCallbackInfos(ctx)
		require.NoError(t, err)
		require.Len(t, infos, 1)

		infos[0].GetInfo().GetRegistrationTime().Seconds = 0
		infos[0].GetInfo().GetCallback().GetNexus().GetHeader()["injected"] = "x"

		cb := op.Callbacks["req-id-0"].Get(ctx)
		require.Equal(t, defaultTime, cb.RegistrationTime.AsTime())
		require.NotContains(t, cb.GetCallback().GetNexus().GetHeader(), "injected")
	})
}

// TestCompletionCallbacksRoundTripThroughTheTree exercises the Callbacks map against a real CHASM tree
// rather than a mock context, so that a missing component registration or an unserializable field shows
// up here instead of at runtime.
func TestCompletionCallbacksRoundTripThroughTheTree(t *testing.T) {
	logger := log.NewNoopLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{
		componentOnlyLibrary: componentOnlyLibrary{
			metricTagConfig:          dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
			maxCallbacksPerExecution: func(string) int { return 10 },
		},
	}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(defaultTime)
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
		},
		HandleGetNamespaceEntry: func() *namespace.Namespace {
			return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
		},
	}
	root := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), root)

	op := NewOperation(&nexusoperationpb.OperationState{
		Status:        nexusoperationpb.OPERATION_STATUS_STARTED,
		Endpoint:      "test-endpoint",
		ScheduledTime: timestamppb.New(defaultTime),
	})
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{})
	op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(ctx, nil, nil))
	require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
		testNexusCallback(testCallbackURL),
	}, 10))
	require.NoError(t, root.SetRootComponent(op))
	_, err := root.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), root)
	require.Len(t, op.Callbacks, 1)
	cb := op.Callbacks["req-id-0"].Get(ctx)
	require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, cb.Status)

	// The callback resolves its parent via a ParentPtr, so the Operation must satisfy
	// callback.CompletionSource from inside the tree, not just as a compile-time assertion.
	require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: mustToPayload(t, "result")}))
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, cb.Status)

	completion, err := cb.CompletionSource.Get(ctx).GetNexusCompletion(ctx, cb.RequestId)
	require.NoError(t, err)
	require.Nil(t, completion.Error)
}

// TestDescribeResponseIncludesCompletionCallbacks covers the plumbing from the component into the
// DescribeNexusOperationExecution response.
func TestDescribeResponseIncludesCompletionCallbacks(t *testing.T) {
	t.Parallel()

	newOp := func(ctx chasm.MutableContext) *Operation {
		op := newTestOperation()
		op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{})
		op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(ctx, nil, nil))
		return op
	}
	req := &nexusoperationpb.DescribeNexusOperationRequest{
		FrontendRequest: &workflowservice.DescribeNexusOperationExecutionRequest{},
	}

	t.Run("WithCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newOp(ctx)
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
			testNexusCallback(testCallbackURL),
		}, 10))

		resp, err := op.buildDescribeResponse(ctx, req)
		require.NoError(t, err)

		cbs := resp.GetFrontendResponse().GetCompletionCallbacks()
		require.Len(t, cbs, 1)
		require.Equal(t, testCallbackURL, cbs[0].GetInfo().GetCallback().GetNexus().GetUrl())
		require.Equal(t, enumspb.CALLBACK_STATE_STANDBY, cbs[0].GetInfo().GetState())
	})

	t.Run("WithoutCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newOp(ctx)

		resp, err := op.buildDescribeResponse(ctx, req)
		require.NoError(t, err)
		require.Empty(t, resp.GetFrontendResponse().GetCompletionCallbacks())
	})
}

func TestOperationGetNexusCompletion(t *testing.T) {
	t.Parallel()

	t.Run("RejectsAnOpenOperation", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		_, err := op.GetNexusCompletion(ctx, "req-id")
		var internalErr *serviceerror.Internal
		require.ErrorAs(t, err, &internalErr)
		require.Contains(t, err.Error(), "nexus operation has not completed yet")
	})

	t.Run("Succeeded", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		result := mustToPayload(t, "result")
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: result}))

		completion, err := op.GetNexusCompletion(ctx, "req-id")
		require.NoError(t, err)
		require.Nil(t, completion.Error)
		gotResult, ok := completion.Result.(*commonpb.Payload)
		require.True(t, ok)
		protorequire.ProtoEqual(t, result, gotResult)
		require.Equal(t, op.GetScheduledTime().AsTime(), completion.StartTime)
		require.Equal(t, defaultTime, completion.CloseTime)
		// A back-link to this operation lets the callback receiver navigate to the caller.
		require.Len(t, completion.Links, 1)
		require.Contains(t, completion.Links[0].URL.String(), "op-id")
	})

	t.Run("Failed", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionFailed.Apply(op, ctx, EventFailed{
			Failure: &failurepb.Failure{Message: "boom"},
		}))

		completion, err := op.GetNexusCompletion(ctx, "req-id")
		require.NoError(t, err)
		require.Nil(t, completion.Result)
		var opErr *nexus.OperationError
		require.ErrorAs(t, completion.Error, &opErr)
		require.Equal(t, nexus.OperationStateFailed, opErr.State)
	})

	t.Run("Canceled", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionCanceled.Apply(op, ctx, EventCanceled{
			Failure: &failurepb.Failure{Message: "canceled"},
		}))

		completion, err := op.GetNexusCompletion(ctx, "req-id")
		require.NoError(t, err)
		var opErr *nexus.OperationError
		require.ErrorAs(t, completion.Error, &opErr)
		require.Equal(t, nexus.OperationStateCanceled, opErr.State)
	})

	t.Run("Terminated", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		_, err := op.Terminate(ctx, chasm.TerminateComponentRequest{RequestID: "t", Reason: "because"})
		require.NoError(t, err)

		completion, err := op.GetNexusCompletion(ctx, "req-id")
		require.NoError(t, err)
		var opErr *nexus.OperationError
		require.ErrorAs(t, completion.Error, &opErr)
		require.Equal(t, nexus.OperationStateFailed, opErr.State)
	})
}
