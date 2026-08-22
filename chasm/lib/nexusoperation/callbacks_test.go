package nexusoperation

import (
	"context"
	"testing"
	"time"

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

func newCallbackTestContext() *chasm.MockMutableContext {
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
				MetricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
			}),
		},
	}
}

func TestNewStandaloneOperationAttachesCompletionCallbacks(t *testing.T) {
	t.Parallel()

	newStartReq := func(cbs ...*commonpb.Callback) *nexusoperationpb.StartNexusOperationRequest {
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

		req := newStartReq(newNexusCallback())
		op, err := newStandaloneOperation(ctx, req, 10, newTestLinkValidator(10, 10))
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)

		// Callbacks start in STANDBY, only transitioning to SCHEDULED when the SANO completes.
		require.Len(t, op.Callbacks, 1)
		attachedCB := op.Callbacks["req-id-0"].Get(ctx)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, attachedCB.Status)
	})

	t.Run("WithoutCallbacks", func(t *testing.T) {
		ctx := newCallbackTestContext()

		op, err := newStandaloneOperation(ctx, newStartReq(), 10, newTestLinkValidator(10, 10))
		require.NoError(t, err)
		require.Nil(t, op.Callbacks)
	})

	t.Run("EnforcesTheCallersLimit", func(t *testing.T) {
		ctx := newCallbackTestContext()

		_, err := newStandaloneOperation(ctx, newStartReq(
			newNexusCallback(),
			newNexusCallback(),
		), 1, newTestLinkValidator(10, 10))
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.ErrorContains(t, err, "cannot attach more than 1 callbacks")
	})
}

func TestAddCompletionCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("AttachesCallbacksInStandby", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		cb1 := newNexusCallback()
		cb1.GetNexus().Url = "https://example.com/callback-1"

		// Set data on the second CB, we confirm is added to the Operation.
		cb2 := newNexusCallback()
		cb2.GetNexus().Url = "https://example.com/callback-2"
		cb2.GetNexus().Header = map[string]string{
			"key": "xxx",
		}
		cb2.Links = []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns-name", WorkflowId: "wf-id"},
		}}}

		cbs := []*commonpb.Callback{
			cb1,
			cb2,
		}

		err := op.addCompletionCallbacks(ctx, "req-id", cbs, 10)
		require.NoError(t, err)
		require.Len(t, op.Callbacks, 2)

		first, ok := op.Callbacks["req-id-0"]
		require.True(t, ok)
		firstCb := first.Get(ctx)
		require.Equal(t, "https://example.com/callback-1", firstCb.GetCallback().GetNexus().GetUrl())

		second, ok := op.Callbacks["req-id-1"]
		require.True(t, ok)
		secondCb := second.Get(ctx)
		require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, secondCb.Status)
		require.Equal(t, defaultTime, secondCb.RegistrationTime.AsTime())
		require.Equal(t, "https://example.com/callback-2", secondCb.GetCallback().GetNexus().GetUrl())
		require.Equal(t, map[string]string{"key": "xxx"}, secondCb.GetCallback().GetNexus().GetHeader())
		require.Len(t, secondCb.GetCallback().GetLinks(), 1)

		// Each callback gets its own, unique request ID.
		require.NotEqual(t, "req-id", firstCb.RequestId)
		require.NotEqual(t, "req-id", secondCb.RequestId)
		require.NotEqual(t, firstCb.RequestId, secondCb.RequestId)

		// STANDBY means no invocation task yet; only the scheduled transition's tasks are present.
		for _, task := range ctx.Tasks {
			_, isInvocation := task.Payload.(*callbackspb.InvocationTask)
			require.False(t, isInvocation, "callbacks must not be invoked while in STANDBY")
		}
	})

	t.Run("EmptyListIsNoOp", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", nil, 10))
		require.Nil(t, op.Callbacks)
	})

	t.Run("ReAttachingTheSameRequestIsIdempotent", func(t *testing.T) {
		// A retried start (or a retried on_conflict_options attach) must not duplicate callbacks.
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{newNexusCallback()}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.Len(t, op.Callbacks, 1)
	})

	t.Run("ReAttachingTheSameRequestIsIdempotentAfterClose", func(t *testing.T) {
		// A start request can reach addCompletionCallbacks twice: once creating the operation, then
		// again if the client retries and the engine dedups on request ID. The operation may have closed
		// in between, and the retry must still report success for callbacks that are already persisted
		// rather than FailedPrecondition.
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{newNexusCallback()}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, op.Callbacks["req-id-0"].Get(ctx).Status)

		tasksBefore := len(ctx.Tasks)
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", cbs, 10))

		// The retry must leave the already-scheduled callback alone: re-attaching would reset it to
		// STANDBY, stranding a callback the terminal transition had already released for delivery.
		require.Len(t, op.Callbacks, 1)
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, op.Callbacks["req-id-0"].Get(ctx).Status)
		require.Len(t, ctx.Tasks, tasksBefore)
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{newNexusCallback()}

		require.NoError(t, op.addCompletionCallbacks(ctx, "req-1", cbs, 10))
		require.NoError(t, op.addCompletionCallbacks(ctx, "req-2", cbs, 10))
		require.Len(t, op.Callbacks, 2)
	})

	t.Run("RejectsExceedingTheLimit", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)
		cbs := []*commonpb.Callback{newNexusCallback(), newNexusCallback()}

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
			newNexusCallback(),
		}, 2))

		err := op.addCompletionCallbacks(ctx, "req-2", []*commonpb.Callback{
			newNexusCallback(),
			newNexusCallback(),
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
			newNexusCallback(),
		}, 10)
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
		require.Contains(t, err.Error(), "cannot attach callbacks to a closed nexus operation")
		require.Empty(t, op.Callbacks)
	})

	t.Run("RejectsAnEmptyRequestID", func(t *testing.T) {
		// Callback IDs are derived from the request ID, so without one two distinct requests would
		// produce colliding keys and silently overwrite each other. The frontend always supplies one
		// (validator.normalizeRequestID); this guards a history-side caller that did not.
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		err := op.addCompletionCallbacks(ctx, "", []*commonpb.Callback{
			newNexusCallback(),
		}, 10)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "without a request ID")
		require.Empty(t, op.Callbacks)
	})
}

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

	// In all scenarios, we expect the CHASM callbacks to be scheduled once the
	// SANO transitions to a terminal state.
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
				newNexusCallback(),
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

			// The task's Destination attribute for Nexus callbacks is the hostname, which
			// is fixed in newNexusCallback.
			const wantHost = "https://nexus.ex.xxxxx.cluster.tmprl.cloud:7243"
			require.Equal(t, wantHost, newTasks[0].Attributes.Destination)
		})
	}

	t.Run("NoCallbacksIsANoOp", func(t *testing.T) {
		ctx := newCallbackTestContext()
		op := newScheduledTestOperation(t, ctx)

		tasksBefore := len(ctx.Tasks)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))
		require.Len(t, ctx.Tasks, tasksBefore)
	})
}

// TestTerminateRejectedForClosedOperation guards the source-state list of TransitionTerminated,
// ensuring you cannot terminate an already terminal SANO.
func TestTerminateRejectedForClosedOperation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		close           func(*testing.T, *Operation, *chasm.MockMutableContext)
		expectedStatus  nexusoperationpb.OperationStatus
		expectedResult  *commonpb.Payload
		expectedFailure *failurepb.Failure
	}{
		{
			name: "Canceled",
			close: func(t *testing.T, o *Operation, ctx *chasm.MockMutableContext) {
				require.NoError(t, TransitionCanceled.Apply(o, ctx, EventCanceled{
					Failure: &failurepb.Failure{Message: "canceled by handler"},
				}))
			},
			expectedStatus:  nexusoperationpb.OPERATION_STATUS_CANCELED,
			expectedFailure: &failurepb.Failure{Message: "canceled by handler"},
		},
		{
			name: "Failed",
			close: func(t *testing.T, o *Operation, ctx *chasm.MockMutableContext) {
				require.NoError(t, TransitionFailed.Apply(o, ctx, EventFailed{
					Failure: &failurepb.Failure{Message: "boom"},
				}))
			},
			expectedStatus:  nexusoperationpb.OPERATION_STATUS_FAILED,
			expectedFailure: &failurepb.Failure{Message: "boom"},
		},
		{
			name: "TimedOut",
			close: func(t *testing.T, o *Operation, ctx *chasm.MockMutableContext) {
				require.NoError(t, TransitionTimedOut.Apply(o, ctx, EventTimedOut{
					Failure: &failurepb.Failure{
						Message: "timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
							},
						},
					},
				}))
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			expectedFailure: &failurepb.Failure{
				Message: "timed out",
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
					TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
						TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
					},
				},
			},
		},
		{
			name: "Succeeded",
			close: func(t *testing.T, o *Operation, ctx *chasm.MockMutableContext) {
				require.NoError(t, TransitionSucceeded.Apply(o, ctx, EventSucceeded{
					Result: mustToPayload(t, "result"),
				}))
			},
			expectedStatus: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			expectedResult: mustToPayload(t, "result"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCallbackTestContext()
			op := newScheduledTestOperation(t, ctx)

			op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{})
			op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibilityWithData(ctx, nil, nil))
			require.NoError(t, op.addCompletionCallbacks(ctx, "req-id", []*commonpb.Callback{
				newNexusCallback(),
			}, 10))

			tc.close(t, op, ctx)
			tasksAfterClose := len(ctx.Tasks)

			_, err := op.Terminate(ctx, chasm.TerminateComponentRequest{
				RequestID: "terminate-req-id",
				Reason:    "because",
				Identity:  "test-identity",
			})
			require.ErrorIs(t, err, chasm.ErrInvalidTransition)
			var failedPreconditionErr *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPreconditionErr)
			require.Contains(t, err.Error(), "from "+tc.expectedStatus.String())

			// The closed state, its outcome, and its already-scheduled callback must all survive.
			require.Equal(t, tc.expectedStatus, op.Status)
			require.Nil(t, op.TerminateState)
			require.Len(t, ctx.Tasks, tasksAfterClose)

			resp, err := op.buildDescribeResponse(ctx, &nexusoperationpb.DescribeNexusOperationRequest{
				FrontendRequest: &workflowservice.DescribeNexusOperationExecutionRequest{IncludeOutcome: true},
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.expectedFailure, resp.GetFrontendResponse().GetFailure())
			protorequire.ProtoEqual(t, tc.expectedResult, resp.GetFrontendResponse().GetResult())
		})
	}
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
		failed.Attempt = 3
		failed.LastAttemptFailure = &failurepb.Failure{Message: "boom"}

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
		require.Nil(t, infos[0].GetInfo().GetResult())
		require.Equal(t, defaultTime, infos[0].GetInfo().GetRegistrationTime().AsTime())
		// Every callback on a standalone operation is triggered by the operation completing.
		require.NotNil(t, infos[0].GetTrigger().GetOperationCompleted())

		require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, infos[1].GetInfo().GetState())
		require.NotNil(t, infos[1].GetInfo().GetSuccess())

		require.Equal(t, enumspb.CALLBACK_STATE_FAILED, infos[2].GetInfo().GetState())
		require.Equal(t, int32(3), infos[2].GetInfo().GetAttempt())
		protorequire.ProtoEqual(t,
			&failurepb.Failure{Message: "boom"},
			infos[2].GetInfo().GetFailure())
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
			metricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
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
		newNexusCallback(),
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
			newNexusCallback(),
		}, 10))

		resp, err := op.buildDescribeResponse(ctx, req)
		require.NoError(t, err)

		cbs := resp.GetFrontendResponse().GetCompletionCallbacks()
		require.Len(t, cbs, 1)
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
