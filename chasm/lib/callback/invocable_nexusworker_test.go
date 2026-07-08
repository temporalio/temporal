package callback

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testNamespaceID   = "namespace-id"
	testNamespaceName = "namespace-name"
	testTaskQueueName = "caller-task-queue"
	testRequestID     = "request-id"
	testDestination   = "caller-task-queue"
)

func newTestNamespace(t *testing.T) *namespace.Namespace {
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   testNamespaceID,
			Name: testNamespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
	}
	ns, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	return ns
}

const (
	testResultValue        = "op-result"
	testSourceContextValue = "some-context"
)

// newNexusWorkerInvocable builds an invocableNexusWorker with a task queue name, a successful completion
// result, and a source context set.
func newNexusWorkerInvocable() invocableNexusWorker {
	return invocableNexusWorker{
		callback: &callbackspb.Callback_NexusWorker{
			TaskqueueName: testTaskQueueName,
			SourceContext: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{payload.EncodeString(testSourceContextValue)},
			},
		},
		completion: nexusrpc.CompleteOperationOptions{
			Result: payload.EncodeString(testResultValue),
		},
		requestID: testRequestID,
	}
}

func newTestHandler(matchingClient resource.MatchingClient) *invocationTaskHandler {
	return &invocationTaskHandler{
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
		matchingClient: matchingClient,
	}
}

// syncSuccessResponse builds a DispatchNexusTaskResponse indicating the worker synchronously succeeded.
func syncSuccessResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
						},
					},
				},
			},
		},
	}
}

// TestInvokeNexusWorker_DispatchesToTaskQueue verifies that a successful invocation dispatches a StartOperation
// Nexus task to the callback's task queue via MatchingService.DispatchNexusTask, with the expected request shape.
func TestInvokeNexusWorker_DispatchesToTaskQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	matchingClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *matchingservice.DispatchNexusTaskRequest, opts ...grpc.CallOption) (*matchingservice.DispatchNexusTaskResponse, error) {
			// Routed to the correct namespace and task queue.
			require.Equal(t, testNamespaceID, req.GetNamespaceId())
			require.Equal(t, testTaskQueueName, req.GetTaskQueue().GetName())
			require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, req.GetTaskQueue().GetKind())

			// Dispatched as a StartOperation targeting the well-known completion service/operation.
			start := req.GetRequest().GetStartOperation()
			require.NotNil(t, start, "expected a StartOperation request variant")
			require.Equal(t, CompletionServiceName, start.GetService())
			require.Equal(t, CompletionOperationName, start.GetOperation())
			// The request ID is carried through for idempotency.
			require.Equal(t, testRequestID, start.GetRequestId())

			// The payload is an OnCompleteHandlerInput carrying the operation outcome and source context.
			require.NotNil(t, start.GetPayload(), "expected an input payload")
			var input nexuspb.OnCompleteHandlerInput
			require.NoError(t, payload.Decode(start.GetPayload(), &input))

			var gotResult string
			require.NotNil(t, input.GetOutcome().GetSuccess(), "expected a successful outcome")
			require.NoError(t, payload.Decode(input.GetOutcome().GetSuccess(), &gotResult))
			require.Equal(t, testResultValue, gotResult)

			var gotContext string
			require.NoError(t, payload.Decode(input.GetSourceContext(), &gotContext))
			require.Equal(t, testSourceContextValue, gotContext)

			return syncSuccessResponse(), nil
		})

	n := newNexusWorkerInvocable()
	h := newTestHandler(matchingClient)

	result := n.Invoke(context.Background(), newTestNamespace(t), h, &callbackspb.InvocationTask{}, chasm.TaskAttributes{Destination: testDestination})
	require.IsType(t, invocationResultOK{}, result)
	require.NoError(t, result.error())
}

// TestInvokeNexusWorker_Outcomes exercises the mapping from DispatchNexusTask results to invocationResults.
func TestInvokeNexusWorker_Outcomes(t *testing.T) {
	cases := []struct {
		name       string
		response   *matchingservice.DispatchNexusTaskResponse
		rpcErr     error
		expectType invocationResult
	}{
		{
			name:       "sync-success",
			response:   syncSuccessResponse(),
			expectType: invocationResultOK{},
		},
		{
			name: "async-success",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{
						Variant: &nexuspb.Response_StartOperation{
							StartOperation: &nexuspb.StartOperationResponse{
								Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
									AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "token"},
								},
							},
						},
					},
				},
			},
			expectType: invocationResultOK{},
		},
		{
			name: "request-timeout-is-retryable",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
					RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
				},
			},
			expectType: invocationResultRetry{},
		},
		{
			name: "worker-failure-is-retryable",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "worker blew up",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
						},
					},
				},
			},
			expectType: invocationResultRetry{},
		},
		{
			name:       "rpc-error-retryable",
			rpcErr:     status.Error(codes.Unavailable, "matching unavailable"),
			expectType: invocationResultRetry{},
		},
		{
			name:       "rpc-error-non-retryable",
			rpcErr:     status.Error(codes.InvalidArgument, "bad request"),
			expectType: invocationResultFail{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			matchingClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).Return(tc.response, tc.rpcErr)

			n := newNexusWorkerInvocable()
			h := newTestHandler(matchingClient)

			result := n.Invoke(context.Background(), newTestNamespace(t), h, &callbackspb.InvocationTask{}, chasm.TaskAttributes{Destination: testDestination})
			require.IsType(t, tc.expectType, result)
		})
	}
}

// TestInvokeNexusWorker_Unprocessable verifies inputs that cannot ever succeed fail permanently without an RPC.
func TestInvokeNexusWorker_Unprocessable(t *testing.T) {
	t.Run("missing-matching-client", func(t *testing.T) {
		n := newNexusWorkerInvocable()
		h := newTestHandler(nil)

		result := n.Invoke(context.Background(), newTestNamespace(t), h, &callbackspb.InvocationTask{}, chasm.TaskAttributes{Destination: testDestination})
		require.IsType(t, invocationResultFail{}, result)
		require.Error(t, result.error())
	})

	t.Run("missing-task-queue-name", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// No DispatchNexusTask call is expected.
		matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)

		n := newNexusWorkerInvocable()
		n.callback.TaskqueueName = ""
		h := newTestHandler(matchingClient)

		result := n.Invoke(context.Background(), newTestNamespace(t), h, &callbackspb.InvocationTask{}, chasm.TaskAttributes{Destination: testDestination})
		require.IsType(t, invocationResultFail{}, result)
		require.Error(t, result.error())
	})
}

// TestBuildCompletionInput verifies the OnCompleteHandlerInput is built from both successful and failed
// completions, and that the source context is carried through.
func TestBuildCompletionInput(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		n := newNexusWorkerInvocable()

		input, err := n.buildCompletionInput()
		require.NoError(t, err)

		require.NotNil(t, input.GetOutcome().GetSuccess(), "expected a successful outcome")
		var gotResult string
		require.NoError(t, payload.Decode(input.GetOutcome().GetSuccess(), &gotResult))
		require.Equal(t, testResultValue, gotResult)
		require.Nil(t, input.GetOutcome().GetFailure())

		var gotContext string
		require.NoError(t, payload.Decode(input.GetSourceContext(), &gotContext))
		require.Equal(t, testSourceContextValue, gotContext)
	})

	t.Run("failure", func(t *testing.T) {
		n := newNexusWorkerInvocable()
		n.completion = nexusrpc.CompleteOperationOptions{
			Error: &nexus.OperationError{
				State: nexus.OperationStateFailed,
				Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}},
			},
		}

		input, err := n.buildCompletionInput()
		require.NoError(t, err)

		require.Nil(t, input.GetOutcome().GetSuccess())
		require.NotNil(t, input.GetOutcome().GetFailure(), "expected a failure outcome")
		require.Equal(t, "operation failed", input.GetOutcome().GetFailure().GetMessage())
	})

	t.Run("nil-source-context", func(t *testing.T) {
		n := newNexusWorkerInvocable()
		n.callback.SourceContext = nil

		input, err := n.buildCompletionInput()
		require.NoError(t, err)
		require.Nil(t, input.GetSourceContext())
	})
}
