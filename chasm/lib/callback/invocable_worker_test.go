package callback

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testWorkerTaskQueue = "completions-task-queue"
	testWorkerService   = "HTTPAdapter"
	testWorkerOperation = "DeliverAsWebhook"
	// The destination the invocation task is grouped under, mirroring what invocationDestination
	// produces for testWorkerTaskQueue.
	testWorkerDestination = "worker://completions-task-queue"
)

func newWorkerCallback(t *testing.T) *Callback {
	t.Helper()

	return &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.New(time.Now()),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Worker_{
					Worker: &callbackspb.Callback_Worker{
						TaskQueueName: testWorkerTaskQueue,
						Service:       testWorkerService,
						Operation:     testWorkerOperation,
						SourceContext: &commonpb.Payload{Data: []byte("source-context")},
					},
				},
			},
			Status:  callbackspb.CALLBACK_STATUS_SCHEDULED,
			Attempt: 0,
		},
	}
}

// startOperationResponse builds the response matching returns when a worker handled the Nexus task and
// replied with the given StartOperation response.
func startOperationResponse(start *nexuspb.StartOperationResponse) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: start},
			},
		},
	}
}

func syncSuccessResponse() *matchingservice.DispatchNexusTaskResponse {
	return startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
		},
	})
}

// handlerFailureResponse builds the response matching returns when a worker fails the Nexus task itself,
// i.e. responds with RespondNexusTaskFailed.
func handlerFailureResponse(errType string) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "handler error (" + errType + "): worker said no",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: errType,
					},
				},
			},
		},
	}
}

// TestExecuteInvocationTaskWorker_Outcomes runs the invocation task end to end against a CHASM tree with a
// mocked matching client, covering how each dispatch outcome maps onto the callback's state.
func TestExecuteInvocationTaskWorker_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		response              *matchingservice.DispatchNexusTaskResponse
		responseErr           error
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, chasm.Context, *Callback, error)
	}{
		{
			name:                  "sync-success",
			response:              syncSuccessResponse(),
			expectedMetricOutcome: "success",
			assertOutcome: func(t *testing.T, _ chasm.Context, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
			},
		},
		{
			// The handler accepted the completion and started an operation to process it. Delivery is
			// done as far as the callback is concerned; it doesn't wait for that operation.
			name: "async-success",
			response: startOperationResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
					AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "operation-token"},
				},
			}),
			expectedMetricOutcome: "success",
			assertOutcome: func(t *testing.T, _ chasm.Context, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
			},
		},
		{
			// The worker ran the completion handler and the operation failed. That verdict is
			// deterministic, so the callback fails permanently rather than retrying.
			name: "operation-failed",
			response: startOperationResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "handler rejected the completion",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
						},
					},
				},
			}),
			expectedMetricOutcome: "operation-failed",
			assertOutcome: func(t *testing.T, ctx chasm.Context, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)

				terminalFailure, ok := cb.TerminalFailure.TryGet(ctx)
				require.True(t, ok)
				require.Contains(t, terminalFailure.GetMessage(), "handler rejected the completion")
			},
		},
		{
			name:                  "retryable-handler-error",
			response:              handlerFailureResponse("INTERNAL"),
			expectedMetricOutcome: "handler-error:INTERNAL",
			assertOutcome: func(t *testing.T, ctx chasm.Context, cb *Callback, err error) {
				// The destination down error is what trips the outbound queue's circuit breaker for
				// this task queue.
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)

				require.NotNil(t, cb.LastAttemptFailure)
				_, hasTerminalFailure := cb.TerminalFailure.TryGet(ctx)
				require.False(t, hasTerminalFailure)
			},
		},
		{
			name:                  "non-retryable-handler-error",
			response:              handlerFailureResponse("BAD_REQUEST"),
			expectedMetricOutcome: "handler-error:BAD_REQUEST",
			assertOutcome: func(t *testing.T, ctx chasm.Context, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)

				terminalFailure, ok := cb.TerminalFailure.TryGet(ctx)
				require.True(t, ok)
				require.Contains(t, terminalFailure.GetMessage(), "BAD_REQUEST")
			},
		},
		{
			// Nobody is polling the task queue (or the worker died holding the task), so matching gave
			// up waiting. A worker may show up later, so keep retrying.
			name: "no-poller",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
					RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
				},
			},
			expectedMetricOutcome: "handler-error:UPSTREAM_TIMEOUT",
			assertOutcome: func(t *testing.T, _ chasm.Context, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name:                  "retryable-rpc-error",
			responseErr:           status.Error(codes.Unavailable, "matching unavailable"),
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, _ chasm.Context, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name:                  "non-retryable-rpc-error",
			responseErr:           status.Error(codes.InvalidArgument, "no such task queue"),
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, _ chasm.Context, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ns := newTestNamespace(t)

			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag(testWorkerDestination),
				metrics.OutcomeTag(tc.expectedMetricOutcome))
			metricsHandler.EXPECT().Timer(RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag(testWorkerDestination),
				metrics.OutcomeTag(tc.expectedMetricOutcome))

			matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			matchingClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).
				Return(tc.response, tc.responseErr)

			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			handler := &invocationTaskHandler{
				config: &Config{
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				namespaceRegistry: nsRegistry,
				metricsHandler:    metricsHandler,
				logger:            log.NewTestLogger(),
				matchingClient:    matchingClient,
			}

			callback := newWorkerCallback(t)
			engineCtx, callbackRef := newInvocationTaskTest(t, handler, callback, nexusrpc.CompleteOperationOptions{})

			executeErr := handler.Execute(
				engineCtx,
				callbackRef,
				chasm.TaskAttributes{Destination: testWorkerDestination},
				&callbackspb.InvocationTask{Attempt: 0},
			)

			readCallbackState(t, engineCtx, callbackRef, func(chasmCtx chasm.Context, c *Callback) {
				tc.assertOutcome(t, chasmCtx, c, executeErr)
			})
		})
	}
}

// TestExecuteInvocationTaskWorker_DispatchedRequest covers what the worker actually receives: the task is
// addressed to the callback's task queue, service, and operation, and its input carries the source
// operation's outcome.
func TestExecuteInvocationTaskWorker_DispatchedRequest(t *testing.T) {
	sourceURL, err := url.Parse("temporal:///namespaces/ns-name/operations/op-id/runs/run-id")
	require.NoError(t, err)
	sourceLink := nexus.Link{URL: sourceURL, Type: "temporal.api.common.v1.Link.NexusOperation"}

	for _, tc := range []struct {
		name       string
		completion nexusrpc.CompleteOperationOptions
		assertOn   func(*testing.T, *notificationpb.OnCompleteRequest)
	}{
		{
			name: "successful-completion",
			completion: nexusrpc.CompleteOperationOptions{
				Result: &commonpb.Payload{Data: []byte("result-data")},
				Links:  []nexus.Link{sourceLink},
			},
			assertOn: func(t *testing.T, req *notificationpb.OnCompleteRequest) {
				payloads := req.GetOutcome().GetSuccess().GetPayloads()
				require.Len(t, payloads, 1)
				require.Equal(t, []byte("result-data"), payloads[0].GetData())
				require.Nil(t, req.GetOutcome().GetFailure())
			},
		},
		{
			// A successful operation without a result still reports success, just without payloads.
			name: "successful-completion-without-a-result",
			completion: nexusrpc.CompleteOperationOptions{
				Links: []nexus.Link{sourceLink},
			},
			assertOn: func(t *testing.T, req *notificationpb.OnCompleteRequest) {
				require.NotNil(t, req.GetOutcome())
				require.Nil(t, req.GetOutcome().GetFailure())
				require.Empty(t, req.GetOutcome().GetSuccess().GetPayloads())
			},
		},
		{
			name: "failed-completion",
			completion: nexusrpc.CompleteOperationOptions{
				Error: &nexus.OperationError{
					State: nexus.OperationStateFailed,
					Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}},
				},
				Links: []nexus.Link{sourceLink},
			},
			assertOn: func(t *testing.T, req *notificationpb.OnCompleteRequest) {
				require.Nil(t, req.GetOutcome().GetSuccess())
				// The operation error is unwrapped; the handler receives the underlying cause.
				require.Equal(t, "operation failed", req.GetOutcome().GetFailure().GetMessage())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var dispatched *matchingservice.DispatchNexusTaskRequest
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			matchingClient.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *matchingservice.DispatchNexusTaskRequest, _ ...grpc.CallOption) (*matchingservice.DispatchNexusTaskResponse, error) {
					dispatched = req
					return syncSuccessResponse(), nil
				})

			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(newTestNamespace(t), nil)

			handler := &invocationTaskHandler{
				config: &Config{
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				namespaceRegistry: nsRegistry,
				metricsHandler:    metrics.NoopMetricsHandler,
				logger:            log.NewTestLogger(),
				matchingClient:    matchingClient,
			}

			callback := newWorkerCallback(t)
			engineCtx, callbackRef := newInvocationTaskTest(t, handler, callback, tc.completion)
			require.NoError(t, handler.Execute(
				engineCtx,
				callbackRef,
				chasm.TaskAttributes{Destination: testWorkerDestination},
				&callbackspb.InvocationTask{Attempt: 0},
			))

			require.NotNil(t, dispatched)
			require.Equal(t, "namespace-id", dispatched.GetNamespaceId())
			require.Equal(t, testWorkerTaskQueue, dispatched.GetTaskQueue().GetName())
			require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, dispatched.GetTaskQueue().GetKind())

			// The worker is asked to answer with Temporal failures, which is what
			// classifyDispatchResult relies on to tell a failed operation from a failed delivery.
			require.True(t, dispatched.GetRequest().GetCapabilities().GetTemporalFailureResponses())

			start := dispatched.GetRequest().GetStartOperation()
			require.Equal(t, testWorkerService, start.GetService())
			require.Equal(t, testWorkerOperation, start.GetOperation())
			// The callback's request ID doubles as the Nexus request ID, so a redelivery is idempotent
			// from the handler's perspective.
			require.Equal(t, "request-id", start.GetRequestId())
			// The source operation is identified to the handler by the completion's links.
			require.Len(t, start.GetLinks(), 1)
			require.Equal(t, sourceLink.URL.String(), start.GetLinks()[0].GetUrl())

			var onComplete notificationpb.OnCompleteRequest
			require.NoError(t, payload.Decode(start.GetPayload(), &onComplete))
			protorequire.ProtoEqual(t, &commonpb.Payload{Data: []byte("source-context")}, onComplete.GetSourceContext())
			tc.assertOn(t, &onComplete)
		})
	}
}

// A worker callback needs a matching client to go anywhere. Rather than panicking on a host that wasn't
// wired with one, the task is rejected as unprocessable.
func TestInvocableWorkerWithoutAMatchingClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handler := &invocationTaskHandler{
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}
	invocable := invocableWorker{callback: newWorkerCallback(t).GetCallback().GetWorker()}

	result := invocable.Invoke(context.Background(), newTestNamespace(t), handler, nil, chasm.TaskAttributes{})

	require.IsType(t, invocationResultFail{}, result)
	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, result.error(), &unprocessableErr)
}

// A Worker callback without a task queue has nowhere to be delivered, and no attempt will change that.
func TestInvocableWorkerWithoutATaskQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handler := &invocationTaskHandler{
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
		// Dispatch must not be attempted, so the mock is left without expectations.
		matchingClient: matchingservicemock.NewMockMatchingServiceClient(ctrl),
	}
	invocable := invocableWorker{callback: &callbackspb.Callback_Worker{Service: testWorkerService}}

	result := invocable.Invoke(context.Background(), newTestNamespace(t), handler, nil, chasm.TaskAttributes{})

	require.IsType(t, invocationResultFail{}, result)
	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, result.error(), &unprocessableErr)
	require.ErrorContains(t, result.error(), "missing a task queue name")
}

var _ resource.MatchingClient = (*matchingservicemock.MockMatchingServiceClient)(nil)
