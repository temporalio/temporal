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
	// The destination the invocation task is grouped under, mirroring what callbackDestination
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

// requireNilPayload asserts the payload is the binary/null representation of "no value", which is
// what a worker's data converter decodes into a nil result.
func requireNilPayload(t *testing.T, p *commonpb.Payload) {
	t.Helper()

	expected, err := payload.Encode(nil)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, p)
}

// requireTerminalFailure asserts the callback permanently failed, recording a non-retryable failure
// whose message contains want.
func requireTerminalFailure(t *testing.T, cb *Callback, want string) {
	t.Helper()

	require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
	require.Contains(t, cb.LastAttemptFailure.GetMessage(), want)
	require.True(t, cb.LastAttemptFailure.GetApplicationFailureInfo().GetNonRetryable())
}

// TestExecuteInvocationTaskWorker_Outcomes runs the invocation task end to end against a CHASM tree with a
// mocked matching client, covering how each dispatch outcome maps onto the callback's state.
func TestExecuteInvocationTaskWorker_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		response              *matchingservice.DispatchNexusTaskResponse
		responseErr           error
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, *Callback, error)
	}{
		{
			name:                  "sync-success",
			response:              syncSuccessResponse(),
			expectedMetricOutcome: "sync-success",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
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
			expectedMetricOutcome: "async-success",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
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
			expectedMetricOutcome: "failure",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "handler rejected the completion")
			},
		},
		{
			// Older workers report a failed operation with the deprecated OperationError variant. It
			// is just as deterministic as the Failure variant, so it must not be retried either.
			name: "deprecated-operation-error",
			response: startOperationResponse(&nexuspb.StartOperationResponse{
				//nolint:staticcheck // Deprecated, still sent by older workers.
				Variant: &nexuspb.StartOperationResponse_OperationError{
					OperationError: &nexuspb.UnsuccessfulOperationError{
						OperationState: string(nexus.OperationStateFailed),
						Failure:        &nexuspb.Failure{Message: "handler rejected the completion"},
					},
				},
			}),
			expectedMetricOutcome: "operation-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "handler rejected the completion")
			},
		},
		{
			// Older workers report a handler error with the deprecated HandlerError outcome. Its type
			// still decides whether the delivery is worth retrying.
			name: "deprecated-non-retryable-handler-error",
			response: &matchingservice.DispatchNexusTaskResponse{
				//nolint:staticcheck // Deprecated, still sent by older workers.
				Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: &nexuspb.HandlerError{
						ErrorType: "BAD_REQUEST",
						Failure:   &nexuspb.Failure{Message: "worker said no"},
					},
				},
			},
			expectedMetricOutcome: "handler-error:BAD_REQUEST",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "BAD_REQUEST")
			},
		},
		{
			// A worker can fail the task with something other than a handler error, e.g. an
			// application error. Only a handler error says whether retrying is worthwhile, so
			// anything else is taken as the worker's final answer.
			name: "non-handler-task-failure",
			response: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "worker rejected the task",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
						},
					},
				},
			},
			expectedMetricOutcome: "worker-failure",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "worker rejected the task")
			},
		},
		{
			name:                  "retryable-handler-error",
			response:              handlerFailureResponse("INTERNAL"),
			expectedMetricOutcome: "handler-error:INTERNAL",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				// The destination down error is what trips the outbound queue's circuit breaker for
				// this task queue.
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)

				// The failure is recorded, but not as a terminal one: another attempt is scheduled.
				require.Contains(t, cb.LastAttemptFailure.GetMessage(), "INTERNAL")
				require.False(t, cb.LastAttemptFailure.GetApplicationFailureInfo().GetNonRetryable())
				require.NotNil(t, cb.NextAttemptScheduleTime)
			},
		},
		{
			name:                  "non-retryable-handler-error",
			response:              handlerFailureResponse("BAD_REQUEST"),
			expectedMetricOutcome: "handler-error:BAD_REQUEST",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "BAD_REQUEST")
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
			expectedMetricOutcome: "handler-timeout",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name:                  "retryable-rpc-error",
			responseErr:           status.Error(codes.Unavailable, "matching unavailable"),
			expectedMetricOutcome: "rpc-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			// Matching rejecting the request describes the callback the caller registered, so it is
			// surfaced verbatim rather than blinded.
			name:                  "rejected-rpc-request",
			responseErr:           status.Error(codes.InvalidArgument, "malformed task queue name"),
			expectedMetricOutcome: "rpc-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				requireTerminalFailure(t, cb, "malformed task queue name")
			},
		},
		{
			// Any other RPC failure describes the state of the server, so it is hidden behind a
			// reference ID and only the shape is asserted.
			name:                  "non-retryable-rpc-error",
			responseErr:           status.Error(codes.NotFound, "namespace not found"),
			expectedMetricOutcome: "rpc-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.NotContains(t, cb.LastAttemptFailure.GetMessage(), "namespace not found")
				requireTerminalFailure(t, cb, "internal error, reference-id:")
			},
		},
		{
			// A response this server cannot interpret is not actionable and no retry would make it
			// so, so the callback fails permanently instead of retrying forever.
			name:                  "unrecognized-outcome",
			response:              &matchingservice.DispatchNexusTaskResponse{},
			expectedMetricOutcome: "unrecognized-outcome",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
				require.True(t, cb.LastAttemptFailure.GetApplicationFailureInfo().GetNonRetryable())
			},
		},
		{
			// A handler error type outside the Nexus spec is collapsed so a worker cannot introduce
			// unbounded metric cardinality.
			name:                  "handler-error-with-an-unknown-type",
			response:              handlerFailureResponse("SOMETHING_MADE_UP"),
			expectedMetricOutcome: "handler-error:UNKNOWN",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				// An unrecognized handler error type is retryable per the Nexus spec, so the delivery
				// is retried. The type is collapsed only in the metric tag; the recorded failure keeps
				// what the worker actually said.
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
				require.Contains(t, cb.LastAttemptFailure.GetMessage(), "SOMETHING_MADE_UP")
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

			readCallbackState(t, engineCtx, callbackRef, func(_ chasm.Context, c *Callback) {
				t.Helper()
				tc.assertOutcome(t, c, executeErr)
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
				require.Equal(t, []byte("result-data"), req.GetSuccess().GetData())
				require.Nil(t, req.GetFailure())
			},
		},
		{
			// A successful operation without a result still reports success, carrying the
			// binary/null representation of "no value".
			name: "successful-completion-without-a-result",
			completion: nexusrpc.CompleteOperationOptions{
				Links: []nexus.Link{sourceLink},
			},
			assertOn: func(t *testing.T, req *notificationpb.OnCompleteRequest) {
				require.IsType(t, &notificationpb.OnCompleteRequest_Success{}, req.GetResult())
				requireNilPayload(t, req.GetSuccess())
			},
		},
		{
			// Completion sources report an absent result as a nil *commonpb.Payload rather than an
			// untyped nil, which must be treated the same as no result at all.
			name: "successful-completion-with-a-typed-nil-result",
			completion: nexusrpc.CompleteOperationOptions{
				Result: (*commonpb.Payload)(nil),
				Links:  []nexus.Link{sourceLink},
			},
			assertOn: func(t *testing.T, req *notificationpb.OnCompleteRequest) {
				require.IsType(t, &notificationpb.OnCompleteRequest_Success{}, req.GetResult())
				requireNilPayload(t, req.GetSuccess())
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
				require.Nil(t, req.GetSuccess())
				// The operation error is unwrapped; the handler receives the underlying cause.
				require.Equal(t, "operation failed", req.GetFailure().GetMessage())
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
			dispatchStart := time.Now()
			require.NoError(t, handler.Execute(
				engineCtx,
				callbackRef,
				chasm.TaskAttributes{Destination: testWorkerDestination},
				&callbackspb.InvocationTask{Attempt: 0},
			))

			require.NotNil(t, dispatched)
			require.Equal(t, "namespace-id", dispatched.GetNamespaceId())
			// The worker's poller measures task latencies against the scheduled time, so it has to
			// reflect when this delivery attempt started.
			require.WithinDuration(t, dispatchStart, dispatched.GetRequest().GetScheduledTime().AsTime(), time.Minute)
			require.Equal(t, testWorkerTaskQueue, dispatched.GetTaskQueue().GetName())
			require.Equal(t, enumspb.TASK_QUEUE_KIND_NORMAL, dispatched.GetTaskQueue().GetKind())

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

// A Worker callback that cannot be dispatched at all fails permanently, since no further attempt
// would change the outcome.
func TestInvocableWorkerCannotDispatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		callback    *callbackspb.Callback_Worker
		completion  nexusrpc.CompleteOperationOptions
		wantMessage string
	}{
		{
			name:        "without a task queue",
			callback:    &callbackspb.Callback_Worker{Service: testWorkerService},
			wantMessage: "missing a task queue name",
		},
		{
			name:        "with a result that isn't a payload",
			callback:    &callbackspb.Callback_Worker{TaskQueueName: testWorkerTaskQueue, Service: testWorkerService},
			completion:  nexusrpc.CompleteOperationOptions{Result: "not-a-payload"},
			wantMessage: "invalid result, expected a payload",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			handler := &invocationTaskHandler{
				config:         &Config{},
				metricsHandler: metrics.NoopMetricsHandler,
				logger:         log.NewTestLogger(),
				// Dispatch must not be attempted, so the mock is left without expectations.
				matchingClient: matchingservicemock.NewMockMatchingServiceClient(ctrl),
			}
			invocable := invocableWorker{callback: tc.callback, completion: tc.completion}

			result := invocable.Invoke(context.Background(), newTestNamespace(t), handler, nil, chasm.TaskAttributes{})

			require.IsType(t, invocationResultFail{}, result)
			require.ErrorContains(t, result.error(), tc.wantMessage)
		})
	}
}
