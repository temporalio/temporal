package callback

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestExecuteInvocationTaskNexusWorker_Outcomes exercises the full invocation flow for the NexusWorker
// callback variant: loading the invocable, resolving the endpoint, dispatching a StartOperation request, and
// saving the resulting callback state.
func TestExecuteInvocationTaskNexusWorker_Outcomes(t *testing.T) {
	const (
		endpointName  = "caller-nexus-endpoint"
		serviceName   = "temporal.nexus.v1.CompletionService"
		operationName = "OnComplete"
	)

	cases := []struct {
		name                  string
		endpointNotFound      bool
		omitDependencies      bool
		onStartOperation      func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, *Callback, error)
	}{
		{
			name: "sync-success",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if service != serviceName {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected service %q", service)
				}
				if operation != operationName {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected operation %q", operation)
				}
				if options.RequestID != "request-id" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected request ID %q", options.RequestID)
				}
				if options.Header.Get("custom-header") != "custom-value" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "missing custom header")
				}
				return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
			},
			expectedMetricOutcome: "success",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
			},
		},
		{
			name: "async-success",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			expectedMetricOutcome: "success",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
			},
		},
		{
			name: "operation-error-fails-terminally",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateFailed,
					Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}},
				}
			},
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
		{
			name: "retryable-handler-error",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name: "non-retryable-handler-error",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad request")
			},
			expectedMetricOutcome: "handler-error:BAD_REQUEST",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
		{
			name:             "endpoint-not-found",
			endpointNotFound: true,
			onStartOperation: nil, // Should not be called.
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
		{
			name:             "missing-dependencies-unprocessable",
			omitDependencies: true,
			onStartOperation: nil, // Should not be called.
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Spin up a Nexus HTTP server to receive the StartOperation request.
			var listenAddr string
			if tc.onStartOperation != nil {
				listenAddr = nexustest.AllocListenAddress()
				nexustest.NewNexusServer(t, listenAddr, nexustest.Handler{OnStartOperation: tc.onStartOperation})
			}

			// Setup namespace.
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:   "namespace-id",
					Name: "namespace-name",
				},
				Config: &persistencespb.NamespaceConfig{},
			}
			ns, err := namespace.FromPersistentState(detail, factory(detail))
			require.NoError(t, err)

			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByName: func(ctx context.Context, namespaceID namespace.ID, name string) (*persistencespb.NexusEndpointEntry, error) {
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					require.Equal(t, endpointName, name)
					return &persistencespb.NexusEndpointEntry{Id: "endpoint-id"}, nil
				},
			}

			clientProvider := func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
				return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
					BaseURL:    "http://" + listenAddr,
					Service:    service,
					Serializer: commonnexus.PayloadSerializer,
				})
			}

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
			}
			if !tc.omitDependencies {
				handler.endpointRegistry = endpointReg
				handler.clientProvider = clientProvider
			}

			chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
			require.NoError(t, chasmRegistry.Register(&Library{InvocationTaskHandler: handler}))
			require.NoError(t, chasmRegistry.Register(&mockNexusCompletionGetterLibrary{}))

			cb := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId:        "request-id",
					RegistrationTime: timestamppb.New(time.Now()),
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_NexusWorker_{
							NexusWorker: &callbackspb.Callback_NexusWorker{
								Endpoint:    endpointName,
								Service:     serviceName,
								Operation:   operationName,
								NexusHeader: map[string]string{"custom-header": "custom-value"},
								Input:       &commonpb.Payload{Data: []byte("input")},
							},
						},
					},
					Status:  callbackspb.CALLBACK_STATUS_SCHEDULED,
					Attempt: 0,
				},
			}

			executionKey := chasm.ExecutionKey{
				NamespaceID: "namespace-id",
				BusinessID:  "workflow-id",
				RunID:       "run-id",
			}
			testEngine := chasmtest.NewEngine(t, chasmRegistry)
			engineCtx := chasm.NewEngineContext(context.Background(), testEngine)
			_, err = chasm.StartExecution(
				engineCtx,
				executionKey,
				func(ctx chasm.MutableContext, _ struct{}) (*mockNexusCompletionGetterComponent, error) {
					return &mockNexusCompletionGetterComponent{
						completion: nexusrpc.CompleteOperationOptions{},
						Callback:   chasm.NewComponentField(ctx, cb),
					}, nil
				},
				struct{}{},
			)
			require.NoError(t, err)

			rootRef := chasm.NewComponentRef[*mockNexusCompletionGetterComponent](executionKey)
			callbackRef, err := chasm.ReadComponent(
				engineCtx,
				rootRef,
				func(_ *mockNexusCompletionGetterComponent, chasmCtx chasm.Context, _ struct{}) (chasm.ComponentRef, error) {
					serialized, err := chasmCtx.Ref(cb)
					if err != nil {
						return chasm.ComponentRef{}, err
					}
					return chasm.DeserializeComponentRef(serialized)
				},
				struct{}{},
			)
			require.NoError(t, err)

			executeErr := handler.Execute(
				engineCtx,
				callbackRef,
				chasm.TaskAttributes{Destination: endpointName},
				&callbackspb.InvocationTask{Attempt: 0},
			)

			resultCallback, err := chasm.ReadComponent(
				engineCtx,
				callbackRef,
				func(c *Callback, _ chasm.Context, _ struct{}) (*Callback, error) {
					return c, nil
				},
				struct{}{},
			)
			require.NoError(t, err)
			tc.assertOutcome(t, resultCallback, executeErr)

			if tc.expectedMetricOutcome != "" {
				snap := capture.Snapshot()
				counterRecordings := snap[RequestCounter.Name()]
				require.Len(t, counterRecordings, 1)
				require.Equal(t, int64(1), counterRecordings[0].Value)
				require.Equal(t, "namespace-name", counterRecordings[0].Tags["namespace"])
				require.Equal(t, endpointName, counterRecordings[0].Tags["destination"])
				require.Equal(t, tc.expectedMetricOutcome, counterRecordings[0].Tags["outcome"])

				timerRecordings := snap[RequestLatencyHistogram.Name()]
				require.Len(t, timerRecordings, 1)
				require.Equal(t, tc.expectedMetricOutcome, timerRecordings[0].Tags["outcome"])
			} else {
				// No outbound request should have been made.
				snap := capture.Snapshot()
				require.Empty(t, snap[RequestCounter.Name()])
			}
		})
	}
}
