package callback

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// setMSPointerBackend is a test helper that uses reflection to set the unexported backend field of chasm.MSPointer.
// This allows us to inject a mock NodeBackend that implements GetNexusCompletion.
func setMSPointerBackend(msPtr *chasm.MSPointer, backend chasm.NodeBackend) {
	// Get the unexported 'backend' field using unsafe pointer manipulation
	backendField := reflect.ValueOf(msPtr).Elem().FieldByName("backend")
	backendField = reflect.NewAt(backendField.Type(), unsafe.Pointer(backendField.UnsafeAddr())).Elem()
	backendField.Set(reflect.ValueOf(backend))
}

// Test the full executeInvocationTask flow with direct executor calls
func TestExecuteInvocationTaskNexus_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		caller                HTTPCaller
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, *Callback, *chasm.MockMutableContext)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:200",
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
				// Success state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
		{
			name: "network-error-retry",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
				// Should generate a BackoffTask
				require.Len(t, mctx.Tasks, 1)
				require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)
				backoffTask := mctx.Tasks[0].Payload.(*callbackspb.BackoffTask)
				require.Equal(t, cb.Attempt, backoffTask.Attempt)
			},
		},
		{
			name: "retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:500",
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
				// Should generate a BackoffTask
				require.Len(t, mctx.Tasks, 1)
				require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)
				backoffTask := mctx.Tasks[0].Payload.(*callbackspb.BackoffTask)
				require.Equal(t, cb.Attempt, backoffTask.Attempt)
			},
		},
		{
			name: "non-retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "status:400",
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
				// Failed state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup namespace
			ns := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:   "namespace-id",
					Name: "namespace-name",
				},
				Config: &persistencespb.NamespaceConfig{},
			})

			// Setup metrics expectations
			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))
			metricsHandler.EXPECT().Timer(RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))

			// Create completion
			completion, err := nexusrpc.NewOperationCompletionSuccessful(nil, nexusrpc.OperationCompletionSuccessfulOptions{})
			require.NoError(t, err)

			// Setup logger and time source
			logger := log.NewNoopLogger()
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(time.Now())

			// Create callback in SCHEDULED state
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId:        "request-id",
					RegistrationTime: timestamppb.New(timeSource.Now()),
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus_{
							Nexus: &callbackspb.Callback_Nexus{
								Url: "http://localhost",
							},
						},
					},
					Status:  callbackspb.CALLBACK_STATUS_SCHEDULED,
					Attempt: 0,
				},
			}

			// Set up the MSPointer field with a mock backend that returns our completion
			// We use reflection to set the unexported backend field for testing
			mockBackend := &chasm.MockNodeBackend{
				HandleGetNexusCompletion: func(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error) {
					return completion, nil
				},
			}
			setMSPointerBackend(&callback.MSPointer, mockBackend)

			// Create task executor with mock namespace registry
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			// Create MockMutableContext to capture tasks
			var capturedMockCtx *chasm.MockMutableContext

			// Create mock engine
			mockEngine := chasm.NewMockEngine(ctrl)

			// Setup engine expectations to directly call executor logic with MockMutableContext
			mockEngine.EXPECT().ReadComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, opts ...chasm.TransitionOption) error {
				mockCtx := &chasm.MockContext{
					HandleNow: func(component chasm.Component) time.Time {
						return timeSource.Now()
					},
					HandleRef: func(component chasm.Component) ([]byte, error) {
						return []byte{}, nil
					},
				}
				return readFn(mockCtx, callback)
			})

			mockEngine.EXPECT().UpdateComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]any, error) {
				mockCtx := &chasm.MockMutableContext{
					MockContext: chasm.MockContext{
						HandleNow: func(component chasm.Component) time.Time {
							return timeSource.Now()
						},
						HandleRef: func(component chasm.Component) ([]byte, error) {
							return []byte{}, nil
						},
					},
				}
				capturedMockCtx = mockCtx
				err := updateFn(mockCtx, callback)
				return nil, err
			})

			executor := &InvocationTaskExecutor{
				config: &Config{
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				namespaceRegistry: nsRegistry,
				metricsHandler:    metricsHandler,
				logger:            logger,
				httpCallerProvider: func(nid queues.NamespaceIDAndDestination) HTTPCaller {
					return tc.caller
				},
				chasmEngine: mockEngine,
			}

			// Create ComponentRef
			ref := chasm.NewComponentRef[*Callback](chasm.EntityKey{
				NamespaceID: "namespace-id",
				BusinessID:  "workflow-id",
				EntityID:    "run-id",
			})

			// Execute with engine context
			engineCtx := chasm.NewEngineContext(context.Background(), mockEngine)
			err = executor.Invoke(
				engineCtx,
				ref,
				chasm.TaskAttributes{Destination: "http://localhost"},
				&callbackspb.InvocationTask{Attempt: 0},
			)

			// For successful and non-retryable errors, expect no error from Invoke
			// For retryable errors, expect an error to be returned
			expectedError := tc.name == "network-error-retry" || tc.name == "retryable-http-error"
			if expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Verify the outcome and tasks
			tc.assertOutcome(t, callback, capturedMockCtx)
		})
	}
}

// TestProcessBackoffTask tests the backoff task execution that transitions
// a callback from BACKING_OFF to SCHEDULED state and adds an invocation task.
func TestProcessBackoffTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := log.NewNoopLogger()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	// Create callback in BACKING_OFF state
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId: "request-id",
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://localhost",
					},
				},
			},
			Status:                  callbackspb.CALLBACK_STATUS_BACKING_OFF,
			Attempt:                 1,
			NextAttemptScheduleTime: timestamppb.New(timeSource.Now().Add(time.Minute)),
		},
	}

	// Create mock mutable context
	mockCtx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(component chasm.Component) time.Time {
				return timeSource.Now()
			},
			HandleRef: func(component chasm.Component) ([]byte, error) {
				return []byte{}, nil
			},
		},
	}

	executor := BackoffTaskExecutor{
		config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		logger: logger,
	}

	// Execute the backoff task
	task := &callbackspb.BackoffTask{Attempt: 1}
	attrs := chasm.TaskAttributes{Destination: "http://localhost"}
	err := executor.Execute(mockCtx, callback, attrs, task)

	// Verify no error
	require.NoError(t, err)

	// Verify callback transitioned to SCHEDULED state
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status)
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Verify an invocation task was added
	require.Len(t, mockCtx.Tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mockCtx.Tasks[0].Payload)
	invTask := mockCtx.Tasks[0].Payload.(*callbackspb.InvocationTask)
	require.Equal(t, int32(1), invTask.Attempt)
}

func TestExecuteInvocationTaskChasm_Outcomes(t *testing.T) {
	dummyRef := persistencespb.ChasmComponentRef{
		NamespaceId: "namespace-id",
		BusinessId:  "business-id",
		EntityId:    "entity-id",
		Archetype:   "test-archetype",
	}

	serializedRef, err := dummyRef.Marshal()
	require.NoError(t, err)
	encodedRef := base64.RawURLEncoding.EncodeToString(serializedRef)
	dummyTime := time.Now().UTC()

	createPayloadBytes := func(data []byte) []byte {
		p := &commonpb.Payload{Data: data}
		payloadBytes, err := proto.Marshal(p)
		require.NoError(t, err)
		return payloadBytes
	}

	cases := []struct {
		name                 string
		setupHistoryClient   func(*testing.T, *gomock.Controller) resource.HistoryClient
		completion           nexusrpc.OperationCompletion
		headerValue          string
		expectsInternalError bool
		assertOutcome        func(*testing.T, *Callback, *chasm.MockMutableContext)
	}{
		{
			name: "success-with-successful-operation",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, req *historyservice.CompleteNexusOperationChasmRequest, opts ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					// Verify completion token
					require.NotNil(t, req.Completion)
					require.NotNil(t, req.Completion.ComponentRef)
					require.Equal(t, "request-id", req.Completion.RequestId)

					// Verify successful operation data
					require.NotNil(t, req.GetSuccess())
					require.Equal(t, []byte("result-data"), req.GetSuccess().Data)
					require.Equal(t, req.CloseTime.AsTime(), dummyTime)

					return &historyservice.CompleteNexusOperationChasmResponse{}, nil
				})
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayloadBytes([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{
						CloseTime: dummyTime,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
				// Success state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
		{
			name: "success-with-failed-operation",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, req *historyservice.CompleteNexusOperationChasmRequest, opts ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					require.NotNil(t, req.Completion)
					require.NotNil(t, req.GetFailure())
					require.Equal(t, req.CloseTime.AsTime(), dummyTime)

					return &historyservice.CompleteNexusOperationChasmResponse{}, nil
				})
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionUnsuccessful(
					&nexus.OperationError{
						State: nexus.OperationStateFailed,
						Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}},
					},
					nexusrpc.OperationCompletionUnsuccessfulOptions{
						CloseTime: dummyTime,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
				// Success state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
		{
			name: "retryable-rpc-error",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).Return(nil, status.Error(codes.Unavailable, "service unavailable"))
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayloadBytes([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          encodedRef,
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
				// Should generate a BackoffTask
				require.Len(t, mctx.Tasks, 1)
				require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)
			},
		},
		{
			name: "non-retryable-rpc-error",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).Return(nil, status.Error(codes.InvalidArgument, "invalid request"))
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayloadBytes([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          encodedRef,
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
				// Failed state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
		{
			name: "invalid-base64-header",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				// No RPC call expected
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayloadBytes([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          "invalid-base64!!!",
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
				// Failed state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
		{
			name: "invalid-protobuf-in-ref",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				// No RPC call expected
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayloadBytes([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          base64.RawURLEncoding.EncodeToString([]byte("not-valid-protobuf")),
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb *Callback, mctx *chasm.MockMutableContext) {
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
				// Failed state is terminal - no tasks should be generated
				require.Empty(t, mctx.Tasks)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup namespace
			ns := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id:   "namespace-id",
					Name: "namespace-name",
				},
				Config: &persistencespb.NamespaceConfig{},
			})

			// Setup history client
			historyClient := tc.setupHistoryClient(t, ctrl)

			// Setup logger and time source
			logger := log.NewNoopLogger()
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(time.Now())

			// Create headers
			headers := make(map[string]string)
			if tc.headerValue != "" {
				headers[commonnexus.CallbackTokenHeader] = tc.headerValue
			}

			// Create callback with chasm internal URL
			callback := &Callback{
				CallbackState: &callbackspb.CallbackState{
					RequestId:        "request-id",
					RegistrationTime: timestamppb.New(timeSource.Now()),
					Callback: &callbackspb.Callback{
						Variant: &callbackspb.Callback_Nexus_{
							Nexus: &callbackspb.Callback_Nexus{
								Url:    chasm.NexusCompletionHandlerURL,
								Header: headers,
							},
						},
					},
					Status:  callbackspb.CALLBACK_STATUS_SCHEDULED,
					Attempt: 1,
				},
			}

			// Set up the MSPointer field with a mock backend that returns our completion
			// We use reflection to set the unexported backend field for testing
			mockBackend := &chasm.MockNodeBackend{
				HandleGetNexusCompletion: func(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error) {
					return tc.completion, nil
				},
			}
			setMSPointerBackend(&callback.MSPointer, mockBackend)

			// Create mock namespace registry
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			// Create MockMutableContext to capture tasks
			var capturedMockCtx *chasm.MockMutableContext

			// Create mock engine and setup expectations
			mockEngine := chasm.NewMockEngine(ctrl)
			mockEngine.EXPECT().ReadComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, opts ...chasm.TransitionOption) error {
				// Create a mock context
				mockCtx := &chasm.MockContext{
					HandleNow: func(component chasm.Component) time.Time {
						return timeSource.Now()
					},
					HandleRef: func(component chasm.Component) ([]byte, error) {
						return []byte{}, nil
					},
					HandleExecutionKey: func() chasm.EntityKey {
						return chasm.EntityKey{
							NamespaceID: "namespace-id",
							BusinessID:  "workflow-id",
							EntityID:    "run-id",
						}
					},
				}

				// Call the readFn with our callback
				return readFn(mockCtx, callback)
			})

			mockEngine.EXPECT().UpdateComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, opts ...chasm.TransitionOption) ([]any, error) {
				// Create a mock mutable context
				mockCtx := &chasm.MockMutableContext{
					MockContext: chasm.MockContext{
						HandleNow: func(component chasm.Component) time.Time {
							return timeSource.Now()
						},
						HandleRef: func(component chasm.Component) ([]byte, error) {
							return []byte{}, nil
						},
					},
				}
				capturedMockCtx = mockCtx

				// Call the updateFn with our callback
				err := updateFn(mockCtx, callback)
				return nil, err
			})

			executor := InvocationTaskExecutor{
				config: &Config{
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				namespaceRegistry: nsRegistry,
				metricsHandler:    metrics.NoopMetricsHandler,
				logger:            logger,
				historyClient:     historyClient,
				chasmEngine:       mockEngine,
			}

			// Create ComponentRef
			ref := chasm.NewComponentRef[*Callback](chasm.EntityKey{
				NamespaceID: "namespace-id",
				BusinessID:  "workflow-id",
				EntityID:    "run-id",
			})

			// Create context with engine
			ctx := chasm.NewEngineContext(context.Background(), mockEngine)

			// Execute the invocation task
			task := &callbackspb.InvocationTask{Attempt: 1}
			err = executor.Invoke(
				ctx,
				ref,
				chasm.TaskAttributes{},
				task,
			)

			if tc.expectsInternalError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "internal error, reference-id:")
			} else {
				require.NoError(t, err)
			}

			tc.assertOutcome(t, callback, capturedMockCtx)
		})
	}
}
