package callback

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

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
	"go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockNexusCompletionGetterComponent struct {
	chasm.UnimplementedComponent

	Empty *emptypb.Empty

	completion nexusrpc.CompleteOperationOptions
	err        error

	Callback chasm.Field[*Callback]
}

func (m *mockNexusCompletionGetterComponent) GetNexusCompletion(_ chasm.Context, requestID string) (nexusrpc.CompleteOperationOptions, error) {
	return m.completion, m.err
}

func (m *mockNexusCompletionGetterComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

type mockNexusCompletionGetterLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *mockNexusCompletionGetterLibrary) Name() string {
	return "mock"
}

func (l *mockNexusCompletionGetterLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*mockNexusCompletionGetterComponent]("nexusCompletionGetter"),
	}
}

// Test the full executeInvocationTask flow with direct executor calls
func TestExecuteInvocationTaskNexus_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		caller                HTTPCaller
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, *Callback, error)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "success",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
			},
		},
		{
			name: "network-error-retry",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name: "retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				var destDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destDownErr)
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
			},
		},
		{
			name: "non-retryable-http-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			expectedMetricOutcome: "handler-error:BAD_REQUEST",
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

			// Setup namespace
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

			// Setup logger and time source
			logger := log.NewTestLogger()
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(time.Now())

			// Create task executor with mock namespace registry
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			// Create mock engine
			mockEngine := chasm.NewMockEngine(ctrl)
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
				httpCallerProvider: func(nid common.NamespaceIDAndDestination) HTTPCaller {
					return tc.caller
				},
			}

			chasmRegistry := chasm.NewRegistry(logger)
			err = chasmRegistry.Register(&Library{
				InvocationTaskExecutor: executor,
			})
			require.NoError(t, err)
			err = chasmRegistry.Register(&mockNexusCompletionGetterLibrary{})
			require.NoError(t, err)

			nodeBackend := &chasm.MockNodeBackend{}
			root := chasm.NewEmptyTree(chasmRegistry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metricsHandler)

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

			// Create completion
			completion := nexusrpc.CompleteOperationOptions{}

			// Set up the CompletionSource field to return our mock completion
			require.NoError(t, root.SetRootComponent(&mockNexusCompletionGetterComponent{
				completion: completion,
				// Create callback in SCHEDULED state
				Callback: chasm.NewComponentField(
					chasm.NewMutableContext(context.Background(), root),
					callback,
				),
			}))
			_, err = root.CloseTransaction()
			require.NoError(t, err)

			// Setup engine expectations to directly call executor logic with MockMutableContext
			mockEngine.EXPECT().ReadComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, readFn func(chasm.Context, chasm.Component, *chasm.Registry) error, opts ...chasm.TransitionOption) error {
				mockCtx := &chasm.MockContext{
					HandleNow: func(component chasm.Component) time.Time {
						return timeSource.Now()
					},
					HandleRef: func(component chasm.Component) ([]byte, error) {
						return []byte{}, nil
					},
				}
				return readFn(mockCtx, callback, chasmRegistry)
			})

			mockEngine.EXPECT().UpdateComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component, *chasm.Registry) error, opts ...chasm.TransitionOption) ([]any, error) {
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
				err := updateFn(mockCtx, callback, chasmRegistry)
				return nil, err
			})

			// Create ComponentRef
			ref := chasm.NewComponentRef[*Callback](chasm.ExecutionKey{
				NamespaceID: "namespace-id",
				BusinessID:  "workflow-id",
				RunID:       "run-id",
			})

			// Execute with engine context
			engineCtx := chasm.NewEngineContext(context.Background(), mockEngine)
			err = executor.Invoke(
				engineCtx,
				ref,
				chasm.TaskAttributes{Destination: "http://localhost"},
				&callbackspb.InvocationTask{Attempt: 0},
			)

			// Verify the outcome and tasks
			tc.assertOutcome(t, callback, err)
		})
	}
}

// TestProcessBackoffTask tests the backoff task execution that transitions
// a callback from BACKING_OFF to SCHEDULED state and adds an invocation task.
func TestProcessBackoffTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := log.NewTestLogger()
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
		RunId:       "run-id",
		ArchetypeId: 1234,
	}

	serializedRef, err := dummyRef.Marshal()
	require.NoError(t, err)
	encodedRef := base64.RawURLEncoding.EncodeToString(serializedRef)
	dummyTime := time.Now().UTC()

	createPayloadBytes := func(data []byte) *commonpb.Payload {
		return &commonpb.Payload{Data: data}
	}

	cases := []struct {
		name               string
		setupHistoryClient func(*testing.T, *gomock.Controller) resource.HistoryClient
		completion         nexusrpc.CompleteOperationOptions
		headerValue        string
		assertOutcome      func(*testing.T, *Callback, error)
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
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Result:    createPayloadBytes([]byte("result-data")),
					CloseTime: dummyTime,
				}
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
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
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Error: &nexus.OperationError{
						State: nexus.OperationStateFailed,
						Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}},
					},
					CloseTime: dummyTime,
				}
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.NoError(t, err)
				require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, cb.Status)
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
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Result: createPayloadBytes([]byte("result-data")),
				}
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.ErrorContains(t, err, "internal error, reference-id:")
				require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.Status)
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
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Result: createPayloadBytes([]byte("result-data")),
				}
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.ErrorContains(t, err, "internal error, reference-id:")
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
		{
			name: "invalid-base64-header",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				// No RPC call expected
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Result: createPayloadBytes([]byte("result-data")),
				}
			}(),
			headerValue: "invalid-base64!!!",
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.ErrorContains(t, err, "internal error, reference-id:")
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
		{
			name: "invalid-protobuf-in-ref",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) resource.HistoryClient {
				// No RPC call expected
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			completion: func() nexusrpc.CompleteOperationOptions {
				return nexusrpc.CompleteOperationOptions{
					Result: createPayloadBytes([]byte("result-data")),
				}
			}(),
			headerValue: base64.RawURLEncoding.EncodeToString([]byte("not-valid-protobuf")),
			assertOutcome: func(t *testing.T, cb *Callback, err error) {
				require.ErrorContains(t, err, "internal error, reference-id:")
				require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, cb.Status)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup namespace
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

			// Setup history client
			historyClient := tc.setupHistoryClient(t, ctrl)

			// Setup logger, metricsHandler, and time source
			logger := log.NewTestLogger()
			metricsHandler := metrics.NoopMetricsHandler
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(time.Now())

			// Create mock namespace registry
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

			// Create mock engine and setup expectations
			mockEngine := chasm.NewMockEngine(ctrl)
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
				historyClient:     historyClient,
			}

			chasmRegistry := chasm.NewRegistry(logger)
			err = chasmRegistry.Register(&Library{
				InvocationTaskExecutor: executor,
			})
			require.NoError(t, err)
			err = chasmRegistry.Register(&mockNexusCompletionGetterLibrary{})
			require.NoError(t, err)

			nodeBackend := &chasm.MockNodeBackend{}
			root := chasm.NewEmptyTree(chasmRegistry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metricsHandler)

			// Create headers
			headers := nexus.Header{}
			if tc.headerValue != "" {
				headers.Set(commonnexus.CallbackTokenHeader, tc.headerValue)
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

			// Set up the CompletionSource field to return our mock completion
			require.NoError(t, root.SetRootComponent(&mockNexusCompletionGetterComponent{
				completion: tc.completion,
				// Create callback in SCHEDULED state
				Callback: chasm.NewComponentField(
					chasm.NewMutableContext(context.Background(), root),
					callback,
				),
			}))
			_, err = root.CloseTransaction()
			require.NoError(t, err)

			mockEngine.EXPECT().ReadComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, readFn func(chasm.Context, chasm.Component, *chasm.Registry) error, opts ...chasm.TransitionOption) error {
				// Create a mock context
				mockCtx := &chasm.MockContext{
					HandleNow: func(component chasm.Component) time.Time {
						return timeSource.Now()
					},
					HandleRef: func(component chasm.Component) ([]byte, error) {
						return []byte{}, nil
					},
					HandleExecutionKey: func() chasm.ExecutionKey {
						return chasm.ExecutionKey{
							NamespaceID: "namespace-id",
							BusinessID:  "workflow-id",
							RunID:       "run-id",
						}
					},
				}

				// Call the readFn with our callback
				return readFn(mockCtx, callback, chasmRegistry)
			})

			mockEngine.EXPECT().UpdateComponent(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component, *chasm.Registry) error, opts ...chasm.TransitionOption) ([]any, error) {
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

				// Call the updateFn with our callback
				err := updateFn(mockCtx, callback, chasmRegistry)
				return nil, err
			})

			// Create ComponentRef
			ref := chasm.NewComponentRef[*Callback](chasm.ExecutionKey{
				NamespaceID: "namespace-id",
				BusinessID:  "workflow-id",
				RunID:       "run-id",
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

			tc.assertOutcome(t, callback, err)
		})
	}
}
