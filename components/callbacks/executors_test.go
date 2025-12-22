package callbacks_test

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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeEnv struct {
	node *hsm.Node
}

func (s fakeEnv) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
	return accessor(s.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now()
}

var _ hsm.Environment = fakeEnv{}

type mutableState struct {
	completionNexus nexusrpc.OperationCompletion
	completionHsm   *persistencespb.HSMCompletionCallbackArg
}

func (ms mutableState) GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error) {
	return ms.completionNexus, nil
}

func (ms mutableState) GetHSMCompletionCallbackArg(ctx context.Context) (*persistencespb.HSMCompletionCallbackArg, error) {
	return ms.completionHsm, nil
}

func TestProcessInvocationTaskNexus_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		caller                callbacks.HTTPCaller
		retryable             bool
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, callbacks.Callback)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			retryable:             false,
			expectedMetricOutcome: "status:200",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, cb.State())
			},
		},
		{
			name: "failed",
			caller: func(r *http.Request) (*http.Response, error) {
				return nil, errors.New("fake failure")
			},
			retryable:             true,
			expectedMetricOutcome: "unknown-error",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name: "retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			},
			retryable:             true,
			expectedMetricOutcome: "status:500",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name: "non-retryable-error",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Body: http.NoBody}, nil
			},
			retryable:             false,
			expectedMetricOutcome: "status:400",
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			namespaceRegistryMock := namespace.NewMockRegistry(ctrl)
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
			namespaceRegistryMock.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(ns, nil)
			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(callbacks.RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))
			metricsHandler.EXPECT().Timer(callbacks.RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag("http://localhost"),
				metrics.OutcomeTag(tc.expectedMetricOutcome))

			root := newRoot(t)
			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{
								Url: "http://localhost",
							},
						},
					},
					State: enumsspb.CALLBACK_STATE_SCHEDULED,
				},
			}
			coll := callbacks.MachineCollection(root)
			node, err := coll.Add("ID", cb)
			require.NoError(t, err)
			env := fakeEnv{node}

			key := definition.NewWorkflowKey("namespace-id", "", "")
			reg := hsm.NewRegistry()
			require.NoError(t, callbacks.RegisterExecutor(
				reg,
				callbacks.TaskExecutorOptions{
					NamespaceRegistry: namespaceRegistryMock,
					MetricsHandler:    metricsHandler,
					HTTPCallerProvider: func(nid queuescommon.NamespaceIDAndDestination) callbacks.HTTPCaller {
						return tc.caller
					},
					Logger: log.NewNoopLogger(),
					Config: &callbacks.Config{
						RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
						RetryPolicy: func() backoff.RetryPolicy {
							return backoff.NewExponentialRetryPolicy(time.Second)
						},
					},
				},
			))

			err = reg.ExecuteImmediateTask(
				context.Background(),
				env,
				hsm.Ref{
					WorkflowKey: key,
					StateMachineRef: &persistencespb.StateMachineRef{
						Path: []*persistencespb.StateMachineKey{
							{
								Type: callbacks.StateMachineType,
								Id:   "ID",
							},
						},
					},
				},
				callbacks.NewInvocationTask("http://localhost"),
			)

			if tc.retryable {
				var target *queueserrors.UnprocessableTaskError
				require.NotErrorAs(t, err, &target)
			} else {
				require.NoError(t, err)
			}

			cb, err = coll.Data("ID")
			require.NoError(t, err)
			tc.assertOutcome(t, cb)
		})
	}
}

func TestProcessBackoffTask(t *testing.T) {
	root := newRoot(t)
	cb := callbacks.Callback{
		CallbackInfo: &persistencespb.CallbackInfo{
			Callback: &persistencespb.Callback{
				Variant: &persistencespb.Callback_Nexus_{
					Nexus: &persistencespb.Callback_Nexus{
						Url: "http://localhost",
					},
				},
			},
			State: enumsspb.CALLBACK_STATE_BACKING_OFF,
		},
	}
	coll := callbacks.MachineCollection(root)
	node, err := coll.Add("ID", cb)
	require.NoError(t, err)
	env := fakeEnv{node}

	reg := hsm.NewRegistry()
	require.NoError(t, callbacks.RegisterExecutor(
		reg,
		callbacks.TaskExecutorOptions{
			HTTPCallerProvider: func(nid queuescommon.NamespaceIDAndDestination) callbacks.HTTPCaller {
				return nil
			},
			Logger: log.NewNoopLogger(),
			Config: &callbacks.Config{
				RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
				RetryPolicy: func() backoff.RetryPolicy {
					return backoff.NewExponentialRetryPolicy(time.Second)
				},
			},
		},
	))

	err = reg.ExecuteTimerTask(
		env,
		node,
		callbacks.BackoffTask{},
	)
	require.NoError(t, err)

	cb, err = coll.Data("ID")
	require.NoError(t, err)
	require.Equal(t, enumsspb.CALLBACK_STATE_SCHEDULED, cb.State())
}

func newMutableState(t *testing.T) mutableState {
	completionNexus, err := nexusrpc.NewOperationCompletionSuccessful(nil, nexusrpc.OperationCompletionSuccessfulOptions{})
	require.NoError(t, err)
	hsmCallbackArg := &persistencespb.HSMCompletionCallbackArg{
		NamespaceId: "mynsid",
		WorkflowId:  "mywid",
		RunId:       "myrid",
		LastEvent: &historypb.HistoryEvent{
			EventId:   42,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	return mutableState{
		completionNexus: completionNexus,
		completionHsm:   hsmCallbackArg,
	}
}

func newRoot(t *testing.T) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	mutableState := newMutableState(t)

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, mutableState, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	return root
}

func TestProcessInvocationTaskChasm_Outcomes(t *testing.T) {
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

	createPayload := func(data []byte) *commonpb.Payload {
		return &commonpb.Payload{Data: data}
	}

	cases := []struct {
		name                 string
		setupHistoryClient   func(*testing.T, *gomock.Controller) *historyservicemock.MockHistoryServiceClient
		completion           nexusrpc.OperationCompletion
		headerValue          string
		expectsInternalError bool
		assertOutcome        func(*testing.T, callbacks.Callback)
	}{
		{
			name: "success-with-successful-operation",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, req *historyservice.CompleteNexusOperationChasmRequest, opts ...grpc.CallOption) (*historyservice.CompleteNexusOperationChasmResponse, error) {
					// Verify completion token
					require.NotNil(t, req.Completion)
					require.NotNil(t, req.Completion.ComponentRef)
					var ref persistencespb.ChasmComponentRef
					require.NoError(t, proto.Unmarshal(req.Completion.ComponentRef, &ref))
					require.Equal(t, "namespace-id", ref.NamespaceId)
					require.Equal(t, "business-id", ref.BusinessId)
					require.Equal(t, "run-id", ref.RunId)
					require.Equal(t, dummyRef.ArchetypeId, ref.ArchetypeId)
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
					createPayload([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{
						Serializer: commonnexus.PayloadSerializer,
						CloseTime:  dummyTime,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue: encodedRef,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, cb.State())
			},
		},
		{
			name: "success-with-failed-operation",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
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
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, cb.State())
			},
		},
		{
			name: "retryable-rpc-error",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).Return(nil, status.Error(codes.Unavailable, "service unavailable"))
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayload([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{
						Serializer: commonnexus.PayloadSerializer,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          encodedRef,
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name: "non-retryable-rpc-error",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CompleteNexusOperationChasm(
					gomock.Any(),
					gomock.Any(),
				).Return(nil, status.Error(codes.InvalidArgument, "invalid request"))
				return client
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayload([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{
						Serializer: commonnexus.PayloadSerializer,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          encodedRef,
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
			},
		},
		{
			name: "invalid-base64-header",
			setupHistoryClient: func(t *testing.T, ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				// No RPC call expected
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			completion: func() nexusrpc.OperationCompletion {
				comp, err := nexusrpc.NewOperationCompletionSuccessful(
					createPayload([]byte("result-data")),
					nexusrpc.OperationCompletionSuccessfulOptions{
						Serializer: commonnexus.PayloadSerializer,
					},
				)
				require.NoError(t, err)
				return comp
			}(),
			headerValue:          "invalid-base64!!!",
			expectsInternalError: true,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			namespaceRegistryMock := namespace.NewMockRegistry(ctrl)
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
			namespaceRegistryMock.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)
			historyClient := tc.setupHistoryClient(t, ctrl)

			headers := nexus.Header{}
			if tc.headerValue != "" {
				headers.Set(commonnexus.CallbackTokenHeader, tc.headerValue)
			}

			// Create mutable state with the test completion
			mutableState := mutableState{
				completionNexus: tc.completion,
			}

			reg := hsm.NewRegistry()
			require.NoError(t, workflow.RegisterStateMachine(reg))
			require.NoError(t, callbacks.RegisterStateMachine(reg))

			root, err := hsm.NewRoot(reg, workflow.StateMachineType, mutableState, make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
			require.NoError(t, err)

			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Nexus_{
							Nexus: &persistencespb.Callback_Nexus{
								Url:    chasm.NexusCompletionHandlerURL,
								Header: headers,
							},
						},
					},
					State:     enumsspb.CALLBACK_STATE_SCHEDULED,
					RequestId: "request-id",
					Attempt:   1,
				},
			}

			require.NoError(t, callbacks.RegisterExecutor(reg, callbacks.TaskExecutorOptions{
				NamespaceRegistry: namespaceRegistryMock,
				MetricsHandler:    metrics.NoopMetricsHandler,
				HistoryClient:     historyClient,
				Logger:            log.NewNoopLogger(),
				Config: &callbacks.Config{
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
			}))

			coll := callbacks.MachineCollection(root)
			node, err := coll.Add("ID", cb)
			require.NoError(t, err)
			env := fakeEnv{node}

			err = reg.ExecuteImmediateTask(
				context.Background(),
				env,
				hsm.Ref{
					WorkflowKey: definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"),
					StateMachineRef: &persistencespb.StateMachineRef{
						Path: []*persistencespb.StateMachineKey{
							{
								Type: callbacks.StateMachineType,
								Id:   "ID",
							},
						},
					},
				},
				callbacks.InvocationTask{},
			)

			if tc.expectsInternalError {
				require.ErrorContains(t, err, "internal error, reference-id:")
			} else {
				require.NoError(t, err)
			}

			tc.assertOutcome(t, cb)
		})
	}
}
