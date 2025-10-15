package callbacks_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/scheduler"
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
		destinationDown       bool
		expectedMetricOutcome string
		assertOutcome         func(*testing.T, callbacks.Callback)
	}{
		{
			name: "success",
			caller: func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			},
			destinationDown:       false,
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
			destinationDown:       true,
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
			destinationDown:       true,
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
			destinationDown:       false,
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
			namespaceRegistryMock.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(
				namespace.FromPersistentState(&persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{
						Id:   "namespace-id",
						Name: "namespace-name",
					},
					Config: &persistencespb.NamespaceConfig{},
				}),
				nil,
			)
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
					HTTPCallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
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

			if tc.destinationDown {
				var destinationDownErr *queues.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}

			cb, err = coll.Data("ID")
			require.NoError(t, err)
			tc.assertOutcome(t, cb)
		})
	}
}

func TestProcessInvocationTaskHsm_Outcomes(t *testing.T) {
	cases := []struct {
		name                  string
		expectedError         error
		expectedMetricOutcome codes.Code
		assertOutcome         func(*testing.T, callbacks.Callback)
	}{
		{
			name:                  "success",
			expectedError:         nil,
			expectedMetricOutcome: codes.OK,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_SUCCEEDED, cb.State())
			},
		},
		{
			name:                  "retryable-error",
			expectedError:         status.Error(codes.Unavailable, "fake error"),
			expectedMetricOutcome: codes.Unavailable,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_BACKING_OFF, cb.State())
			},
		},
		{
			name:                  "non-retryable-error",
			expectedError:         status.Error(codes.NotFound, "fake error"),
			expectedMetricOutcome: codes.NotFound,
			assertOutcome: func(t *testing.T, cb callbacks.Callback) {
				require.Equal(t, enumsspb.CALLBACK_STATE_FAILED, cb.State())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			namespaceRegistryMock := namespace.NewMockRegistry(ctrl)
			namespaceRegistryMock.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(
				namespace.FromPersistentState(&persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{
						Id:   "namespace-id",
						Name: "namespace-name",
					},
					Config: &persistencespb.NamespaceConfig{},
				}),
				nil,
			)
			metricsHandler := metrics.NewMockHandler(ctrl)
			counter := metrics.NewMockCounterIface(ctrl)
			timer := metrics.NewMockTimerIface(ctrl)
			metricsHandler.EXPECT().Counter(callbacks.RequestCounter.Name()).Return(counter)
			counter.EXPECT().Record(int64(1),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag(""),
				metrics.OutcomeTag(fmt.Sprintf("status:%d", tc.expectedMetricOutcome)))
			metricsHandler.EXPECT().Timer(callbacks.RequestLatencyHistogram.Name()).Return(timer)
			timer.EXPECT().Record(gomock.Any(),
				metrics.NamespaceTag("namespace-name"),
				metrics.DestinationTag(""),
				metrics.OutcomeTag(fmt.Sprintf("status:%d", tc.expectedMetricOutcome)))

			root := newRoot(t)
			ref := &persistencespb.StateMachineRef{
				Path: []*persistencespb.StateMachineKey{
					{
						Type: scheduler.WorkflowType,
						Id:   "testId",
					},
				},
			}
			cb := callbacks.Callback{
				CallbackInfo: &persistencespb.CallbackInfo{
					Callback: &persistencespb.Callback{
						Variant: &persistencespb.Callback_Hsm{
							Hsm: &persistencespb.Callback_HSM{
								NamespaceId: "nsid",
								WorkflowId:  "wid",
								RunId:       "rid",
								Ref:         ref,
								Method:      "test",
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
			historyClientMock := historyservicemock.NewMockHistoryServiceClient(ctrl)

			historyClientMock.EXPECT().InvokeStateMachineMethod(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, in *historyservice.InvokeStateMachineMethodRequest, opts ...grpc.CallOption) (*historyservice.InvokeStateMachineMethodResponse, error) {
				require.Equal(t, "nsid", in.NamespaceId)
				require.Equal(t, "wid", in.WorkflowId)
				require.Equal(t, "rid", in.RunId)
				require.True(t, ref.Equal(in.Ref))
				require.Equal(t, "test", in.MethodName)
				arg := &persistencespb.HSMCompletionCallbackArg{}
				err = proto.Unmarshal(in.Input, arg)
				require.NoError(t, err)
				require.Equal(t, "mynsid", arg.NamespaceId)
				require.Equal(t, "mywid", arg.WorkflowId)
				require.Equal(t, "myrid", arg.RunId)
				require.Equal(t, int64(42), arg.LastEvent.EventId)
				require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, arg.LastEvent.EventType)

				return &historyservice.InvokeStateMachineMethodResponse{}, tc.expectedError
			}).Times(1)
			require.NoError(t, callbacks.RegisterExecutor(
				reg,
				callbacks.TaskExecutorOptions{
					NamespaceRegistry: namespaceRegistryMock,
					MetricsHandler:    metricsHandler,
					HistoryClient:     historyClientMock,
					Logger:            log.NewNoopLogger(),
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
				callbacks.InvocationTask{},
			)

			require.NoError(t, err)

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
			HTTPCallerProvider: func(nid queues.NamespaceIDAndDestination) callbacks.HTTPCaller {
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
