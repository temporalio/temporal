package nexusoperation

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"text/template"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// invocationTaskTestEnv holds the test infrastructure for invocation task handler tests.
type invocationTaskTestEnv struct {
	t          *testing.T
	ctrl       *gomock.Controller
	handler    *operationInvocationTaskHandler
	op         *Operation
	mockEngine *chasm.MockEngine
	timeSource *clock.EventTimeSource
}

func newInvocationTaskTestEnv(
	t *testing.T,
	op *Operation,
	invocationData InvocationData,
	endpointReg nexustest.FakeEndpointRegistry,
	clientProvider ClientProvider,
	metricsHandler metrics.Handler,
	requestTimeout time.Duration,
) *invocationTaskTestEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
		namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

	callbackTmpl, err := template.New("callback").Parse("http://localhost/callback")
	require.NoError(t, err)

	handler := &operationInvocationTaskHandler{
		config: &Config{
			RequestTimeout:          dynamicconfig.GetDurationPropertyFnFilteredByDestination(requestTimeout),
			MaxOperationTokenLength: dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
			MinRequestTimeout:       dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Millisecond),
			PayloadSizeLimit:        dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024),
			CallbackURLTemplate:     dynamicconfig.GetTypedPropertyFn(callbackTmpl),
			UseSystemCallbackURL:    dynamicconfig.GetBoolPropertyFn(false),
			UseNewFailureWireFormat: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
			RetryPolicy: dynamicconfig.GetTypedPropertyFn[backoff.RetryPolicy](
				backoff.NewExponentialRetryPolicy(time.Second),
			),
		},
		namespaceRegistry:      nsRegistry,
		metricsHandler:         metricsHandler,
		logger:                 log.NewNoopLogger(),
		clientProvider:         clientProvider,
		endpointRegistry:       endpointReg,
		callbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
	}

	// Set up CHASM tree with mock store as parent of the operation.
	logger := log.NewNoopLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&mockStoreLibrary{}))
	require.NoError(t, registry.Register(&Library{}))

	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	root := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), root)
	require.NoError(t, root.SetRootComponent(&mockStoreComponent{
		invocationData: invocationData,
		Op:             chasm.NewComponentField(ctx, op),
	}))
	_, err = root.CloseTransaction()
	require.NoError(t, err)

	mockEngine := chasm.NewMockEngine(ctrl)

	return &invocationTaskTestEnv{
		t:          t,
		ctrl:       ctrl,
		handler:    handler,
		op:         op,
		mockEngine: mockEngine,
		timeSource: timeSource,
	}
}

func (e *invocationTaskTestEnv) setupReadComponent() {
	e.mockEngine.EXPECT().ReadComponent(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
		executionKey := chasm.ExecutionKey{
			NamespaceID: "ns-id",
			BusinessID:  "wf-id",
			RunID:       "run-id",
		}
		mockCtx := &chasm.MockContext{
			HandleExecutionKey: func() chasm.ExecutionKey {
				return executionKey
			},
			HandleNow: func(_ chasm.Component) time.Time {
				return e.timeSource.Now()
			},
			HandleRef: func(_ chasm.Component) ([]byte, error) {
				return []byte{}, nil
			},
			HandleNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
			},
		}
		return readFn(mockCtx, e.op)
	})
}

func (e *invocationTaskTestEnv) setupUpdateComponent() {
	e.mockEngine.EXPECT().UpdateComponent(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
		mockCtx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(_ chasm.Component) time.Time {
					return e.timeSource.Now()
				},
			},
		}
		err := updateFn(mockCtx, e.op)
		return nil, err
	})
}

func (e *invocationTaskTestEnv) execute(task *nexusoperationpb.InvocationTask) error {
	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: "ns-id",
		BusinessID:  "wf-id",
		RunID:       "run-id",
	})
	engineCtx := chasm.NewEngineContext(context.Background(), e.mockEngine)
	return e.handler.Execute(engineCtx, ref, chasm.TaskAttributes{Destination: "endpoint"}, task)
}

func TestInvocationTaskHandler_HTTP(t *testing.T) {
	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "handler-ns",
		WorkflowId: "handler-wf-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	handlerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	cases := []struct {
		name                  string
		header                nexus.Header
		onStartOperation      func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, op *Operation)
		requestTimeout        time.Duration
		schedToCloseTimeout   time.Duration
		startToCloseTimeout   time.Duration
		schedToStartTimeout   time.Duration
		destinationDown       bool
		endpointNotFound      bool
	}{
		{
			name: "async start",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if len(options.Links) != 2 {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected 2 links, got %d", len(options.Links))
				}
				workflowEventLinkIdx := slices.IndexFunc(options.Links, func(link nexus.Link) bool {
					return link.Type == string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName())
				})
				if workflowEventLinkIdx == -1 {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "missing workflow event link")
				}
				link, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(options.Links[workflowEventLinkIdx])
				if err != nil {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to convert link: %v", err)
				}
				expectedLink := &commonpb.Link_WorkflowEvent{
					Namespace:  "ns-name",
					WorkflowId: "wf-id",
					RunId:      "run-id",
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
						},
					},
				}
				if !proto.Equal(expectedLink, link) {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "link mismatch: got %v, want %v", link, expectedLink)
				}
				protoLinks := commonnexus.ConvertLinksToProto(options.Links)
				if protoLinks[1].GetType() != "temporal.api.common.v1.Link.NexusOperation" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected nexus operation link type: %v", protoLinks[1].GetType())
				}
				if protoLinks[1].GetUrl() != "temporal:///namespaces/ns-name/nexus-operations/wf-id?runID=run-id" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected nexus operation link URL: %v", protoLinks[1].GetUrl())
				}
				nexus.AddHandlerLinks(ctx, handlerNexusLink)
				return &nexus.HandlerStartOperationResultAsync{
					OperationToken: "op-token",
				}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
				require.Equal(t, "op-token", op.OperationToken)
			},
		},
		{
			name:                "sync start",
			schedToCloseTimeout: time.Hour,
			header:              nexus.Header{nexus.HeaderOperationTimeout: commonnexus.FormatDuration(time.Millisecond)},
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if service != "service" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid service name")
				}
				if operation != "operation" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation name")
				}
				if options.CallbackHeader.Get("temporal-callback-token") == "" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "empty callback token")
				}
				if options.CallbackURL != "http://localhost/callback" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback URL")
				}
				if options.Header.Get(nexus.HeaderOperationTimeout) != "1ms" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation timeout header: %s", options.Header.Get(nexus.HeaderOperationTimeout))
				}
				var v string
				if err := input.Consume(&v); err != nil || v != "input" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
				}
				return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.Status)
			},
		},
		{
			name: "sync failed",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State:   nexus.OperationStateFailed,
					Message: "operation failed from handler",
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "cause", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					},
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:failed",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation failed from handler",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "OperationError",
							NonRetryable: true,
						},
					},
					Cause: &failurepb.Failure{
						Message: "cause",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: "NexusFailure",
								Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
									Metadata: map[string][]byte{"encoding": []byte("json/plain")},
									Data:     []byte(`{"metadata":{"encoding":"json/plain"},"details":"details"}`),
								}}},
							},
						},
					},
				}, op.LastAttemptFailure)
			},
		},
		{
			name: "sync canceled",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State:   nexus.OperationStateCanceled,
					Message: "operation canceled from handler",
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "cause", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					},
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:canceled",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation canceled from handler",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
					Cause: &failurepb.Failure{
						Message: "cause",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: "NexusFailure",
								Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
									Metadata: map[string][]byte{"encoding": []byte("json/plain")},
									Data:     []byte(`{"metadata":{"encoding":"json/plain"},"details":"details"}`),
								}}},
							},
						},
					},
				}, op.LastAttemptFailure)
			},
		},
		{
			name:            "transient error",
			destinationDown: true,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.Equal(t, string(nexus.HandlerErrorTypeInternal), op.LastAttemptFailure.GetNexusHandlerFailureInfo().GetType())
				require.Equal(t, "internal server error", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "invocation timeout by request timeout",
			requestTimeout:        2 * time.Millisecond,
			schedToCloseTimeout:   time.Hour,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "invocation timeout by ScheduleToCloseTimeout",
			schedToCloseTimeout:   10 * time.Millisecond,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				opTimeout, err := time.ParseDuration(options.Header.Get(nexus.HeaderOperationTimeout))
				if err != nil || opTimeout > 10*time.Millisecond {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation timeout header: %s", options.Header.Get(nexus.HeaderOperationTimeout))
				}
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "invocation timeout by ScheduleToStartTimeout",
			schedToStartTimeout:   10 * time.Millisecond,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if options.Header.Get(nexus.HeaderOperationTimeout) != "" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "operation timeout header should not be set, got: %s", options.Header.Get(nexus.HeaderOperationTimeout))
				}
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "operation timeout header set by StartToCloseTimeout",
			startToCloseTimeout:   1 * time.Minute,
			expectedMetricOutcome: "pending",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if options.Header.Get(nexus.HeaderOperationTimeout) != "60000ms" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation timeout header: %s", options.Header.Get(nexus.HeaderOperationTimeout))
				}
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
			},
		},
		{
			name:                  "ScheduleToCloseTimeout less than MinRequestTimeout",
			schedToCloseTimeout:   time.Microsecond,
			expectedMetricOutcome: "operation-timeout",
			onStartOperation:      nil, // Should not be called.
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
			},
		},
		{
			name:             "endpoint not found",
			endpointNotFound: true,
			onStartOperation: nil, // Should not be called.
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
			},
		},
		{
			name: "token too long",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "12345678901"}, nil
			},
			expectedMetricOutcome: "invalid-operation-token",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.True(t, op.LastAttemptFailure.GetServerFailureInfo().GetNonRetryable())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			listenAddr := nexustest.AllocListenAddress()
			h := nexustest.Handler{}
			if tc.onStartOperation != nil {
				h.OnStartOperation = tc.onStartOperation
			}
			nexustest.NewNexusServer(t, listenAddr, h)

			op := &Operation{
				OperationState: &nexusoperationpb.OperationState{
					Status:                 nexusoperationpb.OPERATION_STATUS_SCHEDULED,
					EndpointId:             "endpoint-id",
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "operation",
					ScheduledTime:          timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(tc.schedToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.schedToStartTimeout),
					StartToCloseTimeout:    durationpb.New(tc.startToCloseTimeout),
					RequestId:              "request-id",
					Attempt:                1,
				},
			}
			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByID: func(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
				OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
			}

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			clientProvider := func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
				return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
					BaseURL:    "http://" + listenAddr,
					Service:    service,
					Serializer: commonnexus.PayloadSerializer,
				})
			}

			callerLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
				Namespace:  "ns-name",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					},
				},
			})

			env := newInvocationTaskTestEnv(t, op,
				InvocationData{
					Input:      mustToPayload(t, "input"),
					Header:     tc.header,
					NexusLinks: []nexus.Link{callerLink},
				},
				endpointReg, clientProvider, metricsHandler, cmp.Or(tc.requestTimeout, time.Hour))

			env.setupReadComponent()
			env.setupUpdateComponent()

			err := env.execute(&nexusoperationpb.InvocationTask{Attempt: 1})
			if tc.destinationDown {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}
			tc.checkOutcome(t, op)

			if tc.expectedMetricOutcome != "" {
				snap := capture.Snapshot()
				counterRecordings := snap[OutboundRequestCounter.Name()]
				require.Len(t, counterRecordings, 1)
				require.Equal(t, int64(1), counterRecordings[0].Value)
				require.Equal(t, "ns-name", counterRecordings[0].Tags["namespace"])
				require.Equal(t, "endpoint", counterRecordings[0].Tags["destination"])
				require.Equal(t, "StartOperation", counterRecordings[0].Tags["method"])
				require.Equal(t, tc.expectedMetricOutcome, counterRecordings[0].Tags["outcome"])
				require.Equal(t, "_unknown_", counterRecordings[0].Tags["failure_source"])

				timerRecordings := snap[OutboundRequestLatency.Name()]
				require.Len(t, timerRecordings, 1)
				require.Equal(t, tc.expectedMetricOutcome, timerRecordings[0].Tags["outcome"])
			}
		})
	}
}

func TestInvocationTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name        string
		status      nexusoperationpb.OperationStatus
		opAttempt   int32
		taskAttempt int32
		valid       bool
	}{
		{
			name:        "valid when scheduled and attempt matches",
			status:      nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			opAttempt:   1,
			taskAttempt: 1,
			valid:       true,
		},
		{
			name:        "invalid when scheduled but attempt mismatches",
			status:      nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			opAttempt:   2,
			taskAttempt: 1,
			valid:       false,
		},
		{
			name:        "invalid when started",
			status:      nexusoperationpb.OPERATION_STATUS_STARTED,
			opAttempt:   1,
			taskAttempt: 1,
			valid:       false,
		},
	}

	handler := &operationInvocationTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status
			op.Attempt = tc.opAttempt

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.InvocationTask{Attempt: tc.taskAttempt})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestBackoffTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		status  nexusoperationpb.OperationStatus
		attempt int32
		task    *nexusoperationpb.InvocationBackoffTask
		valid   bool
	}{
		{
			name:    "valid when backing off and attempt matches",
			status:  nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			attempt: 2,
			task:    &nexusoperationpb.InvocationBackoffTask{Attempt: 2},
			valid:   true,
		},
		{
			name:    "invalid when backing off but attempt mismatches",
			status:  nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			attempt: 2,
			task:    &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			valid:   false,
		},
		{
			name:    "invalid when scheduled",
			status:  nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			attempt: 1,
			task:    &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			valid:   false,
		},
	}

	handler := &operationBackoffTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status
			op.Attempt = tc.attempt

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, tc.task)
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestBackoffTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	op.Attempt = 2

	handler := &operationBackoffTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.InvocationBackoffTask{Attempt: 2})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)
	// Verify invocation task was emitted
	require.Len(t, ctx.Tasks, 1)
	_, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
}

func TestScheduleToStartTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name   string
		status nexusoperationpb.OperationStatus
		valid  bool
	}{
		{
			name:   "valid when scheduled",
			status: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			valid:  true,
		},
		{
			name:   "valid when backing off",
			status: nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			valid:  true,
		},
		{
			name:   "invalid when started",
			status: nexusoperationpb.OPERATION_STATUS_STARTED,
			valid:  false,
		},
		{
			name:   "invalid when succeeded",
			status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			valid:  false,
		},
		{
			name:   "invalid when timed out",
			status: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			valid:  false,
		},
	}

	handler := &operationScheduleToStartTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToStartTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestScheduleToStartTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	handler := &operationScheduleToStartTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToStartTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}

func TestStartToCloseTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name   string
		status nexusoperationpb.OperationStatus
		valid  bool
	}{
		{
			name:   "valid when started",
			status: nexusoperationpb.OPERATION_STATUS_STARTED,
			valid:  true,
		},
		{
			name:   "invalid when scheduled",
			status: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			valid:  false,
		},
		{
			name:   "invalid when backing off",
			status: nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			valid:  false,
		},
		{
			name:   "invalid when succeeded",
			status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			valid:  false,
		},
		{
			name:   "invalid when timed out",
			status: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			valid:  false,
		},
	}

	handler := &operationStartToCloseTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.StartToCloseTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestStartToCloseTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	handler := &operationStartToCloseTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.StartToCloseTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}

func TestScheduleToCloseTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name   string
		status nexusoperationpb.OperationStatus
		valid  bool
	}{
		{
			name:   "valid when scheduled",
			status: nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			valid:  true,
		},
		{
			name:   "valid when started",
			status: nexusoperationpb.OPERATION_STATUS_STARTED,
			valid:  true,
		},
		{
			name:   "invalid when succeeded",
			status: nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			valid:  false,
		},
		{
			name:   "invalid when timed out",
			status: nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			valid:  false,
		},
	}

	handler := &operationScheduleToCloseTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToCloseTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestScheduleToCloseTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	handler := &operationScheduleToCloseTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToCloseTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}

// testStartProcessor implements chasm.NexusOperationProcessor[string] for system endpoint start tests.
type testStartProcessor struct{}

func (p *testStartProcessor) ProcessInput(
	_ chasm.NexusOperationProcessorContext,
	_ string,
) (*chasm.NexusOperationProcessorResult, error) {
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyRandom{},
	}, nil
}

// testStartProcessorWithInput implements chasm.NexusOperationProcessor[*testProcessorInput]
// for system endpoint tests that verify input re-serialization.
type testProcessorInput struct {
	Value string
}

type testStartProcessorWithInput struct{}

func (p *testStartProcessorWithInput) ProcessInput(
	_ chasm.NexusOperationProcessorContext,
	input *testProcessorInput,
) (*chasm.NexusOperationProcessorResult, error) {
	input.Value = "processed:" + input.Value
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyRandom{},
	}, nil
}

func TestInvocationTaskHandler_SystemEndpoint(t *testing.T) {
	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "handler-ns",
		WorkflowId: "handler-wf-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}

	cases := []struct {
		name                  string
		setupHistoryClient    func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient
		setupChasmRegistry    func() *chasm.Registry
		input                 *commonpb.Payload
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, op *Operation)
	}{
		{
			name: "async start",
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *historyservice.StartNexusOperationRequest, _ ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
					require.Len(t, request.GetRequest().GetLinks(), 2)
					require.Equal(t, "temporal.api.common.v1.Link.WorkflowEvent", request.GetRequest().GetLinks()[0].GetType())
					require.Equal(t, "temporal.api.common.v1.Link.NexusOperation", request.GetRequest().GetLinks()[1].GetType())

					return &historyservice.StartNexusOperationResponse{
						Response: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
								AsyncSuccess: &nexuspb.StartOperationResponse_Async{
									OperationToken: "system-op-token",
									Links: commonnexus.ConvertLinksToProto([]nexus.Link{
										commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink),
									}),
								},
							},
						},
					}, nil
				})
				return client
			},
			setupChasmRegistry: func() *chasm.Registry {
				reg := chasm.NewRegistry(log.NewNoopLogger())
				serviceProc := chasm.NewNexusServiceProcessor("service")
				serviceProc.MustRegisterOperation("operation",
					chasm.NewRegisterableNexusOperationProcessor(&testStartProcessor{}))
				reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
				return reg
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
				require.Equal(t, "system-op-token", op.OperationToken)
			},
		},
		{
			name: "sync start",
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *historyservice.StartNexusOperationRequest, opts ...grpc.CallOption) (*historyservice.StartNexusOperationResponse, error) {
					require.Len(t, request.GetRequest().GetLinks(), 2)
					require.Equal(t, "temporal.api.common.v1.Link.WorkflowEvent", request.GetRequest().GetLinks()[0].GetType())
					require.Equal(t, "temporal.api.common.v1.Link.NexusOperation", request.GetRequest().GetLinks()[1].GetType())

					var input testProcessorInput
					if err := payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{request.Request.Payload}}, &input); err != nil {
						return nil, err
					}
					if input.Value != "processed:test" {
						return nil, fmt.Errorf("unexpected input: %v", input.Value)
					}
					return &historyservice.StartNexusOperationResponse{
						Response: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_SyncSuccess{
								SyncSuccess: &nexuspb.StartOperationResponse_Sync{
									Payload: mustToPayload(t, "result"),
								},
							},
						},
					}, nil
				})
				return client
			},
			setupChasmRegistry: func() *chasm.Registry {
				reg := chasm.NewRegistry(log.NewNoopLogger())
				serviceProc := chasm.NewNexusServiceProcessor("service")
				serviceProc.MustRegisterOperation("operation",
					chasm.NewRegisterableNexusOperationProcessor(&testStartProcessorWithInput{}))
				reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
				return reg
			},
			input:                 mustToPayload(t, testProcessorInput{"test"}),
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.Status)
			},
		},
		{
			name: "operation error",
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					&historyservice.StartNexusOperationResponse{
						Response: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_Failure{
								Failure: &failurepb.Failure{
									Message: "operation failed",
									FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
										ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
									},
								},
							},
						},
					}, nil,
				)
				return client
			},
			setupChasmRegistry: func() *chasm.Registry {
				reg := chasm.NewRegistry(log.NewNoopLogger())
				serviceProc := chasm.NewNexusServiceProcessor("service")
				serviceProc.MustRegisterOperation("operation",
					chasm.NewRegisterableNexusOperationProcessor(&testStartProcessor{}))
				reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
				return reg
			},
			expectedMetricOutcome: "operation-unsuccessful:failed",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation failed",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
					},
				}, op.LastAttemptFailure)
			},
		},
		{
			name: "history service error - retryable",
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().StartNexusOperation(gomock.Any(), gomock.Any(), gomock.Any()).Return(
					nil, serviceerror.NewUnavailable("service unavailable"),
				)
				return client
			},
			setupChasmRegistry: func() *chasm.Registry {
				reg := chasm.NewRegistry(log.NewNoopLogger())
				serviceProc := chasm.NewNexusServiceProcessor("service")
				serviceProc.MustRegisterOperation("operation",
					chasm.NewRegisterableNexusOperationProcessor(&testStartProcessor{}))
				reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
				return reg
			},
			expectedMetricOutcome: "service-error:Unavailable",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "Unavailable: service unavailable",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{},
					},
				}, op.LastAttemptFailure)
			},
		},
		{
			name: "chasm processor error",
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				// Should not be called if processor fails.
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			setupChasmRegistry: func() *chasm.Registry {
				// Don't register a processor so ProcessInput fails.
				return chasm.NewRegistry(log.NewNoopLogger())
			},
			expectedMetricOutcome: "operation-processor-failed",
			checkOutcome: func(t *testing.T, op *Operation) {
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: `service "service" not found`,
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: string(nexus.HandlerErrorTypeNotFound),
						},
					},
				}, op.LastAttemptFailure)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			op := &Operation{
				OperationState: &nexusoperationpb.OperationState{
					Status:                 nexusoperationpb.OPERATION_STATUS_SCHEDULED,
					Endpoint:               commonnexus.SystemEndpoint,
					Service:                "service",
					Operation:              "operation",
					ScheduledTime:          timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					RequestId:              "request-id",
					Attempt:                1,
				},
			}

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			input := tc.input
			if input == nil {
				input = mustToPayload(t, "test")
			}

			callerLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
				Namespace:  "ns-name",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					},
				},
			})

			env := newInvocationTaskTestEnv(t, op,
				InvocationData{Input: input, NexusLinks: []nexus.Link{callerLink}},
				nexustest.FakeEndpointRegistry{}, nil, metricsHandler, time.Hour)

			// Set up system endpoint dependencies.
			historyClient := tc.setupHistoryClient(env.ctrl)
			env.handler.historyClient = historyClient
			env.handler.config.NumHistoryShards = 4
			env.handler.config.MaxOperationTokenLength = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1000)
			env.handler.chasmRegistry = tc.setupChasmRegistry()

			env.setupReadComponent()
			env.setupUpdateComponent()

			err := env.execute(&nexusoperationpb.InvocationTask{Attempt: 1})
			var destinationDownErr *queueserrors.DestinationDownError
			require.NotErrorAs(t, err, &destinationDownErr)
			require.NoError(t, err)

			tc.checkOutcome(t, op)

			snap := capture.Snapshot()
			counterRecordings := snap[OutboundRequestCounter.Name()]
			require.Len(t, counterRecordings, 1)
			require.Equal(t, int64(1), counterRecordings[0].Value)
			require.Equal(t, "ns-name", counterRecordings[0].Tags["namespace"])
			require.Equal(t, commonnexus.SystemEndpoint, counterRecordings[0].Tags["destination"])
			require.Equal(t, "StartOperation", counterRecordings[0].Tags["method"])
			require.Equal(t, tc.expectedMetricOutcome, counterRecordings[0].Tags["outcome"])

			timerRecordings := snap[OutboundRequestLatency.Name()]
			require.Len(t, timerRecordings, 1)
			require.Equal(t, tc.expectedMetricOutcome, timerRecordings[0].Tags["outcome"])
		})
	}
}
