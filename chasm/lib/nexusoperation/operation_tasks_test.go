package nexusoperation

import (
	"context"
	"encoding/json"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var endpointEntry = &persistencespb.NexusEndpointEntry{
	Id: "endpoint-id",
	Endpoint: &persistencespb.NexusEndpoint{
		Spec: &persistencespb.NexusEndpointSpec{
			Name: "endpoint",
			Target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_External_{
					External: &persistencespb.NexusEndpointTarget_External{
						Url: "http://" + uuid.NewString(),
					},
				},
			},
		},
	},
}

func mustToPayload(t *testing.T, input any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(input)
	require.NoError(t, err)
	return payload
}

// mockStoreComponent is a mock parent component that implements OperationStore.
// It allows the Operation to load its start args and apply transitions.
// TODO(stephan): Remove this layer from tests once loading invocation data from the operation component is implemented.
type mockStoreComponent struct {
	chasm.UnimplementedComponent

	// Data is required by CHASM for serialization - every component needs a proto.Message field.
	Data *nexusoperationpb.OperationState

	invocationData InvocationData
	Op             chasm.Field[*Operation]
}

func (m *mockStoreComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (m *mockStoreComponent) NexusOperationInvocationData(_ chasm.Context, _ *Operation) (InvocationData, error) {
	return m.invocationData, nil
}

func (m *mockStoreComponent) OnNexusOperationStarted(ctx chasm.MutableContext, op *Operation, operationToken string, _ []*commonpb.Link) error {
	return TransitionStarted.Apply(op, ctx, EventStarted{OperationToken: operationToken})
}

func (m *mockStoreComponent) OnNexusOperationCompleted(ctx chasm.MutableContext, op *Operation, _ *commonpb.Payload, _ []*commonpb.Link) error {
	return TransitionSucceeded.Apply(op, ctx, EventSucceeded{})
}

func (m *mockStoreComponent) OnNexusOperationFailed(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure) error {
	return TransitionFailed.Apply(op, ctx, EventFailed{Failure: cause})
}

func (m *mockStoreComponent) OnNexusOperationCanceled(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure) error {
	return TransitionCanceled.Apply(op, ctx, EventCanceled{Failure: cause})
}

func (m *mockStoreComponent) OnNexusOperationTimedOut(ctx chasm.MutableContext, op *Operation, _ *failurepb.Failure) error {
	return TransitionTimedOut.Apply(op, ctx, EventTimedOut{})
}

func (m *mockStoreComponent) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func (m *mockStoreComponent) Terminate(_ chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

// mockStoreLibrary registers the mockStoreComponent so the CHASM tree can work with it.
type mockStoreLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *mockStoreLibrary) Name() string {
	return "mockStore"
}

func (l *mockStoreLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*mockStoreComponent]("mockStore"),
	}
}

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
		callbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
		clientProvider:         clientProvider,
		endpointRegistry:       endpointReg,
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
		mockCtx := &chasm.MockContext{
			HandleNow: func(_ chasm.Component) time.Time {
				return e.timeSource.Now()
			},
			HandleRef: func(_ chasm.Component) ([]byte, error) {
				return []byte{}, nil
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

func TestInvocationTaskHandler_Execute(t *testing.T) {
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
		name                       string
		header                     nexus.Header
		checkStartOperationOptions func(t *testing.T, options nexus.StartOperationOptions)
		onStartOperation           func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		expectedMetricOutcome      string
		checkOutcome               func(t *testing.T, op *Operation, err error)
		requestTimeout             time.Duration
		schedToCloseTimeout        time.Duration
		startToCloseTimeout        time.Duration
		schedToStartTimeout        time.Duration
		destinationDown            bool
		endpointNotFound           bool
		noUpdateComponent          bool // Set to true when the handler returns early without calling UpdateComponent.
	}{
		{
			name:           "async start",
			requestTimeout: time.Hour,
			checkStartOperationOptions: func(t *testing.T, options nexus.StartOperationOptions) {
				require.Len(t, options.Links, 1)
				link, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(options.Links[0])
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &commonpb.Link_WorkflowEvent{
					Namespace:  "ns-name",
					WorkflowId: "wf-id",
					RunId:      "run-id",
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
						},
					},
				}, link)
			},
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				nexus.AddHandlerLinks(ctx, handlerNexusLink)
				return &nexus.HandlerStartOperationResultAsync{
					OperationToken: "op-token",
				}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
				require.Equal(t, "op-token", op.OperationToken)
			},
		},
		{
			name:                "sync start",
			requestTimeout:      time.Hour,
			schedToCloseTimeout: time.Hour,
			// Pass a custom header to verify it is forwarded but the operation timeout is
			// determined by ScheduleToCloseTimeout, not this header value.
			header: nexus.Header{nexus.HeaderOperationTimeout: commonnexus.FormatDuration(time.Millisecond)},
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.Status)
			},
		},
		{
			name:           "sync failed",
			requestTimeout: time.Hour,
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
				failure := op.LastAttemptFailure
				require.NotNil(t, failure)
				require.Equal(t, "operation failed from handler", failure.Message)
				require.NotNil(t, failure.GetApplicationFailureInfo())
				require.Equal(t, "OperationError", failure.GetApplicationFailureInfo().GetType())
				require.True(t, failure.GetApplicationFailureInfo().GetNonRetryable())
				require.NotNil(t, failure.Cause)
				require.Equal(t, "cause", failure.Cause.Message)
				require.NotNil(t, failure.Cause.GetApplicationFailureInfo())
				require.Equal(t, "NexusFailure", failure.Cause.GetApplicationFailureInfo().GetType())
			},
		},
		{
			name:           "sync canceled",
			requestTimeout: time.Hour,
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.Status)
				failure := op.LastAttemptFailure
				require.NotNil(t, failure)
				require.Equal(t, "operation canceled from handler", failure.Message)
				require.NotNil(t, failure.GetCanceledFailureInfo())
				require.NotNil(t, failure.Cause)
				require.Equal(t, "cause", failure.Cause.Message)
				require.NotNil(t, failure.Cause.GetApplicationFailureInfo())
				require.Equal(t, "NexusFailure", failure.Cause.GetApplicationFailureInfo().GetType())
			},
		},
		{
			name:            "transient error",
			requestTimeout:  time.Hour,
			destinationDown: true,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "invocation timeout by ScheduleToCloseTimeout",
			requestTimeout:        time.Hour,
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "invocation timeout by ScheduleToStartTimeout",
			requestTimeout:        time.Hour,
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
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, op.Status)
				require.NotNil(t, op.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", op.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "operation timeout header set by StartToCloseTimeout",
			requestTimeout:        time.Hour,
			startToCloseTimeout:   1 * time.Minute,
			expectedMetricOutcome: "pending",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				if options.Header.Get(nexus.HeaderOperationTimeout) != "60000ms" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation timeout header: %s", options.Header.Get(nexus.HeaderOperationTimeout))
				}
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
			},
		},
		{
			name:                  "ScheduleToCloseTimeout less than MinRequestTimeout",
			requestTimeout:        time.Hour,
			schedToCloseTimeout:   time.Microsecond,
			expectedMetricOutcome: "operation-timeout",
			onStartOperation:      nil, // Should not be called.
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
			},
		},
		{
			name:             "endpoint not found",
			endpointNotFound: true,
			requestTimeout:   time.Hour,
			onStartOperation: nil, // Should not be called.
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
			},
		},
		{
			name:           "token too long",
			requestTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "12345678901"}, nil
			},
			expectedMetricOutcome: "invalid-operation-token",
			checkOutcome: func(t *testing.T, op *Operation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.Status)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress()
			h := nexustest.Handler{}
			if tc.onStartOperation != nil {
				h.OnStartOperation = func(
					ctx context.Context,
					service, operation string,
					input *nexus.LazyValue,
					options nexus.StartOperationOptions,
				) (nexus.HandlerStartOperationResult[any], error) {
					if tc.checkStartOperationOptions != nil {
						tc.checkStartOperationOptions(t, options)
					}
					return tc.onStartOperation(ctx, service, operation, input, options)
				}
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

			var metricsHandler metrics.Handler
			if tc.expectedMetricOutcome != "" {
				mockMetrics := metrics.NewMockHandler(ctrl)
				counter := metrics.NewMockCounterIface(ctrl)
				timer := metrics.NewMockTimerIface(ctrl)
				mockMetrics.EXPECT().Counter(OutboundRequestCounter.Name()).Return(counter)
				counter.EXPECT().Record(int64(1),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
				mockMetrics.EXPECT().Timer(OutboundRequestLatency.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
				metricsHandler = mockMetrics
			} else {
				metricsHandler = metrics.NoopMetricsHandler
			}

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
					Input:     mustToPayload(t, "input"),
					Header:    tc.header,
					NexusLink: callerLink,
				},
				endpointReg, clientProvider, metricsHandler, tc.requestTimeout)

			env.setupReadComponent()
			if !tc.noUpdateComponent {
				env.setupUpdateComponent()
			}

			err := env.execute(&nexusoperationpb.InvocationTask{Attempt: 1})
			tc.checkOutcome(t, op, err)
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
