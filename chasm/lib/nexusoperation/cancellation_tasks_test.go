package nexusoperation

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
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
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// cancellationTaskTestEnv holds the test infrastructure for cancellation task handler tests.
type cancellationTaskTestEnv struct {
	t            *testing.T
	ctrl         *gomock.Controller
	handler      *cancellationInvocationTaskHandler
	op           *Operation
	cancellation *Cancellation
	mockEngine   *chasm.MockEngine
	timeSource   *clock.EventTimeSource
}

func newCancellationTaskTestEnv(
	t *testing.T,
	op *Operation,
	cancellation *Cancellation,
	invocationData InvocationData,
	endpointReg nexustest.FakeEndpointRegistry,
	clientProvider ClientProvider,
	metricsHandler metrics.Handler,
	requestTimeout time.Duration,
) *cancellationTaskTestEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
		namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

	handler := &cancellationInvocationTaskHandler{
		nexusTaskHandlerBase: nexusTaskHandlerBase{
			config: &Config{
				RequestTimeout:          dynamicconfig.GetDurationPropertyFnFilteredByDestination(requestTimeout),
				MinRequestTimeout:       dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Millisecond),
				UseNewFailureWireFormat: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
				RetryPolicy: dynamicconfig.GetTypedPropertyFn[backoff.RetryPolicy](
					backoff.NewExponentialRetryPolicy(time.Second),
				),
			},
			namespaceRegistry: nsRegistry,
			metricsHandler:    metricsHandler,
			logger:            log.NewNoopLogger(),
			clientProvider:    clientProvider,
			endpointRegistry:  endpointReg,
		},
	}

	// Wire mock parent pointers so Cancellation.loadArgs can traverse the tree.
	mockStore := &mockStoreComponent{invocationData: invocationData}
	op.Store = chasm.NewMockParentPtr[OperationStore](mockStore)
	cancellation.Operation = chasm.NewMockParentPtr(op)
	op.Cancellation = chasm.NewComponentField(nil, cancellation)

	mockEngine := chasm.NewMockEngine(ctrl)

	return &cancellationTaskTestEnv{
		t:            t,
		ctrl:         ctrl,
		handler:      handler,
		op:           op,
		cancellation: cancellation,
		mockEngine:   mockEngine,
		timeSource:   timeSource,
	}
}

func (e *cancellationTaskTestEnv) setupReadComponent() {
	e.mockEngine.EXPECT().ReadComponent(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
		mockCtx := &chasm.MockContext{
			HandleNow: func(_ chasm.Component) time.Time {
				return e.timeSource.Now()
			},
		}
		return readFn(mockCtx, e.cancellation)
	})
}

func (e *cancellationTaskTestEnv) setupUpdateComponent() {
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
		err := updateFn(mockCtx, e.cancellation)
		return nil, err
	})
}

func (e *cancellationTaskTestEnv) execute(task *nexusoperationpb.CancellationTask) error {
	ref := chasm.NewComponentRef[*Cancellation](chasm.ExecutionKey{
		NamespaceID: "ns-id",
		BusinessID:  "wf-id",
		RunID:       "run-id",
	})
	engineCtx := chasm.NewEngineContext(context.Background(), e.mockEngine)
	return e.handler.Execute(engineCtx, ref, chasm.TaskAttributes{Destination: "endpoint"}, task)
}

func TestCancellationInvocationTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name          string
		status        nexusoperationpb.CancellationStatus
		cancelAttempt int32
		taskAttempt   int32
		valid         bool
	}{
		{
			name:          "valid when scheduled and attempt matches",
			status:        nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
			cancelAttempt: 1,
			taskAttempt:   1,
			valid:         true,
		},
		{
			name:          "invalid when scheduled but attempt mismatches",
			status:        nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
			cancelAttempt: 2,
			taskAttempt:   1,
			valid:         false,
		},
		{
			name:          "invalid when backing off",
			status:        nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
			cancelAttempt: 1,
			taskAttempt:   1,
			valid:         false,
		},
	}

	handler := &cancellationInvocationTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCancellation(&nexusoperationpb.CancellationState{
				Status:  tc.status,
				Attempt: tc.cancelAttempt,
			})

			valid, err := handler.Validate(ctx, c, chasm.TaskAttributes{}, &nexusoperationpb.CancellationTask{Attempt: tc.taskAttempt})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestCancellationBackoffTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		status  nexusoperationpb.CancellationStatus
		attempt int32
		task    *nexusoperationpb.CancellationBackoffTask
		valid   bool
	}{
		{
			name:    "valid when backing off and attempt matches",
			status:  nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
			attempt: 2,
			task:    &nexusoperationpb.CancellationBackoffTask{Attempt: 2},
			valid:   true,
		},
		{
			name:    "invalid when backing off but attempt mismatches",
			status:  nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
			attempt: 2,
			task:    &nexusoperationpb.CancellationBackoffTask{Attempt: 1},
			valid:   false,
		},
		{
			name:    "invalid when scheduled",
			status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
			attempt: 1,
			task:    &nexusoperationpb.CancellationBackoffTask{Attempt: 1},
			valid:   false,
		},
	}

	handler := &cancellationBackoffTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCancellation(&nexusoperationpb.CancellationState{
				Status:  tc.status,
				Attempt: tc.attempt,
			})

			valid, err := handler.Validate(ctx, c, chasm.TaskAttributes{}, tc.task)
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestCancellationBackoffTaskHandler_Execute(t *testing.T) {
	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_STARTED
	op.OperationToken = "op-token"

	cancellation := newCancellation(&nexusoperationpb.CancellationState{
		Status:  nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		Attempt: 2,
	})
	cancellation.Operation = chasm.NewMockParentPtr(op)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	handler := &cancellationBackoffTaskHandler{}
	err := handler.Execute(ctx, cancellation, chasm.TaskAttributes{}, &nexusoperationpb.CancellationBackoffTask{Attempt: 2})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.Status)
	require.Equal(t, int32(3), cancellation.Attempt)
	require.Len(t, ctx.Tasks, 1)
	_, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationTask)
	require.True(t, ok, "expected CancellationTask")
}

func TestCancellationInvocationTaskHandler_Execute(t *testing.T) {
	cases := []struct {
		name                  string
		header                map[string]string
		onCancelOperation     func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, c *Cancellation, err error)
		requestTimeout        time.Duration
		schedToCloseTimeout   time.Duration
		startToCloseTimeout   time.Duration
		destinationDown       bool
		endpointNotFound      bool
	}{
		{
			name:            "failure",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					Message:       "operation not found",
					RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
				}
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				require.Equal(t, string(nexus.HandlerErrorTypeInternal), c.LastAttemptFailure.GetNexusHandlerFailureInfo().GetType())
				require.Equal(t, "operation not found", c.LastAttemptFailure.Message)
			},
		},
		{
			name:            "success",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, c.Status)
				require.Nil(t, c.LastAttemptFailure)
			},
		},
		{
			name:            "success with headers",
			requestTimeout:  time.Hour,
			destinationDown: false,
			header:          map[string]string{"key": "value"},
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				if options.Header["key"] != "value" {
					return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, `"key" header is not equal to "value"`)
				}
				return nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, c.Status)
				require.Nil(t, c.LastAttemptFailure)
			},
		},
		{
			name:            "transient error",
			requestTimeout:  time.Hour,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				require.NotNil(t, c.LastAttemptFailure.GetNexusHandlerFailureInfo())
				require.Equal(t, "internal server error", c.LastAttemptFailure.Message)
			},
		},
		{
			name:            "invocation timeout",
			requestTimeout:  10 * time.Millisecond,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				require.NotNil(t, c.LastAttemptFailure.GetServerFailureInfo())
				require.Equal(t, "request timed out", c.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "operation timeout by ScheduleToCloseTimeout",
			requestTimeout:        time.Hour,
			schedToCloseTimeout:   time.Microsecond,
			destinationDown:       false,
			onCancelOperation:     nil, // Should not be called if the operation has timed out.
			expectedMetricOutcome: "operation-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				require.NotNil(t, c.LastAttemptFailure.GetTimeoutFailureInfo())
				require.Contains(t, c.LastAttemptFailure.Message, "operation timed out")
			},
		},
		{
			name:                  "operation timeout by StartToCloseTimeout",
			requestTimeout:        time.Hour,
			startToCloseTimeout:   time.Microsecond,
			destinationDown:       false,
			onCancelOperation:     nil, // Should not be called if the operation has timed out.
			expectedMetricOutcome: "operation-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				require.NotNil(t, c.LastAttemptFailure.GetTimeoutFailureInfo())
				require.Contains(t, c.LastAttemptFailure.Message, "operation timed out")
			},
		},
		{
			name:              "endpoint not found",
			endpointNotFound:  true,
			requestTimeout:    time.Hour,
			destinationDown:   false,
			onCancelOperation: nil, // Should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, c *Cancellation, err error) {
				require.NoError(t, err)
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				require.Equal(t, string(nexus.HandlerErrorTypeNotFound), c.LastAttemptFailure.GetNexusHandlerFailureInfo().GetType())
				require.Equal(t, "endpoint not registered", c.LastAttemptFailure.Message)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress()
			h := nexustest.Handler{}
			h.OnCancelOperation = tc.onCancelOperation
			nexustest.NewNexusServer(t, listenAddr, h)

			op := &Operation{
				OperationState: &nexusoperationpb.OperationState{
					Status:                 nexusoperationpb.OPERATION_STATUS_STARTED,
					EndpointId:             "endpoint-id",
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "operation",
					ScheduledTime:          timestamppb.Now(),
					StartedTime:            timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(tc.schedToCloseTimeout),
					StartToCloseTimeout:    durationpb.New(tc.startToCloseTimeout),
					RequestId:              "request-id",
					OperationToken:         "op-token",
					Attempt:                1,
				},
			}
			cancellation := newCancellation(&nexusoperationpb.CancellationState{
				Status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
				Attempt: 1,
			})

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
					metrics.NexusMethodTag("CancelOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
				mockMetrics.EXPECT().Timer(OutboundRequestLatency.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("CancelOperation"),
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

			env := newCancellationTaskTestEnv(t, op, cancellation,
				InvocationData{
					Header: tc.header,
				},
				endpointReg, clientProvider, metricsHandler, tc.requestTimeout)

			env.setupReadComponent()
			env.setupUpdateComponent()

			err := env.execute(&nexusoperationpb.CancellationTask{Attempt: 1})
			tc.checkOutcome(t, cancellation, err)
		})
	}
}
