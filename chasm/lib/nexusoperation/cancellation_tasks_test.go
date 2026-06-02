package nexusoperation

import (
	"cmp"
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
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
	"go.temporal.io/server/common/testing/protorequire"
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

func TestCancellationLoadArgs_StandaloneFallsBackToRequestData(t *testing.T) {
	now := time.Now().UTC()
	input := &commonpb.Payload{Data: []byte("test-input")}
	headers := map[string]string{"test-header": "test-value"}

	op := NewOperation(&nexusoperationpb.OperationState{
		Service:                "test-service",
		Operation:              "test-operation",
		OperationToken:         "test-operation-token",
		RequestId:              "test-request-id",
		Endpoint:               "test-endpoint",
		EndpointId:             "test-endpoint-id",
		ScheduledTime:          timestamppb.New(now.Add(-2 * time.Minute)),
		StartedTime:            timestamppb.New(now.Add(-1 * time.Minute)),
		ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
		StartToCloseTimeout:    durationpb.New(5 * time.Minute),
	})
	op.RequestData = chasm.NewDataField(nil, &nexusoperationpb.OperationRequestData{
		Input:       input,
		NexusHeader: headers,
	})

	cancellation := newCancellation(&nexusoperationpb.CancellationState{})
	cancellation.Operation = chasm.NewMockParentPtr(op)

	args, err := cancellation.loadArgs(&chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}, nil)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, input, args.payload)
	require.Equal(t, headers, args.headers)
}

func TestCancellationInvocationTaskHandler_HTTP(t *testing.T) {
	cases := []struct {
		name                  string
		header                map[string]string
		onCancelOperation     func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, c *Cancellation)
		requestTimeout        time.Duration
		schedToCloseTimeout   time.Duration
		startToCloseTimeout   time.Duration
		destinationDown       bool
		endpointNotFound      bool
	}{
		{
			name: "failure",
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					Message:       "operation not found",
					RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
				}
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation not found",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type:          string(nexus.HandlerErrorTypeInternal),
							RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
						},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name: "success",
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, c.Status)
				require.Nil(t, c.LastAttemptFailure)
			},
		},
		{
			name:   "success with headers",
			header: map[string]string{"key": "value"},
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				if options.Header["key"] != "value" {
					return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, `"key" header is not equal to "value"`)
				}
				return nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, c.Status)
				require.Nil(t, c.LastAttemptFailure)
			},
		},
		{
			name:            "transient error",
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "internal server error",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: string(nexus.HandlerErrorTypeInternal),
						},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:            "invocation timeout by request timeout",
			requestTimeout:  2 * time.Millisecond,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "request timed out",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:                "invocation timeout by ScheduleToCloseTimeout",
			schedToCloseTimeout: 10 * time.Millisecond,
			destinationDown:     true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "request timed out",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:                "invocation timeout by StartToCloseTimeout",
			startToCloseTimeout: 10 * time.Millisecond,
			destinationDown:     true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "request timed out",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:                  "operation timeout by ScheduleToCloseTimeout",
			schedToCloseTimeout:   time.Microsecond,
			onCancelOperation:     nil, // Should not be called if the operation has timed out.
			expectedMetricOutcome: "operation-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out before cancellation could be delivered",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
						},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:                  "operation timeout by StartToCloseTimeout",
			startToCloseTimeout:   time.Microsecond,
			onCancelOperation:     nil, // Should not be called if the operation has timed out.
			expectedMetricOutcome: "operation-timeout",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "operation timed out before cancellation could be delivered",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
						},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:              "endpoint not found",
			endpointNotFound:  true,
			requestTimeout:    time.Hour,
			onCancelOperation: nil, // Should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "endpoint not registered",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: string(nexus.HandlerErrorTypeNotFound),
						},
					},
				}, c.LastAttemptFailure)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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

			env := newCancellationTaskTestEnv(t, op, cancellation,
				InvocationData{
					Header: tc.header,
				},
				endpointReg, clientProvider, metricsHandler, cmp.Or(tc.requestTimeout, time.Hour))

			env.setupReadComponent()
			env.setupUpdateComponent()

			err := env.execute(&nexusoperationpb.CancellationTask{Attempt: 1})
			if tc.destinationDown {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}
			tc.checkOutcome(t, cancellation)

			if tc.expectedMetricOutcome != "" {
				snap := capture.Snapshot()
				counterRecordings := snap[OutboundRequestCounter.Name()]
				require.Len(t, counterRecordings, 1)
				require.Equal(t, int64(1), counterRecordings[0].Value)
				require.Equal(t, "ns-name", counterRecordings[0].Tags["namespace"])
				require.Equal(t, "endpoint", counterRecordings[0].Tags["destination"])
				require.Equal(t, "CancelOperation", counterRecordings[0].Tags["method"])
				require.Equal(t, tc.expectedMetricOutcome, counterRecordings[0].Tags["outcome"])
				require.Equal(t, "_unknown_", counterRecordings[0].Tags["failure_source"])

				timerRecordings := snap[OutboundRequestLatency.Name()]
				require.Len(t, timerRecordings, 1)
				require.Equal(t, tc.expectedMetricOutcome, timerRecordings[0].Tags["outcome"])
			}
		})
	}
}

// testCancelProcessor implements chasm.NexusOperationProcessor[string] for system endpoint tests.
type testCancelProcessor struct{}

func (p *testCancelProcessor) ProcessInput(
	_ chasm.NexusOperationProcessorContext,
	_ string,
) (*chasm.NexusOperationProcessorResult, error) {
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyRandom{},
	}, nil
}

func TestCancellationInvocationTaskHandler_SystemEndpoint(t *testing.T) {
	cases := []struct {
		name                  string
		setupHistoryClient    func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient
		registerProcessor     bool
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, c *Cancellation)
	}{
		{
			name:              "success",
			registerProcessor: true,
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any()).
					Return(&historyservice.CancelNexusOperationResponse{}, nil)
				return client
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, c.Status)
				require.Nil(t, c.LastAttemptFailure)
			},
		},
		{
			name:              "history service error - retryable",
			registerProcessor: true,
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any()).
					Return(nil, serviceerror.NewUnavailable("unavailable"))
				return client
			},
			expectedMetricOutcome: "service-error:Unavailable",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "Unavailable: unavailable",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:              "history service error - InvalidArgument",
			registerProcessor: true,
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				client := historyservicemock.NewMockHistoryServiceClient(ctrl)
				client.EXPECT().CancelNexusOperation(gomock.Any(), gomock.Any()).
					Return(nil, serviceerror.NewInvalidArgument("invalid"))
				return client
			},
			expectedMetricOutcome: "service-error:InvalidArgument",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: "InvalidArgument: invalid",
					FailureInfo: &failurepb.Failure_ServerFailureInfo{
						ServerFailureInfo: &failurepb.ServerFailureInfo{
							NonRetryable: true,
						},
					},
				}, c.LastAttemptFailure)
			},
		},
		{
			name:              "chasm processor error",
			registerProcessor: false,
			setupHistoryClient: func(ctrl *gomock.Controller) *historyservicemock.MockHistoryServiceClient {
				return historyservicemock.NewMockHistoryServiceClient(ctrl)
			},
			expectedMetricOutcome: "operation-processor-failed",
			checkOutcome: func(t *testing.T, c *Cancellation) {
				require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, c.Status)
				protorequire.ProtoEqual(t, &failurepb.Failure{
					Message: `service "service" not found`,
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
							Type: string(nexus.HandlerErrorTypeNotFound),
						},
					},
				}, c.LastAttemptFailure)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			op := &Operation{
				OperationState: &nexusoperationpb.OperationState{
					Status:         nexusoperationpb.OPERATION_STATUS_STARTED,
					Endpoint:       commonnexus.SystemEndpoint,
					Service:        "service",
					Operation:      "operation",
					ScheduledTime:  timestamppb.Now(),
					StartedTime:    timestamppb.Now(),
					RequestId:      "request-id",
					OperationToken: "op-token",
					Attempt:        1,
				},
			}
			cancellation := newCancellation(&nexusoperationpb.CancellationState{
				Status:  nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
				Attempt: 1,
			})

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			env := newCancellationTaskTestEnv(t, op, cancellation,
				InvocationData{Input: mustToPayload(t, "test")},
				nexustest.FakeEndpointRegistry{}, nil, metricsHandler, time.Hour)

			// Set up system endpoint dependencies.
			historyClient := tc.setupHistoryClient(env.ctrl)
			env.handler.historyClient = historyClient
			env.handler.config.NumHistoryShards = 4

			reg := chasm.NewRegistry(log.NewNoopLogger())
			if tc.registerProcessor {
				serviceProc := chasm.NewNexusServiceProcessor("service")
				serviceProc.MustRegisterOperation("operation",
					chasm.NewRegisterableNexusOperationProcessor(&testCancelProcessor{}))
				reg.NexusEndpointProcessor.MustRegisterServiceProcessor(serviceProc)
			}
			env.handler.chasmRegistry = reg

			env.setupReadComponent()
			env.setupUpdateComponent()

			err := env.execute(&nexusoperationpb.CancellationTask{Attempt: 1})
			require.NoError(t, err)

			tc.checkOutcome(t, cancellation)

			snap := capture.Snapshot()
			counterRecordings := snap[OutboundRequestCounter.Name()]
			require.Len(t, counterRecordings, 1)
			require.Equal(t, int64(1), counterRecordings[0].Value)
			require.Equal(t, "ns-name", counterRecordings[0].Tags["namespace"])
			require.Equal(t, commonnexus.SystemEndpoint, counterRecordings[0].Tags["destination"])
			require.Equal(t, "CancelOperation", counterRecordings[0].Tags["method"])
			require.Equal(t, tc.expectedMetricOutcome, counterRecordings[0].Tags["outcome"])

			timerRecordings := snap[OutboundRequestLatency.Name()]
			require.Len(t, timerRecordings, 1)
			require.Equal(t, tc.expectedMetricOutcome, timerRecordings[0].Tags["outcome"])
		})
	}
}
