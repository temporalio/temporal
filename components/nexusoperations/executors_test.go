package nexusoperations_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
)

var endpointEntry = &persistencespb.NexusEndpointEntry{
	Id: "enpdoint-id",
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

func TestProcessInvocationTask(t *testing.T) {
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
	handlerNexusLink := nexusoperations.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	cases := []struct {
		name                       string
		endpointNotFound           bool
		eventHasNoEndpointID       bool
		cancelBeforeStart          bool
		header                     nexus.Header
		checkStartOperationOptions func(t *testing.T, options nexus.StartOperationOptions)
		onStartOperation           func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		expectedMetricOutcome      string
		checkOutcome               func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent)
		requestTimeout             time.Duration
		schedToCloseTimeout        time.Duration
		destinationDown            bool
	}{
		{
			name:            "async start",
			requestTimeout:  time.Hour,
			destinationDown: false,
			checkStartOperationOptions: func(t *testing.T, options nexus.StartOperationOptions) {
				require.Len(t, options.Links, 1)
				var links []*commonpb.Link
				for _, nexusLink := range options.Links {
					link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
					require.NoError(t, err)
					links = append(links, &commonpb.Link{
						Variant: &commonpb.Link_WorkflowEvent_{
							WorkflowEvent: link,
						},
					})
				}
				require.NotNil(t, links[0].GetWorkflowEvent())
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
				}, links[0].GetWorkflowEvent())
			},
			onStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				nexus.AddHandlerLinks(ctx, handlerNexusLink)
				return &nexus.HandlerStartOperationResultAsync{
					OperationToken: "op-token",
				}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, events[0].EventType)
				protorequire.ProtoEqual(t, &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: 1,
					OperationToken:   "op-token",
					OperationId:      "op-token",
					RequestId:        op.RequestId,
				}, events[0].GetNexusOperationStartedEventAttributes())
				require.Len(t, events[0].Links, 1)
				protorequire.ProtoEqual(t, handlerLink, events[0].Links[0].GetWorkflowEvent())
			},
		},
		{
			name:                "sync start",
			requestTimeout:      time.Hour,
			schedToCloseTimeout: time.Hour,
			header:              nexus.Header{nexus.HeaderOperationTimeout: time.Microsecond.String()}, // to test this value is ignored when ScheduleToCloseTimeout is set
			destinationDown:     false,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				// Also use this test case to check the input and options provided.
				if service != "service" {
					return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation name")
				}
				if operation != "operation" {
					return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation name")
				}
				if options.CallbackHeader.Get("temporal-callback-token") == "" {
					return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "empty callback token")
				}
				if options.CallbackURL != "http://localhost/callback" {
					return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback URL")
				}
				var v string
				if err := input.Consume(&v); err != nil || v != "input" {
					return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
				}
				return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, events[0].EventType)
				attrs := &historypb.NexusOperationCompletedEventAttributes{
					ScheduledEventId: 1,
					Result:           mustToPayload(t, "result"),
					RequestId:        op.RequestId,
				}
				protorequire.ProtoEqual(t, attrs, events[0].GetNexusOperationCompletedEventAttributes())
			},
		},
		{
			name:            "sync failed",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateFailed,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "operation failed from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					},
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:failed",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, events[0].EventType)
				attrs := &historypb.NexusOperationFailedEventAttributes{
					ScheduledEventId: 1,
					RequestId:        op.RequestId,
					Failure: &failurepb.Failure{
						Message: "nexus operation completed unsuccessfully",
						FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
							NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
								ScheduledEventId: 1,
								Endpoint:         "endpoint",
								Service:          "service",
								Operation:        "operation",
							},
						},
						Cause: &failurepb.Failure{
							Message: "operation failed from handler",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
								ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
									Type: "NexusFailure",
									Details: &commonpb.Payloads{
										Payloads: []*commonpb.Payload{
											mustToPayload(t, nexus.Failure{Metadata: map[string]string{"encoding": "json/plain"}, Details: []byte(`"details"`)}),
										},
									},
									NonRetryable: true,
								},
							},
						},
					},
				}
				protorequire.ProtoEqual(t, attrs, events[0].GetNexusOperationFailedEventAttributes())
			},
		},
		{
			name:            "sync canceled",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateCanceled,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "operation canceled from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					},
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:canceled",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_CANCELED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, events[0].EventType)
				attrs := &historypb.NexusOperationCanceledEventAttributes{
					ScheduledEventId: 1,
					RequestId:        op.RequestId,
					Failure: &failurepb.Failure{
						Message: "nexus operation completed unsuccessfully",
						FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
							NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
								ScheduledEventId: 1,
								Endpoint:         "endpoint",
								Service:          "service",
								Operation:        "operation",
							},
						},
						Cause: &failurepb.Failure{
							Message: "operation canceled from handler",
							FailureInfo: &failurepb.Failure_CanceledFailureInfo{
								CanceledFailureInfo: &failurepb.CanceledFailureInfo{
									Details: &commonpb.Payloads{
										Payloads: []*commonpb.Payload{
											mustToPayload(t, nexus.Failure{Metadata: map[string]string{"encoding": "json/plain"}, Details: []byte(`"details"`)}),
										},
									},
								},
							},
						},
					},
				}
				protorequire.ProtoEqual(t, attrs, events[0].GetNexusOperationCanceledEventAttributes())
			},
		},
		{
			name:            "transient error",
			requestTimeout:  time.Hour,
			destinationDown: true,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (INTERNAL): internal server error", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:                  "invocation timeout by request timeout",
			requestTimeout:        2 * time.Millisecond,
			schedToCloseTimeout:   time.Hour,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				time.Sleep(time.Millisecond * 100)
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "request timed out", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:                  "invocation timeout by ScheduleToCloseTimeout",
			requestTimeout:        time.Hour,
			schedToCloseTimeout:   10 * time.Millisecond,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "op-token"}, nil
			},
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "request timed out", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:                  "ScheduleToCloseTimeout less than MinRequestTimeout",
			requestTimeout:        time.Hour,
			schedToCloseTimeout:   time.Microsecond,
			destinationDown:       false,
			expectedMetricOutcome: "operation-timeout",
			onStartOperation:      nil, // This should not be called if the operation has timed out.
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT, op.State())
				require.Equal(t, 1, len(events))
				failure := events[0].GetNexusOperationTimedOutEventAttributes().Failure.Cause
				require.NotNil(t, failure.GetTimeoutFailureInfo())
				require.Equal(t, "operation timed out", failure.Message)
			},
		},
		{
			name:             "endpoint not found",
			endpointNotFound: true,
			requestTimeout:   time.Hour,
			destinationDown:  false,
			onStartOperation: nil, // This should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.Equal(t, 1, len(events))
				failure := events[0].GetNexusOperationFailedEventAttributes().Failure.Cause
				require.NotNil(t, failure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (NOT_FOUND): endpoint not registered", failure.Message)
			},
		},
		{
			name:                 "endpoint not found on command processing",
			eventHasNoEndpointID: true,
			requestTimeout:       time.Hour,
			destinationDown:      false,
			onStartOperation:     nil, // This should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.Equal(t, 1, len(events))
				failure := events[0].GetNexusOperationFailedEventAttributes().Failure.Cause
				require.NotNil(t, failure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (NOT_FOUND): endpoint not registered", failure.Message)
				require.NotNil(t, failure.Cause.GetApplicationFailureInfo())
				require.Equal(t, "endpoint not registered", failure.Cause.Message)
			},
		},
		{
			name:              "cancel before start",
			cancelBeforeStart: true,
			requestTimeout:    time.Hour,
			destinationDown:   false,
			onStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				nexus.AddHandlerLinks(ctx, handlerNexusLink)
				return &nexus.HandlerStartOperationResultAsync{
					OperationToken: "op-token",
				}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
				require.Nil(t, op.LastAttemptFailure)
				require.Equal(t, 1, len(events))
			},
		},
		{
			name:            "token to long",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onStartOperation: func(
				ctx context.Context,
				service, operation string,
				input *nexus.LazyValue,
				options nexus.StartOperationOptions,
			) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: "12345678901"}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.Equal(t, 1, len(events))
				failure := events[0].GetNexusOperationFailedEventAttributes().Failure.Cause
				require.NotNil(t, failure.GetApplicationFailureInfo())
				require.Equal(t, "invalid operation token: length exceeds allowed limit (11/10)", failure.Message)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress()
			h := nexustest.Handler{}
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
			nexustest.NewNexusServer(t, listenAddr, h)

			reg := newRegistry(t)
			event := mustNewScheduledEvent(time.Now(), tc.schedToCloseTimeout)
			if tc.eventHasNoEndpointID {
				event.GetNexusOperationScheduledEventAttributes().EndpointId = ""
			}
			if tc.header != nil {
				event.GetNexusOperationScheduledEventAttributes().NexusHeader = tc.header
			}
			backend := &hsmtest.NodeBackend{Events: []*historypb.HistoryEvent{event}}
			node := newOperationNode(t, backend, backend.Events[0])
			env := fakeEnv{node}
			if tc.cancelBeforeStart {
				op, err := hsm.MachineData[nexusoperations.Operation](node)
				require.NoError(t, err)
				_, err = op.Cancel(node, time.Now(), 0)
				require.NoError(t, err)
				c, err := op.Cancelation(node)
				require.NoError(t, err)
				require.NotNil(t, c)
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED, c.State())
			}
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
				namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

			metricsHandler := metrics.NewMockHandler(ctrl)
			if tc.expectedMetricOutcome != "" {
				counter := metrics.NewMockCounterIface(ctrl)
				timer := metrics.NewMockTimerIface(ctrl)
				metricsHandler.EXPECT().Counter(nexusoperations.OutboundRequestCounter.Name()).Return(counter)
				counter.EXPECT().Record(int64(1),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
				metricsHandler.EXPECT().Timer(nexusoperations.OutboundRequestLatency.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
			}

			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByID: func(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
					require.Equal(t, "endpoint-id", endpointID)
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
				OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
					require.Equal(t, "endpoint", endpointName)
					require.Equal(t, "ns-id", namespaceID.String())
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
			}
			require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
				Config: &nexusoperations.Config{
					Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
					RequestTimeout:          dynamicconfig.GetDurationPropertyFnFilteredByDestination(tc.requestTimeout),
					MaxOperationTokenLength: dynamicconfig.GetIntPropertyFnFilteredByNamespace(10),
					MinRequestTimeout:       dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Millisecond),
					PayloadSizeLimit:        dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024),
					CallbackURLTemplate:     dynamicconfig.GetStringPropertyFn("http://localhost/callback"),
					UseSystemCallbackURL:    dynamicconfig.GetBoolPropertyFn(true),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				CallbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
				NamespaceRegistry:      namespaceRegistry,
				MetricsHandler:         metricsHandler,
				Logger:                 log.NewNoopLogger(),
				EndpointRegistry:       endpointReg,
				ClientProvider: func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
					return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
						BaseURL:    "http://" + listenAddr,
						Service:    service,
						Serializer: commonnexus.PayloadSerializer,
					})
				},
			}))

			err := reg.ExecuteImmediateTask(
				context.Background(),
				env,
				hsm.Ref{
					WorkflowKey:     definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
					StateMachineRef: &persistencespb.StateMachineRef{},
				},
				nexusoperations.InvocationTask{EndpointName: "endpoint-id"},
			)
			if tc.destinationDown {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			tc.checkOutcome(t, op, backend.Events[1:]) // Ignore the original scheduled event.
			if tc.cancelBeforeStart {
				c, err := op.Cancelation(node)
				require.NoError(t, err)
				require.NotNil(t, c)
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, c.State())
			}
		})
	}
}

func TestProcessBackoffTask(t *testing.T) {
	reg := newRegistry(t)
	backend := &hsmtest.NodeBackend{}
	node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
	env := fakeEnv{node}

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{}))
	err := hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionAttemptFailed.Apply(op, nexusoperations.EventAttemptFailed{
			Node:        node,
			Time:        time.Now(),
			Failure:     &failurepb.Failure{Message: "test"},
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		})
	})
	require.NoError(t, err)

	err = reg.ExecuteTimerTask(
		env,
		node,
		nexusoperations.BackoffTask{},
	)
	require.NoError(t, err)
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.State())
	require.Equal(t, 0, len(backend.Events))
}

func TestProcessTimeoutTask(t *testing.T) {
	reg := newRegistry(t)
	backend := &hsmtest.NodeBackend{}
	node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
	env := fakeEnv{node}

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{}))

	err := reg.ExecuteTimerTask(
		env,
		node,
		nexusoperations.TimeoutTask{},
	)
	require.NoError(t, err)
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT, op.State())
	require.Equal(t, 1, len(backend.Events))
	require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, backend.Events[0].EventType)
	protorequire.ProtoEqual(t, &historypb.NexusOperationTimedOutEventAttributes{
		ScheduledEventId: 1,
		RequestId:        op.RequestId,
		Failure: &failurepb.Failure{
			Message: "nexus operation completed unsuccessfully",
			FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
				NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
					ScheduledEventId: 1,
					Endpoint:         "endpoint",
					Service:          "service",
					Operation:        "operation",
				},
			},
			Cause: &failurepb.Failure{
				Message: "operation timed out",
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
					TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
						TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
					},
				},
			},
		},
	}, backend.Events[0].GetNexusOperationTimedOutEventAttributes())
}

func TestProcessCancelationTask(t *testing.T) {
	cases := []struct {
		name                  string
		endpointNotFound      bool
		onCancelOperation     func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, op nexusoperations.Cancelation)
		requestTimeout        time.Duration
		schedToCloseTimeout   time.Duration
		destinationDown       bool
		header                map[string]string
	}{
		{
			name:            "failure",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				// Check non retryable internal error.
				return &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					Cause:         errors.New("operation not found"),
					RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
				}
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (INTERNAL): operation not found", c.LastAttemptFailure.Message)
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
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, c.State())
				require.Nil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
			},
		},
		{
			name:            "success with headers",
			requestTimeout:  time.Hour,
			destinationDown: false,
			header:          map[string]string{"key": "value"},
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				if options.Header["key"] != "value" {
					return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, `"key" header is not equal to "value"`)
				}
				return nil
			},
			expectedMetricOutcome: "successful",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, c.State())
				require.Nil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
			},
		},
		{
			name:            "transient error",
			requestTimeout:  time.Hour,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "handler-error:INTERNAL",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (INTERNAL): internal server error", c.LastAttemptFailure.Message)
			},
		},
		{
			name:            "invocation timeout",
			requestTimeout:  10 * time.Millisecond,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "Post \"http://localhost:\\d+/service/operation/cancel\": context deadline exceeded", c.LastAttemptFailure.Message)
			},
		},
		{
			name:                  "operation timeout",
			requestTimeout:        time.Hour,
			schedToCloseTimeout:   time.Microsecond,
			destinationDown:       false,
			onCancelOperation:     nil, // This should not be called if the operation has timed out.
			expectedMetricOutcome: "operation-timeout",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, nexusoperations.ErrOperationTimeoutBelowMin.Error(), c.LastAttemptFailure.Message)
			},
		},
		{
			name:              "endpoint not found",
			endpointNotFound:  true,
			requestTimeout:    time.Hour,
			destinationDown:   false,
			onCancelOperation: nil, // This should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetNexusHandlerFailureInfo())
				require.Equal(t, "handler error (NOT_FOUND): endpoint not registered", c.LastAttemptFailure.Message)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress()
			h := nexustest.Handler{}
			h.OnCancelOperation = tc.onCancelOperation
			nexustest.NewNexusServer(t, listenAddr, h)

			reg := newRegistry(t)
			event := mustNewScheduledEvent(time.Now(), tc.schedToCloseTimeout)
			if tc.header != nil {
				event.GetNexusOperationScheduledEventAttributes().NexusHeader = tc.header
			}
			backend := &hsmtest.NodeBackend{Events: []*historypb.HistoryEvent{event}}
			node := newOperationNode(t, backend, backend.Events[0])
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			_, err = nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
				Time: time.Now(),
				Attributes: &historypb.NexusOperationStartedEventAttributes{
					OperationToken: "op-token",
				},
				Node: node,
			})
			require.NoError(t, err)
			_, err = op.Cancel(node, time.Now(), 0)
			require.NoError(t, err)
			node, err = node.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
			require.NoError(t, err)

			env := fakeEnv{node}
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
				namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

			metricsHandler := metrics.NewMockHandler(ctrl)
			if tc.expectedMetricOutcome != "" {
				counter := metrics.NewMockCounterIface(ctrl)
				timer := metrics.NewMockTimerIface(ctrl)
				metricsHandler.EXPECT().Counter(nexusoperations.OutboundRequestCounter.Name()).Return(counter)
				counter.EXPECT().Record(int64(1),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("CancelOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
				metricsHandler.EXPECT().Timer(nexusoperations.OutboundRequestLatency.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint"),
					metrics.NexusMethodTag("CancelOperation"),
					metrics.OutcomeTag(tc.expectedMetricOutcome),
					metrics.FailureSourceTag("_unknown_"))
			}
			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByID: func(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
					require.Equal(t, "endpoint-id", endpointID)
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
				OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
					require.Equal(t, "endpoint", endpointName)
					require.Equal(t, "ns-id", namespaceID.String())
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return endpointEntry, nil
				},
			}

			require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
				Config: &nexusoperations.Config{
					Enabled:                             dynamicconfig.GetBoolPropertyFn(true),
					RequestTimeout:                      dynamicconfig.GetDurationPropertyFnFilteredByDestination(tc.requestTimeout),
					MinRequestTimeout:                   dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Millisecond),
					RecordCancelRequestCompletionEvents: dynamicconfig.GetBoolPropertyFn(true),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				NamespaceRegistry: namespaceRegistry,
				MetricsHandler:    metricsHandler,
				Logger:            log.NewNoopLogger(),
				EndpointRegistry:  endpointReg,
				ClientProvider: func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
					return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
						BaseURL:    "http://" + listenAddr,
						Service:    service,
						Serializer: commonnexus.PayloadSerializer,
					})
				},
			}))

			err = reg.ExecuteImmediateTask(
				context.Background(),
				env,
				hsm.Ref{
					WorkflowKey:     definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
					StateMachineRef: &persistencespb.StateMachineRef{},
				},
				nexusoperations.CancelationTask{EndpointName: "endpoint-id"},
			)
			if tc.destinationDown {
				var destinationDownErr *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}
			cancelation, err := hsm.MachineData[nexusoperations.Cancelation](node)
			require.NoError(t, err)
			tc.checkOutcome(t, cancelation)
		})
	}
}

func TestProcessCancelationTask_OperationCompleted(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	reg := newRegistry(t)
	backend := &hsmtest.NodeBackend{}
	node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	_, err = nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
		Time: time.Now(),
		Attributes: &historypb.NexusOperationStartedEventAttributes{
			OperationToken: "op-token",
		},
		Node: node,
	})
	require.NoError(t, err)
	_, err = op.Cancel(node, time.Now(), 0)
	require.NoError(t, err)
	_, err = nexusoperations.TransitionSucceeded.Apply(op, nexusoperations.EventSucceeded{
		Node: node,
	})
	require.NoError(t, err)
	node, err = node.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
	require.NoError(t, err)

	env := fakeEnv{node}
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
		namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
		Config: &nexusoperations.Config{
			Enabled:        dynamicconfig.GetBoolPropertyFn(true),
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Hour),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		NamespaceRegistry: namespaceRegistry,
		EndpointRegistry: nexustest.FakeEndpointRegistry{
			OnGetByID: func(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
				return endpointEntry, nil
			},
		},
		ClientProvider: func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
			return nil, serviceerror.NewInternal("shouldn't get here")
		},
	}))

	err = reg.ExecuteImmediateTask(
		context.Background(),
		env,
		hsm.Ref{
			WorkflowKey:     definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
			StateMachineRef: &persistencespb.StateMachineRef{},
		},
		nexusoperations.CancelationTask{EndpointName: "endpoint-name"},
	)
	require.ErrorIs(t, err, consts.ErrStaleReference)
}

func TestProcessCancelationBackoffTask(t *testing.T) {
	reg := newRegistry(t)
	backend := &hsmtest.NodeBackend{}
	node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	_, err = nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
		Time: time.Now(),
		Attributes: &historypb.NexusOperationStartedEventAttributes{
			OperationToken: "op-token",
		},
		Node: node,
	})
	require.NoError(t, err)
	_, err = op.Cancel(node, time.Now(), 0)
	require.NoError(t, err)

	node, err = node.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
	require.NoError(t, err)

	err = hsm.MachineTransition(node, func(c nexusoperations.Cancelation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionCancelationAttemptFailed.Apply(c, nexusoperations.EventCancelationAttemptFailed{
			Time:        time.Now(),
			Failure:     &failurepb.Failure{Message: "test attempt failed"},
			Node:        node,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		})
	})
	require.NoError(t, err)

	env := fakeEnv{node}

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{}))

	err = reg.ExecuteTimerTask(
		env,
		node,
		nexusoperations.CancelationBackoffTask{},
	)
	require.NoError(t, err)
	c, err := hsm.MachineData[nexusoperations.Cancelation](node)
	require.NoError(t, err)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, c.State())
	require.Equal(t, 0, len(backend.Events))
}
