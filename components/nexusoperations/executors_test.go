// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nexusoperations_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/queues"
)

func mustToPayload(t *testing.T, input any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(input)
	require.NoError(t, err)
	return payload
}

type handler struct {
	nexus.UnimplementedHandler
	OnStartOperation  func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
	OnCancelOperation func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error
}

func (h handler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return h.OnStartOperation(ctx, service, operation, input, options)
}

func (h handler) CancelOperation(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
	return h.OnCancelOperation(ctx, service, operation, operationID, options)
}

func TestProcessInvocationTask(t *testing.T) {
	cases := []struct {
		name                  string
		endpointNotFound      bool
		onStartOperation      func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent)
		requestTimeout        time.Duration
		destinationDown       bool
	}{
		{
			name:            "async start",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationID: "op-id"}, nil
			},
			expectedMetricOutcome: "pending",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_STARTED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, events[0].EventType)
				protorequire.ProtoEqual(t, &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: 1,
					OperationId:      "op-id",
				}, events[0].GetNexusOperationStartedEventAttributes())
			},
		},
		{
			name:            "sync start",
			requestTimeout:  time.Hour,
			destinationDown: false,
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
				}
				protorequire.ProtoEqual(t, attrs, events[0].GetNexusOperationCompletedEventAttributes())
			},
		},
		{
			name:            "sync failed",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.UnsuccessfulOperationError{
					Failure: nexus.Failure{Message: "operation failed from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					State:   nexus.OperationStateFailed,
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:failed",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, events[0].EventType)
				attrs := &historypb.NexusOperationFailedEventAttributes{
					ScheduledEventId: 1,
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
									Type: "NexusOperationFailure",
									Details: &commonpb.Payloads{
										Payloads: []*commonpb.Payload{
											mustToPayload(t, "details"),
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
				return nil, &nexus.UnsuccessfulOperationError{
					Failure: nexus.Failure{Message: "operation canceled from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					State:   nexus.OperationStateCanceled,
				}
			},
			expectedMetricOutcome: "operation-unsuccessful:canceled",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_CANCELED, op.State())
				require.Equal(t, 1, len(events))
				require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, events[0].EventType)
				attrs := &historypb.NexusOperationCanceledEventAttributes{
					ScheduledEventId: 1,
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
											mustToPayload(t, "details"),
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
			expectedMetricOutcome: "request-error:500",
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "unexpected response status: \"500 Internal Server Error\": internal server error", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:                  "invocation timeout",
			requestTimeout:        time.Microsecond,
			destinationDown:       true,
			expectedMetricOutcome: "request-timeout",
			onStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				time.Sleep(time.Millisecond * 100)
				return &nexus.HandlerStartOperationResultAsync{OperationID: "op-id"}, nil
			},
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "Post \"http://localhost:\\d+/service/operation\\?callback=http%3A%2F%2Flocalhost%2Fcallback\": context deadline exceeded", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:             "service not found",
			endpointNotFound: true,
			requestTimeout:   time.Hour,
			destinationDown:  false,
			onStartOperation: nil, // This should not be called if the service is not found.
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_FAILED, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "endpoint not registered", op.LastAttemptFailure.Message)
				require.Equal(t, 1, len(events))
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress(t)
			h := handler{}
			h.OnStartOperation = tc.onStartOperation
			nexustest.NewNexusServer(t, listenAddr, h)

			reg := newRegistry(t)
			backend := &hsmtest.NodeBackend{Events: []*historypb.HistoryEvent{mustNewScheduledEvent(time.Now(), time.Hour)}}
			node := newOperationNode(t, backend, backend.Events[0])
			env := fakeEnv{node}
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
				namespace.NewNamespaceForTest(&persistence.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

			metricsHandler := metrics.NewMockHandler(ctrl)
			if tc.expectedMetricOutcome != "" {
				counter := metrics.NewMockCounterIface(ctrl)
				timer := metrics.NewMockTimerIface(ctrl)
				metricsHandler.EXPECT().Counter(nexusoperations.OutboundRequestCounter.Name()).Return(counter)
				counter.EXPECT().Record(int64(1),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint-name"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.NexusOutcomeTag(tc.expectedMetricOutcome))
				metricsHandler.EXPECT().Timer(nexusoperations.OutboundRequestLatencyHistogram.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint-name"),
					metrics.NexusMethodTag("StartOperation"),
					metrics.NexusOutcomeTag(tc.expectedMetricOutcome))
			}

			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByID: func(ctx context.Context, endpointID string) (*persistence.NexusEndpointEntry, error) {
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return nil, nil // The endpoint isn't used here, it's okay to return nil.
				},
			}
			require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
				Config: &nexusoperations.Config{
					Enabled:             dynamicconfig.GetBoolPropertyFn(true),
					RequestTimeout:      dynamicconfig.GetDurationPropertyFnFilteredByDestination(tc.requestTimeout),
					PayloadSizeLimit:    dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024),
					CallbackURLTemplate: dynamicconfig.GetStringPropertyFn("http://localhost/callback"),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				CallbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
				NamespaceRegistry:      namespaceRegistry,
				MetricsHandler:         metricsHandler,
				Logger:                 log.NewNoopLogger(),
				EndpointRegistry:       endpointReg,
				ClientProvider: func(ctx context.Context, nid queues.NamespaceIDAndDestination, service string) (*nexus.Client, error) {
					return nexus.NewClient(nexus.ClientOptions{
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
					StateMachineRef: &persistence.StateMachineRef{},
				},
				nexusoperations.InvocationTask{Destination: "endpoint-name"},
			)
			if tc.destinationDown {
				var destinationDownErr *queues.DestinationDownError
				require.ErrorAs(t, err, &destinationDownErr)
			} else {
				require.NoError(t, err)
			}
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			tc.checkOutcome(t, op, backend.Events[1:]) // Ignore the original scheduled event.
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
			Err:         errors.New("test"),
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
		onCancelOperation     func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error
		expectedMetricOutcome string
		checkOutcome          func(t *testing.T, op nexusoperations.Cancelation)
		requestTimeout        time.Duration
		destinationDown       bool
	}{
		{
			name:            "failure",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onCancelOperation: func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
				return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "operation not found")
			},
			expectedMetricOutcome: "request-error:404",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "unexpected response status: \"404 Not Found\": operation not found", c.LastAttemptFailure.Message)
			},
		},
		{
			name:            "success",
			requestTimeout:  time.Hour,
			destinationDown: false,
			onCancelOperation: func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
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
			onCancelOperation: func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
				return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			expectedMetricOutcome: "request-error:500",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "unexpected response status: \"500 Internal Server Error\": internal server error", c.LastAttemptFailure.Message)
			},
		},
		{
			name:            "invocation timeout",
			requestTimeout:  time.Microsecond,
			destinationDown: true,
			onCancelOperation: func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
				time.Sleep(time.Millisecond * 100)
				return nil
			},
			expectedMetricOutcome: "request-timeout",
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "Post \"http://localhost:\\d+/service/operation/op-id/cancel\": context deadline exceeded", c.LastAttemptFailure.Message)
			},
		},
		{
			name:              "service not found",
			endpointNotFound:  true,
			requestTimeout:    time.Hour,
			destinationDown:   false,
			onCancelOperation: nil, // This should not be called if the endpoint is not found.
			checkOutcome: func(t *testing.T, c nexusoperations.Cancelation) {
				require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, c.State())
				require.NotNil(t, c.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "endpoint not registered", c.LastAttemptFailure.Message)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			listenAddr := nexustest.AllocListenAddress(t)
			h := handler{}
			h.OnCancelOperation = tc.onCancelOperation
			nexustest.NewNexusServer(t, listenAddr, h)

			reg := newRegistry(t)
			backend := &hsmtest.NodeBackend{}
			node := newOperationNode(t, backend, mustNewScheduledEvent(time.Now(), time.Hour))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			_, err = nexusoperations.TransitionStarted.Apply(op, nexusoperations.EventStarted{
				Time: time.Now(),
				Attributes: &historypb.NexusOperationStartedEventAttributes{
					OperationId: "op-id",
				},
				Node: node,
			})
			require.NoError(t, err)
			_, err = op.Cancel(node, time.Now())
			require.NoError(t, err)
			node, err = node.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
			require.NoError(t, err)

			env := fakeEnv{node}
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
				namespace.NewNamespaceForTest(&persistence.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

			metricsHandler := metrics.NewMockHandler(ctrl)
			if tc.expectedMetricOutcome != "" {
				counter := metrics.NewMockCounterIface(ctrl)
				timer := metrics.NewMockTimerIface(ctrl)
				metricsHandler.EXPECT().Counter(nexusoperations.OutboundRequestCounter.Name()).Return(counter)
				counter.EXPECT().Record(int64(1),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint-name"),
					metrics.NexusMethodTag("CancelOperation"),
					metrics.NexusOutcomeTag(tc.expectedMetricOutcome))
				metricsHandler.EXPECT().Timer(nexusoperations.OutboundRequestLatencyHistogram.Name()).Return(timer)
				timer.EXPECT().Record(gomock.Any(),
					metrics.NamespaceTag("ns-name"),
					metrics.DestinationTag("endpoint-name"),
					metrics.NexusMethodTag("CancelOperation"),
					metrics.NexusOutcomeTag(tc.expectedMetricOutcome))
			}
			endpointReg := nexustest.FakeEndpointRegistry{
				OnGetByID: func(ctx context.Context, endpointID string) (*persistence.NexusEndpointEntry, error) {
					if tc.endpointNotFound {
						return nil, serviceerror.NewNotFound("endpoint not found")
					}
					return nil, nil // The endpoint isn't used here, it's okay to return nil.
				},
			}

			require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.TaskExecutorOptions{
				Config: &nexusoperations.Config{
					Enabled:        dynamicconfig.GetBoolPropertyFn(true),
					RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(tc.requestTimeout),
					RetryPolicy: func() backoff.RetryPolicy {
						return backoff.NewExponentialRetryPolicy(time.Second)
					},
				},
				NamespaceRegistry: namespaceRegistry,
				MetricsHandler:    metricsHandler,
				Logger:            log.NewNoopLogger(),
				EndpointRegistry:  endpointReg,
				ClientProvider: func(ctx context.Context, nid queues.NamespaceIDAndDestination, service string) (*nexus.Client, error) {
					return nexus.NewClient(nexus.ClientOptions{
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
					StateMachineRef: &persistence.StateMachineRef{},
				},
				nexusoperations.CancelationTask{Destination: "endpoint-name"},
			)
			if tc.destinationDown {
				require.IsType(t, &queues.DestinationDownError{}, err)
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
			OperationId: "op-id",
		},
		Node: node,
	})
	require.NoError(t, err)
	_, err = op.Cancel(node, time.Now())
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
		namespace.NewNamespaceForTest(&persistence.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)

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
			OnGetByID: func(ctx context.Context, endpointID string) (*persistence.NexusEndpointEntry, error) {
				return nil, nil // The endpoint isn't used here, it's okay to return nil.
			},
		},
		ClientProvider: func(ctx context.Context, nid queues.NamespaceIDAndDestination, service string) (*nexus.Client, error) {
			return nil, serviceerror.NewInternal("shouldn't get here")
		},
	}))

	err = reg.ExecuteImmediateTask(
		context.Background(),
		env,
		hsm.Ref{
			WorkflowKey:     definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
			StateMachineRef: &persistence.StateMachineRef{},
		},
		nexusoperations.CancelationTask{Destination: "endpoint-name"},
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
			OperationId: "op-id",
		},
		Node: node,
	})
	require.NoError(t, err)
	_, err = op.Cancel(node, time.Now())
	require.NoError(t, err)

	node, err = node.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
	require.NoError(t, err)

	err = hsm.MachineTransition(node, func(c nexusoperations.Cancelation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionCancelationAttemptFailed.Apply(c, nexusoperations.EventCancelationAttemptFailed{
			Time:        time.Now(),
			Err:         fmt.Errorf("test attempt failed"),
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
