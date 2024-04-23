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
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/converter"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
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
	OnStartOperation func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
}

func (h handler) StartOperation(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return h.OnStartOperation(ctx, operation, input, options)
}

func TestProcessInvocationTask(t *testing.T) {
	cases := []struct {
		name             string
		onStartOperation func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
		checkOutcome     func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent)
		taskTimeout      time.Duration
	}{
		{
			name:        "async start",
			taskTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationID: "op-id"}, nil
			},
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
			name:        "sync start",
			taskTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				// Also use this test case to check the input and options provided.
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
			name:        "sync failed",
			taskTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.UnsuccessfulOperationError{
					Failure: nexus.Failure{Message: "operation failed from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					State:   nexus.OperationStateFailed,
				}
			},
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
			name:        "sync canceled",
			taskTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, &nexus.UnsuccessfulOperationError{
					Failure: nexus.Failure{Message: "operation canceled from handler", Metadata: map[string]string{"encoding": "json/plain"}, Details: json.RawMessage("\"details\"")},
					State:   nexus.OperationStateCanceled,
				}
			},
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
			name:        "transient error",
			taskTimeout: time.Hour,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
			},
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Equal(t, "unexpected response status: \"500 Internal Server Error\": internal server error", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
			},
		},
		{
			name:        "invocation timeout",
			taskTimeout: time.Microsecond,
			onStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				time.Sleep(time.Millisecond * 100)
				return &nexus.HandlerStartOperationResultAsync{OperationID: "op-id"}, nil
			},
			checkOutcome: func(t *testing.T, op nexusoperations.Operation, events []*historypb.HistoryEvent) {
				require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF, op.State())
				require.NotNil(t, op.LastAttemptFailure.GetApplicationFailureInfo())
				require.Regexp(t, "Post \"http://localhost:\\d+/operation\\?callback=http%3A%2F%2Flocalhost%2Fcallback\": context deadline exceeded", op.LastAttemptFailure.Message)
				require.Equal(t, 0, len(events))
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
			backend := &nodeBackend{}
			node := newOperationNode(t, backend, time.Now(), time.Hour)
			env := fakeEnv{node}
			namespaceRegistry := namespace.NewMockRegistry(ctrl)
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(
				namespace.NewNamespaceForTest(&persistence.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0), nil)
			namespaceRegistry.EXPECT().NexusOutgoingService(namespace.ID("ns-id"), "svc-name").Return(
				&nexuspb.OutgoingServiceSpec{Url: fmt.Sprintf("http://%s", listenAddr), PublicCallbackUrl: "http://localhost/callback"}, nil)

			require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.ActiveExecutorOptions{
				Config: &nexusoperations.Config{
					Enabled:               dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
					InvocationTaskTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(tc.taskTimeout),
				},
				CallbackTokenGenerator: commonnexus.NewCallbackTokenGenerator(),
				NamespaceRegistry:      namespaceRegistry,
				ClientProvider: func(nid queues.NamespaceIDAndDestination, spec *nexuspb.OutgoingServiceSpec) (*nexus.Client, error) {
					return nexus.NewClient(nexus.ClientOptions{
						ServiceBaseURL: spec.Url,
						Serializer:     commonnexus.PayloadSerializer,
					})
				},
			}))

			err := hsm.Execute(context.Background(), reg, env, hsm.Ref{WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"), StateMachineRef: &persistence.StateMachineRef{}}, nexusoperations.InvocationTask{Destination: "svc-name"})
			require.NoError(t, err)
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			tc.checkOutcome(t, op, backend.events)
		})
	}
}

func TestProcessBackoffTask(t *testing.T) {
	reg := newRegistry(t)
	backend := &nodeBackend{}
	node := newOperationNode(t, backend, time.Now(), time.Hour)
	env := fakeEnv{node}

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.ActiveExecutorOptions{}))
	err := hsm.MachineTransition(node, func(op nexusoperations.Operation) (hsm.TransitionOutput, error) {
		return nexusoperations.TransitionAttemptFailed.Apply(op, nexusoperations.EventAttemptFailed{
			Node: node,
			AttemptFailure: nexusoperations.AttemptFailure{
				Time: time.Now(),
				Err:  errors.New("test"),
			},
		})
	})
	require.NoError(t, err)

	err = hsm.Execute(context.Background(), reg, env, hsm.Ref{}, nexusoperations.BackoffTask{})
	require.NoError(t, err)
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.State())
	require.Equal(t, 0, len(backend.events))
}

func TestProcessTimeoutTask(t *testing.T) {
	reg := newRegistry(t)
	backend := &nodeBackend{}
	node := newOperationNode(t, backend, time.Now(), time.Hour)
	env := fakeEnv{node}

	require.NoError(t, nexusoperations.RegisterExecutor(reg, nexusoperations.ActiveExecutorOptions{}))

	err := hsm.Execute(context.Background(), reg, env, hsm.Ref{}, nexusoperations.TimeoutTask{})
	require.NoError(t, err)
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT, op.State())
	require.Equal(t, 1, len(backend.events))
	require.Equal(t, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, backend.events[0].EventType)
	protorequire.ProtoEqual(t, &historypb.NexusOperationTimedOutEventAttributes{
		ScheduledEventId: 1,
		Failure: &failurepb.Failure{
			Message: "nexus operation completed unsuccessfully",
			FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
				NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
					ScheduledEventId: 1,
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
	}, backend.events[0].GetNexusOperationTimedOutEventAttributes())
}
