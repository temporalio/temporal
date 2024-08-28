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

package tests

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *ClientFunctionalSuite) TestNexusOperationCancelation() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			if service != "service" {
				return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, `expected service to equal "service"`)
			}
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, operationID string, options nexus.CancelOperationOptions) error {
			return nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: time.Second,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: &taskqueue.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		assert.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and wait for the "started" event to be recorded.
	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Get the scheduleEventId to issue the cancel command.
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Greater(scheduledEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: pollResp.History.Events[scheduledEventIdx].EventId,
					},
				},
			},
		},
	})
	s.NoError(err)
	// Poll and wait for the cancelation request to go through.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(desc.PendingNexusOperations))
		op := desc.PendingNexusOperations[0]
		require.Equal(t, pollResp.History.Events[scheduledEventIdx].EventId, op.ScheduledEventId)
		require.Equal(t, endpointName, op.Endpoint)
		require.Equal(t, "service", op.Service)
		require.Equal(t, "operation", op.Operation)
		require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED, op.State)
		require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, op.CancellationInfo.State)

	}, time.Second*10, time.Millisecond*30)

	err = s.sdkClient.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)
}

func (s *ClientFunctionalSuite) TestNexusOperationSyncCompletion() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: &taskqueue.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		assert.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *ClientFunctionalSuite) TestNexusOperationSyncCompletion_LargePayload() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
			// additional Content headers. See common/rpc/grpc.go:66
			return &nexus.HandlerStartOperationResultSync[any]{Value: strings.Repeat("a", (2*1024*1024)-10)}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: &taskqueue.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		assert.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	failedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationFailedEventAttributes() != nil
	})
	s.Greater(failedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								s.mustToPayload(pollResp.History.Events[failedEventIdx].GetNexusOperationFailedEventAttributes().Failure.Cause.Message),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("http: response body too large", result)
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletion() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	testClusterInfo, err := s.client.GetClusterInfo(ctx, &workflowservice.GetClusterInfoRequest{})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	var callbackToken, publicCallbackUrl string

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

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			s.Equal(testClusterInfo.GetClusterId(), options.CallbackHeader.Get("source"))

			s.Len(options.Links, 1)
			var links []*commonpb.Link
			for _, nexusLink := range options.Links {
				link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
				s.NoError(err)
				links = append(links, &commonpb.Link{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: link,
					},
				})
			}
			s.NotNil(links[0].GetWorkflowEvent())
			protorequire.ProtoEqual(s.T(), &commonpb.Link_WorkflowEvent{
				Namespace:  s.namespace,
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						// Event history:
						// 1 WORKFLOW_EXECUTION_STARTED
						// 2 WORKFLOW_TASK_SCHEDULED
						// 3 WORKFLOW_TASK_STARTED
						// 4 WORKFLOW_TASK_COMPLETED
						// 5 NEXUS_OPERATION_SCHEDULED
						EventId:   5,
						EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					},
				},
			}, links[0].GetWorkflowEvent())

			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{
				OperationID: "test",
				Links:       []nexus.Link{handlerNexusLink},
			}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err = s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// Remember the workflow task completed event ID (next after the last WFT started), we'll use it to test reset
	// below.
	wftCompletedEventID := int64(len(pollResp.History.Events))

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	s.Len(pollResp.History.Events[startedEventIdx].Links, 1)
	protorequire.ProtoEqual(s.T(), handlerLink, pollResp.History.Events[startedEventIdx].Links[0].GetWorkflowEvent())

	// Completion request fails if the result payload is too large.
	largeCompletion, err := nexus.NewOperationCompletionSuccessful(
		// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
		// additional Content headers. See common/rpc/grpc.go:66
		s.mustToPayload(strings.Repeat("a", (2*1024*1024)-10)),
		nexus.OperationCompletionSuccesfulOptions{Serializer: commonnexus.PayloadSerializer},
	)
	s.NoError(err)
	res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, largeCompletion, callbackToken)
	s.Equal(http.StatusBadRequest, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_bad_request"})

	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload(nil), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	invalidNamespace := s.randomizeStr("ns")
	_, err = s.client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        invalidNamespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	// Send an invalid completion request and verify that we get an error that the namespace in the URL doesn't match the namespace in the token.
	invalidCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(invalidNamespace)
	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), invalidCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusBadRequest, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": invalidNamespace, "outcome": "error_bad_request"})

	// Manipulate the token to verify we get the expected errors in the API.
	gen := &commonnexus.CallbackTokenGenerator{}
	decodedToken, err := commonnexus.DecodeCallbackToken(callbackToken)
	s.NoError(err)
	completionToken, err := gen.DecodeCompletion(decodedToken)
	s.NoError(err)

	// Request fails if the workflow is not found.
	workflowNotFoundToken := common.CloneProto(completionToken)
	workflowNotFoundToken.WorkflowId = "not-found"
	callbackToken, err = gen.Tokenize(workflowNotFoundToken)
	s.NoError(err)

	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_not_found"})

	// Request fails if the state machine reference is stale.
	staleToken := common.CloneProto(completionToken)
	staleToken.Ref.MachineInitialVersionedTransition.NamespaceFailoverVersion++
	callbackToken, err = gen.Tokenize(staleToken)
	s.NoError(err)

	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_not_found"})

	// Send a valid - successful completion request.
	completion, err = nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	callbackToken, err = gen.Tokenize(completionToken)
	s.NoError(err)

	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "success"})
	// Ensure that CompleteOperation request is tracked as part of normal service telemetry metrics
	idx := slices.IndexFunc(snap["service_requests"], func(m *metricstest.CapturedRecording) bool {
		opTag, ok := m.Tags["operation"]
		return ok && opTag == "CompleteNexusOperation"
	})
	s.Greater(idx, -1)

	// Resend the request and verify we get a not found error since the operation has already completed.
	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_not_found"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)

	// Reset the workflow and check that the completion event has been reapplied.
	resp, err := s.client.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := s.sdkClient.GetWorkflowHistory(ctx, run.GetID(), resp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenCompletedEvent := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED {
			seenCompletedEvent = true
		}
	}
	s.True(seenCompletedEvent)
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncFailure() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
			TaskQueue: &taskqueue.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		assert.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and verify that the "started" event was recorded.
	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Send a valid - failed completion request.
	completion := &nexus.OperationCompletionUnsuccessful{
		State: nexus.OperationStateFailed,
		Failure: &nexus.Failure{
			Message: "test operation failed",
		},
	}
	res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "success"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationFailedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
					FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
						Failure: pollResp.History.Events[completedEventIdx].GetNexusOperationFailedEventAttributes().Failure,
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	err = run.Get(ctx, &result)
	var wee *temporal.WorkflowExecutionError

	s.ErrorAs(err, &wee)
	s.Equal("nexus operation completed unsuccessfully: test operation failed (type: NexusOperationFailure, retryable: false)", wee.Unwrap().Error())
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletionErrors() {
	ctx := NewContext()

	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	s.Run("ConfigDisabled", func() {
		s.OverrideDynamicConfig(dynamicconfig.EnableNexus, false)
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(s.namespace)
		res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, "")
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("NamespaceNotFound", func() {
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path("namespace-doesnt-exist")
		res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, "")
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("InvalidToken", func() {
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(s.namespace)
		res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, "")
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(0, len(snap["nexus_completion_request_preprocess_errors"]))
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_bad_request"})
	})

	s.Run("InvalidClientVersion", func() {
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(s.namespace)
		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		req, err := nexus.NewCompletionHTTPRequest(ctx, publicCallbackUrl, completion)
		s.NoError(err)
		req.Header.Set("User-Agent", "Nexus-go-sdk/v99.0.0")

		res, err := http.DefaultClient.Do(req)
		s.NoError(err)
		_, err = io.ReadAll(res.Body)
		s.NoError(err)
		defer res.Body.Close()

		snap := capture.Snapshot()
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "unsupported_client"})
	})
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletionAuthErrors() {
	ctx := NewContext()

	onAuthorize := func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.CompleteNexusOperation {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	s.testCluster.host.SetOnAuthorize(onAuthorize)
	defer s.testCluster.host.SetOnAuthorize(nil)

	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(s.namespace)
	res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, "")
	s.Equal(http.StatusForbidden, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_unauthorized"})
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletionInternalAuth() {
	// Set URL template with invalid host
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://INTERNAL/namespaces/{{.NamespaceName}}/nexus/callback")

	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.namespace,
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	completionWFType := "completion_wf"
	completionWFTaskQueue := s.randomizeStr(s.T().Name())
	completionWFStartReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.namespace,
		WorkflowId:         s.randomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: completionWFType},
		TaskQueue:          &taskqueue.TaskQueue{Name: completionWFTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           "test",
	}

	go s.nexusTaskPoller(ctx, taskQueue, func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
		start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
		s.Equal(op.Name(), start.Operation)

		completionWFStartReq.CompletionCallbacks = []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:    start.Callback,
						Header: start.CallbackHeader,
					},
				},
			},
		}

		_, err := s.client.StartWorkflowExecution(ctx, completionWFStartReq)
		s.NoError(err)

		return &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
						AsyncSuccess: &nexuspb.StartOperationResponse_Async{
							OperationId: "test-id",
						},
					},
				},
			},
		}, nil
	})

	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "test-service",
						Operation: "my-operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)
	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Complete workflow containing callback
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: completionWFTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								s.mustToPayload("result"),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})

	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletionAfterReset() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	endpointName := RandomizedNexusEndpoint(s.T().Name())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.operatorClient.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Remember the workflow task completed event ID (next after the last WFT started), we'll use it to test reset
	// below.
	wftCompletedEventID := int64(len(pollResp.History.Events))

	// Reset the workflow and check that the started event has been reapplied.
	resetResp, err := s.client.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := s.sdkClient.GetWorkflowHistory(ctx, run.GetID(), resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenCompletedEvent := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED {
			seenCompletedEvent = true
		}
	}
	s.True(seenCompletedEvent)
	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	res, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.client.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	run = s.sdkClient.GetWorkflow(ctx, run.GetID(), resetResp.RunId)
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *FunctionalTestBase) sendNexusCompletionRequest(
	ctx context.Context,
	t *testing.T,
	url string,
	completion nexus.OperationCompletion,
	callbackToken string,
) (*http.Response, map[string][]*metricstest.CapturedRecording) {
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	req, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	require.NoError(t, err)
	if callbackToken != "" {
		req.Header.Add(commonnexus.CallbackTokenHeader, callbackToken)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	defer res.Body.Close()
	return res, capture.Snapshot()
}
