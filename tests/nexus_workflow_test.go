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
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
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
	"go.temporal.io/server/service/frontend/configs"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestNexusScheduleAndCancelCommands verifies that ScheduleNexusOperation and CancelNexusOperation commands can be
// processed on workflow task completion.
// No further assertions are made. When the task executors for nexus operations are implemented this test will be extended.
func (s *ClientFunctionalSuite) TestNexusScheduleAndCancelCommands() {
	taskQueue := s.randomizeStr(s.T().Name())
	ctx := NewContext()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)
	pollResp, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   "service",
						Operation: "operation",
					},
				},
			},
			{
				// Start a timer to get a new workflow task.
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{
					StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "1",
						StartToFireTimeout: durationpb.New(time.Millisecond),
					},
				},
			},
		},
	})
	s.NoError(err)
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Greater(scheduledEventIdx, 0)

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	desc, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Equal(1, len(desc.PendingNexusOperations))
	op := desc.PendingNexusOperations[0]
	s.Equal("service", op.Service)
	s.Equal("operation", op.Operation)
	s.True(op.State == enumspb.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF || op.State == enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED)
	s.True(op.CancelationInfo.State == enumspb.NEXUS_OPERATION_CANCELATION_STATE_BACKING_OFF || op.State == enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED)
	err = s.sdkClient.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)
}

func (s *ClientFunctionalSuite) TestNexusOperationSyncCompletion() {
	ctx := NewContext()
	namespace := s.randomizeStr(s.T().Name())
	taskQueue := s.randomizeStr(s.T().Name())
	serviceName := s.randomizeStr(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)
	_, err = s.operatorClient.CreateNexusOutgoingService(ctx, &operatorservice.CreateNexusOutgoingServiceRequest{
		Namespace: namespace,
		Name:      serviceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url:               "http://" + listenAddr,
			PublicCallbackUrl: "http://localhost/callback",
		},
	})
	s.NoError(err)

	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.hostPort,
		Namespace: namespace,
	})
	s.NoError(err)

	run, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)
	pollResp, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   serviceName,
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
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

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletion() {
	ctx := NewContext()
	namespace := s.randomizeStr(s.T().Name())
	taskQueue := s.randomizeStr(s.T().Name())
	serviceName := s.randomizeStr(s.T().Name())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	_, err = s.operatorClient.CreateNexusOutgoingService(ctx, &operatorservice.CreateNexusOutgoingServiceRequest{
		Namespace: namespace,
		Name:      serviceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url:               "http://" + listenAddr,
			PublicCallbackUrl: "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(namespace),
		},
	})
	s.NoError(err)

	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.hostPort,
		Namespace: namespace,
	})
	s.NoError(err)

	run, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)
	pollResp, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   serviceName,
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Send a valid - successful completion request.
	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)
	res, snap := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "success"})

	// Resend the request and verify we get a not found error since the operation has already completed.
	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "error_not_found"})

	// Send an invalid completion request and verify that we get an error that the namespace in the URL doesn't match the namespace in the token.
	invalidCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(s.namespace)
	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), invalidCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusBadRequest, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.namespace, "outcome": "error_bad_request"})

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
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "error_not_found"})

	// Request fails if the state machine reference is stale.
	staleToken := common.CloneProto(completionToken)
	staleToken.Ref.MutableStateTransitionCount -= 1
	callbackToken, err = gen.Tokenize(workflowNotFoundToken)
	s.NoError(err)

	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "error_not_found"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
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

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

func (s *ClientFunctionalSuite) TestNexusOperationAsyncFailure() {
	ctx := NewContext()
	namespace := s.randomizeStr(s.T().Name())
	taskQueue := s.randomizeStr(s.T().Name())
	serviceName := s.randomizeStr(s.T().Name())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	_, err = s.operatorClient.CreateNexusOutgoingService(ctx, &operatorservice.CreateNexusOutgoingServiceRequest{
		Namespace: namespace,
		Name:      serviceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url:               "http://" + listenAddr,
			PublicCallbackUrl: "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(namespace),
		},
	})
	s.NoError(err)

	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.hostPort,
		Namespace: namespace,
	})
	s.NoError(err)

	run, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)
	pollResp, err := s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   serviceName,
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "success"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.engine.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
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

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

	// var f *failurepb.Failure
	s.ErrorAs(err, &wee)
	s.Equal("nexus operation completed unsuccessfully: test operation failed (type: NexusOperationFailure, retryable: false)", wee.Unwrap().Error())
}

func (s *ClientFunctionalSuite) TestNexusOperationAsyncCompletionErrors() {
	ctx := NewContext()
	namespace := s.randomizeStr(s.T().Name())

	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	s.T().Run("ConfigDisabled", func(t *testing.T) {
		dc := s.testCluster.host.dcClient

		dc.OverrideValue(t, dynamicconfig.FrontendEnableNexusAPIs, false)
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(namespace)
		res, snap := s.sendNexusCompletionRequest(ctx, t, publicCallbackUrl, completion, "")
		require.Equal(t, http.StatusNotFound, res.StatusCode)
		require.Equal(t, 1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.T().Run("NamespaceNotFound", func(t *testing.T) {
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path("namespace-doesnt-exist")
		res, snap := s.sendNexusCompletionRequest(ctx, t, publicCallbackUrl, completion, "")
		require.Equal(t, http.StatusNotFound, res.StatusCode)
		require.Equal(t, 1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.T().Run("InvalidToken", func(t *testing.T) {
		publicCallbackUrl := "http://" + s.httpAPIAddress + "/" + commonnexus.RouteCompletionCallback.Path(namespace)
		res, snap := s.sendNexusCompletionRequest(ctx, t, publicCallbackUrl, completion, "")
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
		require.Equal(t, 0, len(snap["nexus_completion_request_preprocess_errors"]))
		require.Equal(t, 1, len(snap["nexus_completion_requests"]))
		require.Subset(t, snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": namespace, "outcome": "error_bad_request"})
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
