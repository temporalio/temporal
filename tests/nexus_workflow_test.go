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
	"slices"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/nexus/nexustest"
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
			Kind: enums.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enums.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Service:   "service",
						Operation: "operation",
					},
				},
			},
			{
				// Start a timer to get a new workflow task.
				CommandType: enums.COMMAND_TYPE_START_TIMER,
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
			Kind: enums.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Greater(scheduledEventIdx, 0)

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:                   "test",
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
		TaskToken:                  pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enums.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: pollResp.History.Events[scheduledEventIdx].EventId,
					},
				},
			},
		},
	})
	s.NoError(err)
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
			Kind: enums.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enums.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
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
			Kind: enums.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.engine.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:                   "test",
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
		TaskToken:                  pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enums.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
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
