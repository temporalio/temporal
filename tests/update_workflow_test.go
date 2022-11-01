// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestUpdateWorkflow_FirstWorkflowTask_AcceptComplete() {
	id := "integration-update-workflow-test-1"
	wt := "integration-update-workflow-test-1-type"
	tq := "integration-update-workflow-test-1-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   id,
		WorkflowType: workflowType,
		TaskQueue:    taskQueue,
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
  9 WorkflowTaskCompleted
 10 WorkflowExecutionUpdateAccepted
 11 WorkflowExecutionUpdateCompleted
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1, 2:
			updRequestMsg := task.Messages[0]
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

			s.Equal(payloads.EncodeString("update args"), updRequest.GetInput().GetArgs())
			s.Equal("update_handler", updRequest.GetInput().GetName())

			if msgHandlerCalls == 1 {
				s.EqualValues(2, updRequestMsg.GetEventId())
			} else if msgHandlerCalls == 2 {
				s.EqualValues(6, updRequestMsg.GetEventId())
			}

			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Response{
						Meta: updRequest.GetMeta(),
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("update success"),
							},
						},
					}),
				},
			}, nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}

	testCases := []struct {
		Name                       string
		RunID                      string
		ForceCreateNewWorkflowTask bool
		ResetHistoryEventID        int64
	}{
		{
			Name:                       "with RunID",
			RunID:                      we.GetRunId(),
			ForceCreateNewWorkflowTask: false,
			ResetHistoryEventID:        0,
		},
		{
			Name:                       "without RunID",
			RunID:                      "",
			ForceCreateNewWorkflowTask: true, // Must be true to create WT to complete WF.
			ResetHistoryEventID:        0,
		},
	}
	var lastWorkflowTask *workflowservice.PollWorkflowTaskQueueResponse
	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			updateResultCh := make(chan UpdateResult)
			updateWorkflowFn := func() {
				updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: we.GetWorkflowId(),
						RunId:      tc.RunID,
					},
					Request: &updatepb.Request{
						Meta:  &updatepb.Meta{UpdateId: uuid.New()},
						Input: &updatepb.Input{Name: "update_handler", Args: payloads.EncodeString("update args")},
					},
				})
				updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
			}
			go updateWorkflowFn()
			time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

			// Process update in workflow.
			_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, tc.ForceCreateNewWorkflowTask, nil)
			s.NoError(err)
			updateResult := <-updateResultCh
			s.NoError(updateResult.Err)
			s.EqualValues(payloads.EncodeString("update success"), updateResult.Response.GetOutcome().GetSuccess())
			s.Equal(tc.ResetHistoryEventID, updateResp.ResetHistoryEventId)
			lastWorkflowTask = updateResp.GetWorkflowTask()
		})
	}

	s.NotNil(lastWorkflowTask)
	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(lastWorkflowTask, true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionUpdateAccepted
 11 WorkflowExecutionUpdateCompleted
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted
 15 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_NewWorkflowTask_AcceptComplete() {
	id := "integration-update-workflow-test-2"
	wt := "integration-update-workflow-test-2-type"
	tq := "integration-update-workflow-test-2-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   id,
		WorkflowType: workflowType,
		TaskQueue:    taskQueue,
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: "activity_type_1"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
					ScheduleToCloseTimeout: timestamp.DurationPtr(10 * time.Hour),
				}},
			}}, nil
		case 2:
			// Speculative WT, with update.Request message.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			return nil, nil
		case 2:
			updRequestMsg := task.Messages[0]
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

			s.Equal(payloads.EncodeString("update args"), updRequest.GetInput().GetArgs())
			s.Equal("update_handler", updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Response{
						Meta: updRequest.GetMeta(),
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("update success"),
							},
						},
					}),
				},
			}, nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start activity using existing workflow task.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateResultCh := make(chan UpdateResult)
	updateWorkflowFn := func() {
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: we,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: uuid.New()},
				Input: &updatepb.Input{
					Name: "update_handler",
					Args: payloads.EncodeString("update args"),
				},
			},
		})
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.NoError(updateResult.Err)
	s.EqualValues(payloads.EncodeString("update success"), updateResult.Response.GetOutcome().GetSuccess())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_FirstWorkflowTask_Reject() {
	id := "integration-update-workflow-test-3"
	wt := "integration-update-workflow-test-3-type"
	tq := "integration-update-workflow-test-3-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Minute),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			// Message handler will add commands.
			return nil, nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			updRequestMsg := task.Messages[0]
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

			s.Equal(payloads.EncodeString("update args"), updRequest.GetInput().GetArgs())
			s.Equal("update_handler", updRequest.GetInput().GetName())
			s.EqualValues(2, updRequestMsg.GetEventId())

			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Rejection{
						RejectedRequestMessageId:         updRequestMsg.GetId(),
						RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
						RejectedRequest:                  updRequest,
						Failure: &failurepb.Failure{
							Message:     "update rejected",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
						},
					}),
				},
			}, nil
		case 2:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateResultCh := make(chan UpdateResult)
	updateWorkflowFn := func() {
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: we,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: uuid.New()},
				Input: &updatepb.Input{
					Name: "update_handler",
					Args: payloads.EncodeString("update args"),
				},
			},
		})
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.NoError(updateResult.Err)
	s.Equal("update rejected", updateResult.Response.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_NewWorkflowTask_Reject() {
	id := "integration-update-workflow-test-4"
	wt := "integration-update-workflow-test-4-type"
	tq := "integration-update-workflow-test-4-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Minute),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: "activity_type_1"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
					ScheduleToCloseTimeout: timestamp.DurationPtr(10 * time.Hour),
				}},
			}}, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			return nil, nil
		case 2:
			updRequestMsg := task.Messages[0]
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

			s.Equal(payloads.EncodeString("update args"), updRequest.GetInput().GetArgs())
			s.Equal("update_handler", updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Rejection{
						RejectedRequestMessageId:         updRequestMsg.GetId(),
						RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
						RejectedRequest:                  updRequest,
						Failure: &failurepb.Failure{
							Message:     "update rejected",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
						},
					}),
				},
			}, nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start activity using existing workflow task.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateResultCh := make(chan UpdateResult)
	updateWorkflowFn := func() {
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: we,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: uuid.New()},
				Input: &updatepb.Input{
					Name: "update_handler",
					Args: payloads.EncodeString("update args"),
				},
			},
		})
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.NoError(updateResult.Err)
	s.Equal("update rejected", updateResult.Response.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_FirstWorkflowTask_1stAccept_2ndAccept_2ndComplete_1stComplete() {
	id := "integration-update-workflow-test-5"
	wt := "integration-update-workflow-test-5-type"
	tq := "integration-update-workflow-test-5-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Minute),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	var upd1Meta, upd2Meta *updatepb.Meta

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			// Message handler accepts 1st update.
			return nil, nil
		case 2:
			// Complete WT with empty command and message list.
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, history)
			// Message handler accepts 2nd update.
			return nil, nil
		case 4:
			s.EqualHistory(`
 11 WorkflowTaskCompleted
 12 WorkflowExecutionUpdateAccepted
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted`, history)
			// Message handler completes 2nd update.
			return nil, nil
		case 5:
			s.EqualHistory(`
 15 WorkflowTaskCompleted
 16 WorkflowExecutionUpdateCompleted
 17 WorkflowTaskScheduled
 18 WorkflowTaskStarted`, history)
			// Message handler completes 1st update.
			return nil, nil
		case 6:
			s.EqualHistory(`
 19 WorkflowTaskCompleted
 20 WorkflowExecutionUpdateCompleted
 21 WorkflowTaskScheduled
 22 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(2, updRequestMsg.GetEventId())
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())
			upd1Meta = updRequest.GetMeta()
			// Accept 1st update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
			}, nil
		case 2:
			return nil, nil
		case 3:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(8, updRequestMsg.GetEventId())
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())
			upd2Meta = updRequest.GetMeta()
			// Accept 2nd update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
			}, nil
		case 4:
			s.NotNil(upd2Meta)
			// Complete 2nd update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd2Meta.GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Response{
						Meta: upd2Meta,
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("update2 success"),
							},
						},
					}),
				},
			}, nil
		case 5:
			s.NotNil(upd1Meta)
			// Complete 1st update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1Meta.GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Response{
						Meta: upd1Meta,
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("update1 success"),
							},
						},
					}),
				},
			}, nil
		case 6:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateWorkflowFn := func(ch chan<- UpdateResult, updateArgs string) {
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: we,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: uuid.New()},
				Input: &updatepb.Input{
					Name: "update_handler",
					Args: payloads.EncodeString(updateArgs),
				},
			},
		})
		ch <- UpdateResult{Response: updateResponse, Err: err1}
	}

	updateResultCh1 := make(chan UpdateResult)
	go updateWorkflowFn(updateResultCh1, "update1 args")
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Accept update1 in WT1.
	_, updateAcceptResp1, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	s.NotNil(updateAcceptResp1)
	s.EqualValues(0, updateAcceptResp1.ResetHistoryEventId)

	// Start update after WT2 has started.
	updateResultCh2 := make(chan UpdateResult)
	go updateWorkflowFn(updateResultCh2, "update2 args")
	time.Sleep(500 * time.Millisecond) // This is to make sure that update2 gets to the server before WT2 is completed and WT3 is started.

	// WT2 from updateAcceptResp1.GetWorkflowTask() doesn't have 2nd update because WT2 has started before update was received.
	// Complete WT2 with empty commands list.
	completeEmptyWTResp, err := poller.HandlePartialWorkflowTask(updateAcceptResp1.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeEmptyWTResp)
	s.EqualValues(0, completeEmptyWTResp.ResetHistoryEventId)

	// WT3 from completeEmptyWTResp.GetWorkflowTask() has 2nd update.
	// Accept update2 in WT3.
	updateAcceptResp2, err := poller.HandlePartialWorkflowTask(completeEmptyWTResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateAcceptResp2)
	s.EqualValues(0, updateAcceptResp2.ResetHistoryEventId)

	// Complete update2 in WT4.
	updateCompleteResp2, err := poller.HandlePartialWorkflowTask(updateAcceptResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp2)
	updateResult2 := <-updateResultCh2
	s.NoError(updateResult2.Err)
	s.EqualValues(payloads.EncodeString("update2 success"), updateResult2.Response.GetOutcome().GetSuccess())
	s.EqualValues(0, updateCompleteResp2.ResetHistoryEventId)

	// Complete update1 in WT5.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateCompleteResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.NoError(updateResult1.Err)
	s.EqualValues(payloads.EncodeString("update1 success"), updateResult1.Response.GetOutcome().GetSuccess())
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WT6.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(6, wtHandlerCalls)
	s.Equal(6, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionUpdateAccepted
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionUpdateCompleted
 17 WorkflowTaskScheduled
 18 WorkflowTaskStarted
 19 WorkflowTaskCompleted
 20 WorkflowExecutionUpdateCompleted
 21 WorkflowTaskScheduled
 22 WorkflowTaskStarted
 23 WorkflowTaskCompleted
 24 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_FirstWorkflowTask_1stAccept_2ndReject_1stComplete() {
	id := "integration-update-workflow-test-6"
	wt := "integration-update-workflow-test-6-type"
	tq := "integration-update-workflow-test-6-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Minute),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      startResp.GetRunId(),
	}

	var upd1Meta *updatepb.Meta

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			// Message handler accepts 1st update.
			return nil, nil
		case 2:
			// Complete WT with empty command and message list.
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			// Return non-empty command list to indicate that WT is not heartbeat.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: "activity_type_1"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
					ScheduleToCloseTimeout: timestamp.DurationPtr(10 * time.Hour),
				}},
			}}, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 ActivityTaskScheduled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
			// Message handler rejects 2nd update.
			return nil, nil
		case 4:
			// WT3 was speculative.
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 ActivityTaskScheduled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
			return nil, nil
		case 5:
			s.EqualHistory(`
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(2, updRequestMsg.GetEventId())
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())
			upd1Meta = updRequest.GetMeta()
			// Accept 1st update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  updRequest,
					}),
				},
			}, nil
		case 2:
			return nil, nil
		case 3:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(9, updRequestMsg.GetEventId())
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())
			// Reject 2nd update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Rejection{
						RejectedRequestMessageId:         updRequestMsg.GetId(),
						RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
						RejectedRequest:                  updRequest,
						Failure: &failurepb.Failure{
							Message:     "update2 rejected",
							FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
						},
					}),
				},
			}, nil
		case 4:
			s.NotNil(upd1Meta)
			// Complete 1st update.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1Meta.GetUpdateId(),
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Response{
						Meta: upd1Meta,
						Outcome: &updatepb.Outcome{
							Value: &updatepb.Outcome_Success{
								Success: payloads.EncodeString("update1 success"),
							},
						},
					}),
				},
			}, nil
		case 5:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateWorkflowFn := func(ch chan<- UpdateResult, updateArgs string) {
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: we,
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: uuid.New()},
				Input: &updatepb.Input{
					Name: "update_handler",
					Args: payloads.EncodeString(updateArgs),
				},
			},
		})
		ch <- UpdateResult{Response: updateResponse, Err: err1}
	}

	updateResultCh1 := make(chan UpdateResult)
	go updateWorkflowFn(updateResultCh1, "update1 args")
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Accept update1 in WT1.
	_, updateAcceptResp1, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	s.NotNil(updateAcceptResp1)
	s.EqualValues(0, updateAcceptResp1.ResetHistoryEventId)

	// Start update after WT2 has started.
	updateResultCh2 := make(chan UpdateResult)
	go updateWorkflowFn(updateResultCh2, "update2 args")
	time.Sleep(500 * time.Millisecond) // This is to make sure that update2 gets to the server before WT2 is completed and WT3 is started.

	// WT2 from updateAcceptResp1.GetWorkflowTask() doesn't have 2nd update because WT2 has started before update was received.
	// Complete WT2 with empty commands list.
	// Don't force create new workflow task to make it speculative (otherwise it will be created as normal).
	completeEmptyWTResp, err := poller.HandlePartialWorkflowTask(updateAcceptResp1.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(completeEmptyWTResp)
	s.EqualValues(0, completeEmptyWTResp.ResetHistoryEventId)

	// WT3 from completeEmptyWTResp.GetWorkflowTask() has 2nd update.
	// Reject update2 in WT3.
	updateRejectResp2, err := poller.HandlePartialWorkflowTask(completeEmptyWTResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateRejectResp2)
	updateResult2 := <-updateResultCh2
	s.NoError(updateResult2.Err)
	s.Equal("update2 rejected", updateResult2.Response.GetOutcome().GetFailure().GetMessage())
	s.printWorkflowHistoryCompact(s.namespace, we)
	s.EqualValues(7, updateRejectResp2.ResetHistoryEventId)

	// Complete update1 in WT4.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateRejectResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.NoError(updateResult1.Err)
	s.EqualValues(payloads.EncodeString("update1 success"), updateResult1.Response.GetOutcome().GetSuccess())
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WT5.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 ActivityTaskScheduled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionCompleted`, events)
}
