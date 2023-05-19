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
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestUpdateWorkflow_FirstWorkflowTask_AcceptComplete() {
	id := "integration-update-workflow-test-1"
	wt := "integration-update-workflow-test-1-type"
	tq := "integration-update-workflow-test-1-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	stickyQueue := &taskqueuepb.TaskQueue{Name: tq + "-sticky"}

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
				s.EqualValues(7, updRequestMsg.GetEventId())
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
		StickyTaskQueue:     stickyQueue,
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
				assert.NoError(s.T(), err1)
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

func (s *integrationSuite) TestUpdateWorkflow_ValidateMessages() {
	id := "integration-update-workflow-validate-message-id"
	wt := "integration-update-workflow-validate-message-wt"
	tq := "integration-update-workflow-validate-message-tq"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	testCases := []struct {
		Name                     string
		RespondWorkflowTaskError string
		MessageFn                func(reqMsg *protocolpb.Message) []*protocolpb.Message
	}{
		{
			Name:                     "update-id-not-found",
			RespondWorkflowTaskError: "not found",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 uuid.New(),
						ProtocolInstanceId: "bogus-update-id",
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
		},
		{
			Name:                     "complete-without-accept",
			RespondWorkflowTaskError: "invalid state transition attempted",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
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
				}
			},
		},
		{
			Name:                     "accept-twice",
			RespondWorkflowTaskError: "invalid state transition attempted",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 uuid.New(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 uuid.New(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
		},
		{
			Name:                     "success-case",
			RespondWorkflowTaskError: "",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 uuid.New(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
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
				}
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:    uuid.New(),
				Namespace:    s.namespace,
				WorkflowId:   id + tc.Name,
				WorkflowType: workflowType,
				TaskQueue:    taskQueue,
			}

			startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
			s.NoError(err)

			we := &commonpb.WorkflowExecution{
				WorkflowId: id + tc.Name,
				RunId:      startResp.GetRunId(),
			}

			wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
				return nil, nil
			}

			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				updRequestMsg := task.Messages[0]
				return tc.MessageFn(updRequestMsg), nil
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

			updateWorkflowFn := func() {
				_, _ = s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: we.GetWorkflowId(),
						RunId:      we.GetRunId(),
					},
					Request: &updatepb.Request{
						Meta:  &updatepb.Meta{UpdateId: uuid.New()},
						Input: &updatepb.Input{Name: "update_handler", Args: payloads.EncodeString("update args")},
					},
				})
			}
			go updateWorkflowFn()

			// Process update in workflow.
			_, _, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, false, nil)
			if len(tc.RespondWorkflowTaskError) > 0 {
				// respond workflow task should return error
				require.Error(t, err, "RespondWorkflowTaskCompleted should return an error contains`%v`", tc.RespondWorkflowTaskError)
				require.Contains(t, err.Error(), tc.RespondWorkflowTaskError)
			} else {
				require.NoError(t, err)
			}

		})
	}

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
			s.EqualValues(6, updRequestMsg.GetEventId())

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
		assert.NoError(s.T(), err1)
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

func (s *integrationSuite) TestUpdateWorkflow_NewWorkflowTask_AcceptComplete_Sticky() {
	id := "integration-update-workflow-test-2"
	wt := "integration-update-workflow-test-2-type"
	tq := "integration-update-workflow-test-2-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	stickyQueue := &taskqueuepb.TaskQueue{Name: tq + "-sticky"}

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
			// We make sure that this task contains partial history because it is sticky enabled
			s.EqualHistory(`
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
			s.EqualValues(6, updRequestMsg.GetEventId())

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
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyQueue,
		StickyScheduleToStartTimeout: 3 * time.Second * debug.TimeoutMultiplier,
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// poll from regular task queue, but respond with sticky enabled response, next wft will be sticky
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.NoError(err)

	type UpdateResult struct {
		Response *workflowservice.UpdateWorkflowExecutionResponse
		Err      error
	}
	updateResultCh := make(chan UpdateResult)
	updateWorkflowFn := func() {
		time.Sleep(500 * time.Millisecond) // This is to make sure that sticky poller reach to server first

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
		assert.NoError(s.T(), err1)
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()

	// Process update in workflow task (it is sticky).
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, true, false, 1, 5, true, nil)
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

func (s *integrationSuite) TestUpdateWorkflow_NewWorkflowTask_AcceptComplete_StickyWorkerUnavailable() {
	id := "integration-update-workflow-test-swu"
	wt := "integration-update-workflow-test-swu-type"
	tq := "integration-update-workflow-test-swu-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	stickyQueue := &taskqueuepb.TaskQueue{Name: tq + "-sticky"}

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
			// Worker gets full history because update was issued after sticky worker is gone.
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
			s.EqualValues(6, updRequestMsg.GetEventId())

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
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyQueue,
		StickyScheduleToStartTimeout: 3 * time.Second * debug.TimeoutMultiplier,
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// poll from regular task queue, but respond with sticky enabled response to enable stick task queue.
	_, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 1, 5)
	s.NoError(err)

	s.Logger.Info("Sleep 10 seconds to make sure stickyPollerUnavailableWindow time has passed.")
	time.Sleep(10 * time.Second)
	s.Logger.Info("Sleep 10 seconds is done.")

	// Now send an update. It should try sticky task queue first, but got "StickyWorkerUnavailable" error
	// and resend it to normal.
	// This can be observed in wtHandler: if history is partial => sticky task queue is used.

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
		assert.NoError(s.T(), err1)
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Process update in workflow task from non-sticky task queue.
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
		assert.NoError(s.T(), err1)
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
			s.EqualValues(6, updRequestMsg.GetEventId())

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
		assert.NoError(s.T(), err1)
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
			s.EqualValues(9, updRequestMsg.GetEventId())
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
		assert.NoError(s.T(), err1)
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
			s.EqualValues(10, updRequestMsg.GetEventId())
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
		assert.NoError(s.T(), err1)
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

func (s *integrationSuite) TestUpdateWorkflow_FailWorkflowTask() {
	id := "integration-update-workflow-test-7"
	wt := "integration-update-workflow-test-7-type"
	tq := "integration-update-workflow-test-7-task-queue"

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
			s.Fail("should not be called because messageHandler returns error")
			return nil, nil
		case 4:
			s.Fail("should not be called because messageHandler returns error")
			return nil, nil
		case 5:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, history)
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
			s.EqualValues(6, updRequestMsg.GetEventId())

			// Emulate bug in worker/SDK update handler code. Return malformed acceptance response.
			return []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: "some-random-wrong-id",
					SequencingId:       nil,
					Body: marshalAny(s, &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  nil, // must not be nil.
					}),
				},
			}, nil
		case 3:
			// 2nd attempt has same updates attached to it.
			updRequestMsg := task.Messages[0]
			s.EqualValues(9, updRequestMsg.GetEventId())
			wtHandlerCalls++ // because it won't be called for case 3 but counter should be in sync.
			// Fail WT one more time. Although 2nd attempt is normal WT, it is also transient and shouldn't appear in the history.
			// Returning error will cause the poller to fail WT.
			return nil, errors.New("malformed request")
		case 4:
			// 3rd attempt UpdateWorkflowExecution call has timed out but the
			// update is still running
			updRequestMsg := task.Messages[0]
			s.EqualValues(9, updRequestMsg.GetEventId())
			wtHandlerCalls++ // because it won't be called for case 4 but counter should be in sync.
			// Fail WT one more time. This is transient WT and shouldn't appear in the history.
			// Returning error will cause the poller to fail WT.
			return nil, errors.New("malformed request")
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

	// Start activity using existing workflow task.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		ctx1, cancel := context.WithTimeout(NewContext(), 2*time.Second)
		defer cancel()
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(ctx1, &workflowservice.UpdateWorkflowExecutionRequest{
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
		assert.Error(s.T(), err1)
		// UpdateWorkflowExecution is timed out after 2 seconds.
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1))
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// Try to accept update in workflow: get malformed response.
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err)
	s.Contains(err.Error(), "not found")
	// New normal (but transient) WT will be created but not returned.

	// Try to accept update in workflow 2nd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Wait for UpdateWorkflowExecution to timeout.
	// This does NOT remove update from registry
	<-updateResultCh

	// Try to accept update in workflow 3rd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Complete workflow.
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err, "update was never successfully accepted so it prevents completion")

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_ConvertSpeculativeWorkflowTaskToNormal_BecauseOfBufferedSignal() {
	id := "integration-update-workflow-test-8"
	wt := "integration-update-workflow-test-8-type"
	tq := "integration-update-workflow-test-8-task-queue"

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
			// Send signal which will be buffered. This will persist MS and speculative WT must be converted to normal.
			err = s.sendSignal(s.namespace, we, "SignalName", payloads.EncodeString("signal_data"), "worker_identity")
			s.NoError(err)
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionSignaled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
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
			s.EqualValues(6, updRequestMsg.GetEventId())

			// Update is rejected but corresponding speculative WT will be in the history anyway, because it was converted to normal due to buffered signal.
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
		assert.NoError(s.T(), err1)
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
  9 WorkflowExecutionSignaled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_TimeoutSpeculativeWorkflowTask() {
	id := "integration-update-workflow-test-9"
	wt := "integration-update-workflow-test-9-type"
	tq := "integration-update-workflow-test-9-task-queue"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
			// Emulate slow worker: sleep more than WT timeout.
			time.Sleep(1*time.Second + 100*time.Millisecond)
			return nil, nil
		case 3:
			// Speculative WT timed out and retried as normal WT.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, history)
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
		case 2, 3:
			updRequestMsg := task.Messages[0]
			updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

			s.Equal(payloads.EncodeString("update args"), updRequest.GetInput().GetArgs())
			s.Equal("update_handler", updRequest.GetInput().GetName())

			// This doesn't matter because:
			//   for iteration 2: WT times out before update is applied.
			//   for iteration 3: workflow completes with command, which ignores messages.
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
		// case 3:
		// 	return nil, nil
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
		assert.NoError(s.T(), err1)
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Try to process update in workflow, but it takes more than WT timeout. So, WT times out.
	_, _, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.Error(err)
	s.Equal("Workflow task not found.", err.Error())

	// New normal WT was created on server and can be polled now.
	// It will complete workflow using commands and then try to complete update using messages.
	// Messages will be ignored though, because completed workflow can't have updates.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.NoError(updateResult.Err)
	s.EqualValues(payloads.EncodeString("update success"), updateResult.Response.GetOutcome().GetSuccess())
	s.EqualValues(0, updateResp.ResetHistoryEventId)
	s.Nil(updateResp.GetWorkflowTask())

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
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionUpdateAccepted
 13 WorkflowExecutionUpdateCompleted
 14 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_TerminateWorkflow() {
	id := "integration-update-workflow-test-sswt1"
	wt := "integration-update-workflow-test-sswt1-type"
	tq := "integration-update-workflow-test-sswt1-task-queue"

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
			// Terminate workflow while speculative WT is running.
			_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace:         s.namespace,
				WorkflowExecution: we,
				Reason:            "test-reason",
			})
			s.NoError(err)

			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return nil, nil
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
			s.EqualValues(6, updRequestMsg.GetEventId())

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
		oneSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 1*time.Second)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(oneSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
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
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.Error(err)
	s.IsType(err, (*serviceerror.NotFound)(nil))
	s.ErrorContains(err, "Workflow task not found.")
	s.Nil(updateResp)
	<-updateResultCh

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowExecutionTerminated`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_TerminateWorkflow() {
	id := "integration-update-workflow-test-sswt2"
	wt := "integration-update-workflow-test-sswt2-type"
	tq := "integration-update-workflow-test-sswt2-task-queue"

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
		oneSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 1*time.Second)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(oneSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
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
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- UpdateResult{Response: updateResponse, Err: err1}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server.

	// Terminate workflow after speculative WT is scheduled but not started.
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	// Wait for update context to timeout.
	time.Sleep(1 * time.Second)

	s.Equal(1, wtHandlerCalls)
	s.Equal(1, msgHandlerCalls)

	events := s.getHistory(s.namespace, we)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionTerminated`, events)
}
