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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/tests/testvars"
)

func (s *integrationSuite) startWorkflow(tv *testvars.TestVars) *testvars.TestVars {
	s.T().Helper()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    tv.Any(),
		Namespace:    s.namespace,
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	return tv.WithRunID(startResp.GetRunId())
}

func (s *integrationSuite) acceptUpdateCommands(tv *testvars.TestVars, updateID string) []*commandpb.Command {
	s.T().Helper()
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
			MessageId: tv.MessageID("update-accepted", updateID),
		}},
	}}
}

func (s *integrationSuite) completeUpdateCommands(tv *testvars.TestVars, updateID string) []*commandpb.Command {
	s.T().Helper()
	return []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: tv.MessageID("update-completed", updateID),
			}},
		},
	}
}

func (s *integrationSuite) acceptCompleteUpdateCommands(tv *testvars.TestVars, updateID string) []*commandpb.Command {
	s.T().Helper()
	return append(s.acceptUpdateCommands(tv, updateID), s.completeUpdateCommands(tv, updateID)...)
}

func (s *integrationSuite) acceptUpdateMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, updateID string) []*protocolpb.Message {
	s.T().Helper()
	updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-accepted", updateID),
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: marshalAny(s, &updatepb.Acceptance{
				AcceptedRequestMessageId:         updRequestMsg.GetId(),
				AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
				AcceptedRequest:                  updRequest,
			}),
		},
	}
}

func (s *integrationSuite) completeUpdateMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, updateID string) []*protocolpb.Message {
	s.T().Helper()
	updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-completed", updateID),
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: marshalAny(s, &updatepb.Response{
				Meta: updRequest.GetMeta(),
				Outcome: &updatepb.Outcome{
					Value: &updatepb.Outcome_Success{
						Success: payloads.EncodeString(tv.String("success-result", updateID)),
					},
				},
			}),
		},
	}
}

func (s *integrationSuite) acceptCompleteUpdateMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, updateID string) []*protocolpb.Message {
	s.T().Helper()
	return append(s.acceptUpdateMessages(tv, updRequestMsg, updateID), s.completeUpdateMessages(tv, updRequestMsg, updateID)...)
}

func (s *integrationSuite) rejectUpdateMessages(tv *testvars.TestVars, updRequestMsg *protocolpb.Message, updateID string) []*protocolpb.Message {
	s.T().Helper()
	updRequest := unmarshalAny[*updatepb.Request](s, updRequestMsg.GetBody())

	return []*protocolpb.Message{
		{
			Id:                 tv.MessageID("update-rejected", updateID),
			ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
			SequencingId:       nil,
			Body: marshalAny(s, &updatepb.Rejection{
				RejectedRequestMessageId:         updRequestMsg.GetId(),
				RejectedRequestSequencingEventId: updRequestMsg.GetEventId(),
				RejectedRequest:                  updRequest,
				Failure: &failurepb.Failure{
					Message:     tv.String("update rejected", updateID),
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{}},
				},
			}),
		},
	}
}

func (s *integrationSuite) sendUpdateNoError(tv *testvars.TestVars, updateID string) *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	resp, err := s.sendUpdate(tv, updateID)
	// It is important to do assert here to fail fast without trying to process update in wtHandler.
	assert.NoError(s.T(), err)
	return resp
}

func (s *integrationSuite) sendUpdate(tv *testvars.TestVars, updateID string) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	s.T().Helper()
	return s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: tv.WorkflowExecution(),
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateID)},
			Input: &updatepb.Input{
				Name: tv.HandlerName(),
				Args: payloads.EncodeString(tv.String("args", updateID)),
			},
		},
	})
}

func (s *integrationSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_AcceptComplete() {
	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			tv := testvars.New(t.Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
					return s.acceptCompleteUpdateCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, history)
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

					s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
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
				TaskQueue:           tv.TaskQueue(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain exiting first workflow task.
			_, err := poller.PollAndProcessWorkflowTask(true, false)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow.
			_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 1, true, nil)
			s.NoError(err)
			updateResult := <-updateResultCh
			s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
			s.NoError(err)
			s.NotNil(completeWorkflowResp)
			s.Nil(completeWorkflowResp.GetWorkflowTask())
			s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

			s.Equal(3, wtHandlerCalls)
			s.Equal(3, msgHandlerCalls)

			events := s.getHistory(s.namespace, tv.WorkflowExecution())

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
		})
	}
}

func (s *integrationSuite) TestUpdateWorkflow_FirstNormalWorkflowTask_AcceptComplete() {

	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			tv := testvars.New(t.Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
					return s.acceptCompleteUpdateCommands(tv, "1"), nil
				case 2:
					s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, history)
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

					s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(2, updRequestMsg.GetEventId())

					return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
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
				TaskQueue:           tv.TaskQueue(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow.
			_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
			s.NoError(err)

			updateResult := <-updateResultCh
			s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)
			lastWorkflowTask := updateResp.GetWorkflowTask()

			s.NotNil(lastWorkflowTask)
			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(lastWorkflowTask, true)
			s.NoError(err)
			s.NotNil(completeWorkflowResp)
			s.Nil(completeWorkflowResp.GetWorkflowTask())
			s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)

			events := s.getHistory(s.namespace, tv.WorkflowExecution())

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
 10 WorkflowExecutionCompleted`, events)
		})
	}
}

func (s *integrationSuite) TestUpdateWorkflow_ValidateWorkerMessages() {
	tv := testvars.New(s.T().Name())

	testCases := []struct {
		Name                     string
		RespondWorkflowTaskError string
		MessageFn                func(reqMsg *protocolpb.Message) []*protocolpb.Message
		CommandFn                func(history *historypb.History) []*commandpb.Command
	}{
		{
			Name:                     "message-update-id-not-found",
			RespondWorkflowTaskError: "not found",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: tv.WithUpdateID("bogus-update-id").UpdateID(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
		{
			Name:                     "command-reference-missed-message",
			RespondWorkflowTaskError: "referenced absent message ID",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any(), // Random message Id.
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
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
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
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: payloads.EncodeString(tv.Any()),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
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
						Id:                 tv.MessageID("update-accepted", "1"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-accepted", "2"),
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
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted", "1"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted", "2"),
						}},
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
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: payloads.EncodeString(tv.Any()),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
					},
				}
			},
		},
		{
			Name:                     "success-case-no-commands", // PROTOCOL_MESSAGE commands are optional.
			RespondWorkflowTaskError: "",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.Any(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: payloads.EncodeString(tv.Any()),
								},
							},
						}),
					},
				}
			},
		},
		{
			Name:                     "invalid-command-order",
			RespondWorkflowTaskError: "invalid state transition attempted",
			MessageFn: func(reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := unmarshalAny[*updatepb.Request](s, reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: marshalAny(s, &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: payloads.EncodeString(tv.Any()),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					// Complete command goes before Accept command.
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			tv := testvars.New(t.Name())

			tv = s.startWorkflow(tv)

			wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
				if tc.CommandFn == nil {
					return nil, nil
				}
				return tc.CommandFn(history), nil
			}

			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				if tc.MessageFn == nil {
					return nil, nil
				}
				updRequestMsg := task.Messages[0]
				return tc.MessageFn(updRequestMsg), nil
			}

			poller := &TaskPoller{
				Engine:              s.engine,
				Namespace:           s.namespace,
				TaskQueue:           tv.TaskQueue(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			updateResultCh := make(chan struct{})
			updateWorkflowFn := func(errExpected bool) {
				halfSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 500*time.Millisecond)
				defer cancel()

				updateResponse, err1 := s.engine.UpdateWorkflowExecution(halfSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
					Namespace:         s.namespace,
					WorkflowExecution: tv.WorkflowExecution(),
					Request: &updatepb.Request{
						Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
						Input: &updatepb.Input{
							Name: tv.HandlerName(),
							Args: payloads.EncodeString(tv.Any()),
						},
					},
				})
				// When worker returns validation error, API caller got timeout error.
				if errExpected {
					assert.Error(s.T(), err1)
					assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
					assert.Nil(s.T(), updateResponse)
				} else {
					assert.NoError(s.T(), err1)
				}

				updateResultCh <- struct{}{}
			}
			go updateWorkflowFn(tc.RespondWorkflowTaskError != "")

			// Process update in workflow.
			_, err := poller.PollAndProcessWorkflowTask(false, false)
			if tc.RespondWorkflowTaskError != "" {
				// respond workflow task should return error
				require.Error(t, err, "RespondWorkflowTaskCompleted should return an error contains`%v`", tc.RespondWorkflowTaskError)
				require.Contains(t, err.Error(), tc.RespondWorkflowTaskError)
			} else {
				require.NoError(t, err)
			}
			<-updateResultCh
		})
	}
}

func (s *integrationSuite) TestUpdateWorkflow_NewStickySpeculativeWorkflowTask_AcceptComplete() {
	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			tv := testvars.New(t.Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: tv.InfiniteTimeout(),
						}},
					}}, nil
				case 2:
					// Speculative WT, with update.Request message.
					// This WT contains partial history because it is sticky enabled.
					s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
					return s.acceptCompleteUpdateCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, history)
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

					s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
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
				TaskQueue:                    tv.TaskQueue(),
				StickyTaskQueue:              tv.StickyTaskQueue(),
				StickyScheduleToStartTimeout: 3 * time.Second,
				WorkflowTaskHandler:          wtHandler,
				MessageHandler:               msgHandler,
				Logger:                       s.Logger,
				T:                            s.T(),
			}

			// Drain existing first WT from regular task queue, but respond with sticky queue enabled response, next WT will go to sticky queue.
			_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				time.Sleep(500 * time.Millisecond) // This is to make sure that next sticky poller reach to server first.
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow task (it is sticky).
			_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, true, false, 1, 5, true, nil)
			s.NoError(err)
			updateResult := <-updateResultCh
			s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
			s.NoError(err)
			s.NotNil(completeWorkflowResp)
			s.Nil(completeWorkflowResp.GetWorkflowTask())
			s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

			s.Equal(3, wtHandlerCalls)
			s.Equal(3, msgHandlerCalls)

			events := s.getHistory(s.namespace, tv.WorkflowExecution())

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
		})
	}
}

func (s *integrationSuite) TestUpdateWorkflow_NewStickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			return s.acceptCompleteUpdateCommands(tv, "1"), nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

			s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:                    tv.TaskQueue(),
		StickyTaskQueue:              tv.StickyTaskQueue(),
		StickyScheduleToStartTimeout: 3 * time.Second,
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// Drain existing WT from regular task queue, but respond with sticky enabled response to enable stick task queue.
	_, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 1, 5)
	s.NoError(err)

	s.Logger.Info("Sleep 10 seconds to make sure stickyPollerUnavailableWindow time has passed.")
	time.Sleep(10 * time.Second)
	s.Logger.Info("Sleep 10 seconds is done.")

	// Now send an update. It should try sticky task queue first, but got "StickyWorkerUnavailable" error
	// and resend it to normal.
	// This can be observed in wtHandler: if history is partial => sticky task queue is used.
	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow task from non-sticky task queue.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

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

func (s *integrationSuite) TestUpdateWorkflow_FirstNormalWorkflowTask_Reject() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

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
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

			s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(2, updRequestMsg.GetEventId())

			return s.rejectUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal(tv.String("update rejected", "1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

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

func (s *integrationSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_Reject() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

			s.Equal(tv.String("args", "1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.rejectUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain existing first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal(tv.String("update rejected", "1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())
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

func (s *integrationSuite) TestUpdateWorkflow_1stAccept_2ndAccept_2ndComplete_1stComplete() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			return s.acceptUpdateCommands(tv, "1"), nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return s.acceptUpdateCommands(tv, "2"), nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
			return s.completeUpdateCommands(tv, "2"), nil
		case 4:
			s.EqualHistory(`
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted`, history)
			return s.completeUpdateCommands(tv, "1"), nil
		case 5:
			s.EqualHistory(`
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted
 18 WorkflowTaskScheduled
 19 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	var upd1RequestMsg, upd2RequestMsg *protocolpb.Message

	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			upd1RequestMsg = task.Messages[0]
			upd1Request := unmarshalAny[*updatepb.Request](s, upd1RequestMsg.GetBody())
			s.Equal(tv.String("args", "1"), decodeString(s, upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.acceptUpdateMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg = task.Messages[0]
			upd2Request := unmarshalAny[*updatepb.Request](s, upd2RequestMsg.GetBody())
			s.Equal(tv.String("args", "2"), decodeString(s, upd2Request.GetInput().GetArgs()))
			s.EqualValues(6, upd2RequestMsg.GetEventId())
			return s.acceptUpdateMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd2RequestMsg)
			return s.completeUpdateMessages(tv, upd2RequestMsg, "2"), nil
		case 4:
			s.NotNil(upd1RequestMsg)
			return s.completeUpdateMessages(tv, upd1RequestMsg, "1"), nil
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh1 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh1 <- s.sendUpdateNoError(tv, "1")
	}()

	// Accept update1 in normal WT1.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh2 <- s.sendUpdateNoError(tv, "2")
	}()

	// Poll for WT2 which 2nd update. Accept update2.
	_, updateAcceptResp2, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 1, true, nil)
	s.NoError(err)
	s.NotNil(updateAcceptResp2)
	s.EqualValues(0, updateAcceptResp2.ResetHistoryEventId)

	// Complete update2 in WT3.
	updateCompleteResp2, err := poller.HandlePartialWorkflowTask(updateAcceptResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp2)
	updateResult2 := <-updateResultCh2
	s.EqualValues(tv.String("success-result", "2"), decodeString(s, updateResult2.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp2.ResetHistoryEventId)

	// Complete update1 in WT4.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateCompleteResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WF in WT5.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted
 18 WorkflowTaskScheduled
 19 WorkflowTaskStarted
 20 WorkflowTaskCompleted
 21 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_1stAccept_2ndReject_1stComplete() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, history)
			return s.acceptUpdateCommands(tv, "1"), nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			// Message handler rejects 2nd update.
			return nil, nil
		case 3:
			// WT2 was speculative.
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, history)
			return s.completeUpdateCommands(tv, "1"), nil
		case 4:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	var upd1RequestMsg *protocolpb.Message
	msgHandlerCalls := 0
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			upd1RequestMsg = task.Messages[0]
			upd1Request := unmarshalAny[*updatepb.Request](s, upd1RequestMsg.GetBody())
			s.Equal(tv.String("args", "1"), decodeString(s, upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.acceptUpdateMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg := task.Messages[0]
			upd2Request := unmarshalAny[*updatepb.Request](s, upd2RequestMsg.GetBody())
			s.Equal(tv.String("args", "2"), decodeString(s, upd2Request.GetInput().GetArgs()))
			s.EqualValues(6, upd2RequestMsg.GetEventId())
			return s.rejectUpdateMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd1RequestMsg)
			return s.completeUpdateMessages(tv, upd1RequestMsg, "1"), nil
		case 4:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh1 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh1 <- s.sendUpdateNoError(tv, "1")
	}()

	// Accept update1 in WT1.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh2 <- s.sendUpdateNoError(tv, "2")
	}()

	// Poll for WT2 which 2nd update. Reject update2.
	_, updateRejectResp2, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 1, true, nil)
	s.NoError(err)
	s.NotNil(updateRejectResp2)
	s.EqualValues(3, updateRejectResp2.ResetHistoryEventId)

	updateResult2 := <-updateResultCh2
	s.Equal(tv.String("update rejected", "2"), updateResult2.GetOutcome().GetFailure().GetMessage())

	// Complete update1 in WT3.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateRejectResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WT4.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(4, wtHandlerCalls)
	s.Equal(4, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_FailWorkflowTask() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			return s.acceptUpdateCommands(tv, "1"), nil
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
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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
					Id:                 tv.MessageID("update-accepted", "1"),
					ProtocolInstanceId: tv.Any(), // Random update Id.
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		ctx1, cancel := context.WithTimeout(NewContext(), 2*time.Second)
		defer cancel()
		updateResponse, err1 := s.engine.UpdateWorkflowExecution(ctx1, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.HandlerName(),
					Args: payloads.EncodeString(tv.Any()),
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
	s.NoError(err)

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_ConvertStartedSpeculativeWorkflowTaskToNormal_BecauseOfBufferedSignal() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			err := s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any(), payloads.EncodeString(tv.Any()), tv.Any())
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
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

			// Update is rejected but corresponding speculative WT will be in the history anyway, because it was converted to normal due to buffered signal.
			return s.rejectUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal(tv.String("update rejected", "1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

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

func (s *integrationSuite) TestUpdateWorkflow_ConvertScheduledSpeculativeWorkflowTaskToNormal_BecauseOfSignal() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			// Speculative WT was already converted to normal because of the signal.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowExecutionSignaled
  8 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
  9 WorkflowTaskCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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

			s.EqualValues(7, updRequestMsg.GetEventId())

			// Update is rejected but corresponding speculative WT was already converted to normal,
			// and will be in the history anyway.
			return s.rejectUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server before the next Signal call.

	// Send signal which will NOT be buffered because speculative WT is not started yet (only scheduled).
	// This will persist MS and speculative WT must be converted to normal.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any(), payloads.EncodeString(tv.Any()), tv.Any())
	s.NoError(err)

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal(tv.String("update rejected", "1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowExecutionSignaled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_StartToCloseTimeoutSpeculativeWorkflowTask() {
	tv := testvars.New(s.T().Name())

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.Any(),
		Namespace:           s.namespace,
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second), // Important!
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	tv = tv.WithRunID(startResp.GetRunId())

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			// This doesn't matter because WT times out before update is applied.
			return s.acceptUpdateCommands(tv, "1"), nil
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
			commands := append(s.acceptUpdateCommands(tv, "1"),
				&commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				})
			return commands, nil
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
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]

			// This doesn't matter because WT times out before update is applied.
			return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
		case 3:
			// Update is still in registry and was sent again.
			updRequestMsg := task.Messages[0]

			return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start activity using existing workflow task.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Try to process update in workflow, but it takes more than WT timeout. So, WT times out.
	_, _, err = poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.Error(err)
	s.Equal("Workflow task not found.", err.Error())

	// New normal WT was created on server after speculative WT has timed out.
	// It will accept and complete update first and workflow itself with the same WT.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.EqualValues(tv.String("success-result", "1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)
	s.Nil(updateResp.GetWorkflowTask())

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

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

func (s *integrationSuite) TestUpdateWorkflow_ScheduleToCloseTimeoutSpeculativeWorkflowTask() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			// Speculative WT, timed out on sticky task queue. Server sent full history with sticky timeout event.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskTimedOut
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted`, history)
			return nil, nil
		case 3:
			s.EqualHistory(`
 10 WorkflowTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, history)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
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
			// Reject update, but WT still will be in the history due to timeout on sticky queue.
			return s.rejectUpdateMessages(tv, updRequestMsg, "1"), nil
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
		TaskQueue:                    tv.TaskQueue(),
		StickyTaskQueue:              tv.StickyTaskQueue(),
		StickyScheduleToStartTimeout: 1 * time.Second, // Important!
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// Drain first WT and respond with sticky enabled response to enable sticky task queue.
	_, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 1, 5)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Wait for sticky timeout to fire.
	time.Sleep(poller.StickyScheduleToStartTimeout + 100*time.Millisecond)

	// Try to process update in workflow, poll from normal task queue.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.NoError(err)
	s.NotNil(updateResp)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskTimedOut
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
}

func (s *integrationSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			// Terminate workflow while speculative WT is running.
			_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace:         s.namespace,
				WorkflowExecution: tv.WorkflowExecution(),
				Reason:            tv.Any(),
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
			return s.acceptCompleteUpdateCommands(tv, "1"), nil
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
			return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		oneSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 1*time.Second)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(oneSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.HandlerName(),
					Args: payloads.EncodeString(tv.Any()),
				},
			},
		})
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// Process update in workflow.
	_, updateResp, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(false, false, false, false, 1, 5, true, nil)
	s.Error(err)
	s.IsType(err, (*serviceerror.NotFound)(nil))
	s.ErrorContains(err, "Workflow task not found.")
	s.Nil(updateResp)
	<-updateResultCh

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

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

	msResp, err := s.adminClient.DescribeMutableState(NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	// completion_event_batch_id should point to WTFailed event.
	s.EqualValues(8, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId())
}

func (s *integrationSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with update unrelated command.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		oneSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 1*time.Second)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(oneSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.HandlerName(),
					Args: payloads.EncodeString(tv.Any()),
				},
			},
		})
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()
	time.Sleep(500 * time.Millisecond) // This is to make sure that update gets to the server before the next Terminate call.

	// Terminate workflow after speculative WT is scheduled but not started.
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: tv.WorkflowExecution(),
		Reason:            tv.Any(),
	})
	s.NoError(err)

	<-updateResultCh

	s.Equal(1, wtHandlerCalls)
	s.Equal(1, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionTerminated`, events)

	msResp, err := s.adminClient.DescribeMutableState(NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	// completion_event_batch_id should point to WFTerminated event.
	s.EqualValues(6, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId())
}

func (s *integrationSuite) TestUpdateWorkflow_CompleteWorkflow_TerminateUpdate() {
	testCases := []struct {
		Name         string
		UpdateErrMsg string
		Commands     func(tv *testvars.TestVars) []*commandpb.Command
		Messages     func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message
	}{
		{
			Name:         "requested",
			UpdateErrMsg: "update has been terminated",
			Commands:     func(_ *testvars.TestVars) []*commandpb.Command { return nil },
			Messages:     func(_ *testvars.TestVars, _ *protocolpb.Message) []*protocolpb.Message { return nil },
		},
		{
			Name:         "accepted",
			UpdateErrMsg: "update has been terminated",
			Commands:     func(tv *testvars.TestVars) []*commandpb.Command { return s.acceptUpdateCommands(tv, "1") },
			Messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.acceptUpdateMessages(tv, updRequestMsg, "1")
			},
		},
		{
			Name:         "completed",
			UpdateErrMsg: "",
			Commands:     func(tv *testvars.TestVars) []*commandpb.Command { return s.acceptCompleteUpdateCommands(tv, "1") },
			Messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.acceptCompleteUpdateMessages(tv, updRequestMsg, "1")
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			tv := testvars.New(t.Name())

			tv = s.startWorkflow(tv)

			wtHandlerCalls := 0
			wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with update unrelated command.
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: tv.InfiniteTimeout(),
						}},
					}}, nil
				case 2:
					return append(tc.Commands(tv), &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					}), nil
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
					return tc.Messages(tv, updRequestMsg), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &TaskPoller{
				Engine:              s.engine,
				Namespace:           s.namespace,
				TaskQueue:           tv.TaskQueue(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain exiting first workflow task.
			_, err := poller.PollAndProcessWorkflowTask(true, false)
			s.NoError(err)

			updateResultCh := make(chan struct{})
			go func(updateErrMsg string) {
				resp, err1 := s.sendUpdate(tv, "1")
				if updateErrMsg == "" {
					s.NoError(err1)
					s.NotNil(resp)
				} else {
					s.Error(err1)
					s.ErrorContains(err1, updateErrMsg)
					s.Nil(resp)
				}
				updateResultCh <- struct{}{}
			}(tc.UpdateErrMsg)

			// Complete workflow.
			_, err = poller.PollAndProcessWorkflowTask(false, false)
			s.NoError(err)
			<-updateResultCh

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)
		})
	}
}
