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
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/runtime"
	"go.temporal.io/server/service/history/workflow/update"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testvars"
)

func (s *FunctionalSuite) startWorkflow(tv *testvars.TestVars) *testvars.TestVars {
	s.T().Helper()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    tv.Any().String(),
		Namespace:    s.namespace,
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	return tv.WithRunID(startResp.GetRunId())
}

func (s *FunctionalSuite) sendUpdateNoError(tv *testvars.TestVars, updateID string) *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	resp, err := s.sendUpdate(tv, updateID)
	// It is important to do assert here to fail fast without trying to process update in wtHandler.
	assert.NoError(s.T(), err)
	return resp
}

func (s *FunctionalSuite) sendUpdate(tv *testvars.TestVars, updateID string) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	return s.sendUpdateWithWaitPolicy(tv, updateID, nil)
}

func (s *FunctionalSuite) sendUpdateWaitPolicyAccepted(tv *testvars.TestVars, updateID string) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	return s.sendUpdateWithWaitPolicy(tv, updateID, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func (s *FunctionalSuite) sendUpdateWithWaitPolicy(tv *testvars.TestVars, updateID string, waitPolicy *updatepb.WaitPolicy) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
	s.T().Helper()
	return s.engine.UpdateWorkflowExecution(NewContext(), &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: tv.WorkflowExecution(),
		WaitPolicy:        waitPolicy,
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateID)},
			Input: &updatepb.Input{
				Name: tv.HandlerName(),
				Args: payloads.EncodeString("args-value-of-" + tv.UpdateID(updateID)),
			},
		},
	})
}

func (s *FunctionalSuite) pollUpdate(tv *testvars.TestVars, updateID string, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	s.T().Helper()
	return s.engine.PollWorkflowExecutionUpdate(NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: s.namespace,
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: tv.WorkflowExecution(),
			UpdateId:          tv.UpdateID(updateID),
		},
		WaitPolicy: waitPolicy,
	})
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_AcceptComplete() {
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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT events are not written to the history yet.
  6 WorkflowTaskStarted
`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(5, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
			_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow.
			res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
			s.NoError(err)
			updateResp := res.NewTask
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Test non-blocking poll
			for _, waitPolicy := range []*updatepb.WaitPolicy{{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED}, nil} {
				pollUpdateResp, err := s.pollUpdate(tv, "1", waitPolicy)
				s.NoError(err)
				s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, pollUpdateResp.Stage)
				s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, pollUpdateResp.Outcome.GetSuccess()))
				s.True(len(pollUpdateResp.UpdateRef.GetWorkflowExecution().RunId) > 0)
			}

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled // Was speculative WT...
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // ...and events were written to the history when WT completes.  
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted
`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewNormalWorkflowTask_AcceptComplete() {
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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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
					s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Normal WT because there is ActivityTaskScheduled(5) event after WorkflowTaskCompleted(4).
  7 WorkflowTaskStarted
`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, task.History)
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
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow.
			res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
			s.NoError(err)
			updateResp := res.NewTask
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  9 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 6} // WTScheduled event which delivered update to the worker.
 10 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 9}
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted
`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_FirstNormalScheduledWorkflowTask_AcceptComplete() {

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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted // First normal WT. No speculative WT was created.
`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 2:
					s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, task.History)
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
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(2, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
				Identity:            tv.WorkerIdentity(),
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
			res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
			s.NoError(err)
			updateResp := res.NewTask

			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5}
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionCompleted`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_NormalScheduledWorkflowTask_AcceptComplete() {

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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled // This WT was already created by signal and no speculative WT was created.
  7 WorkflowTaskStarted`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted
 10 WorkflowExecutionUpdateCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, task.History)
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
					s.Require().True(len(task.Messages) > 0, "Task has no messages", task)
					updRequestMsg := task.Messages[0]
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)

			// Send signal to schedule new WT.
			err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()
			// Signal creates WFT, and it is important to wait for update to get blocked,
			// to make sure that poll bellow won't poll just for WFT from signal.
			runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

			// Process update in workflow. It will be attached to existing WT.
			res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
			s.NoError(err)
			updateResp := res.NewTask

			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 6} // WTScheduled event which delivered update to the worker.
 10 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 9}
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewSpeculativeFromStartedWorkflowTask_Rejected() {

	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Send update after 1st WT has started.
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()
			// To make sure that 1st update gets to the sever while WT1 is running.
			runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)
			// Completes WT with empty command list to create next WT as speculative.
			return nil, nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT2 which was created while completing WT1.
  6 WorkflowTaskStarted`, task.History)
			// Message handler rejects update.
			return nil, nil
		case 3:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted // Speculative WT2 disappeared and new normal WT was created.
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)

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
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT which starts 1st update.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	wt1Resp := res.NewTask

	// Reject update in 2nd WT.
	wt2Resp, err := poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, wt2Resp.ResetHistoryEventId)

	// Send signal to create WT.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
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
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewNormalFromStartedWorkflowTask_Rejected() {

	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Send update after 1st WT has started.
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()
			// To make sure that 1st update gets to the sever while WT1 is running.
			runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)
			// Completes WT with update unrelated commands to prevent next WT to be speculative.
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
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Normal WT2 which was created while completing WT1.
  7 WorkflowTaskStarted`, task.History)
			// Message handler rejects update.
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted // New normal WT is created.
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)

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
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT which starts 1st update.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	wt1Resp := res.NewTask

	// Reject update in 2nd WT.
	wt2Resp, err := poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), true)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, wt2Resp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(wt2Resp.GetWorkflowTask(), false)
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
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_ValidateWorkerMessages() {
	testCases := []struct {
		Name                     string
		RespondWorkflowTaskError string
		MessageFn                func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message
		CommandFn                func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command
	}{
		{
			Name:                     "message-update-id-not-found-and-accepted-request-not-set",
			RespondWorkflowTaskError: "wasn't found",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: tv.WithUpdateID("bogus-update-id").UpdateID(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  nil, // Important not to pass original request back.
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			Name:                     "message-update-id-not-found-and-accepted-request-is-set",
			RespondWorkflowTaskError: "",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: tv.WithUpdateID("lost-update-id").UpdateID(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest, // Update will be resurrected from original request.
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: tv.Any().Payloads(),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted", "1"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-accepted", "2"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: tv.Any().Payloads(),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: tv.Any().Payloads(),
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
							Meta: updRequest.GetMeta(),
							Outcome: &updatepb.Outcome{
								Value: &updatepb.Outcome_Success{
									Success: tv.Any().Payloads(),
								},
							},
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)

			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				if tc.CommandFn == nil {
					return nil, nil
				}
				return tc.CommandFn(tv, task.History), nil
			}

			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				if tc.MessageFn == nil {
					return nil, nil
				}
				updRequestMsg := task.Messages[0]
				return tc.MessageFn(tv, updRequestMsg), nil
			}

			poller := &TaskPoller{
				Engine:              s.engine,
				Namespace:           s.namespace,
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
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
							Args: tv.Any().Payloads(),
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
			_, err := poller.PollAndProcessWorkflowTask()
			if tc.RespondWorkflowTaskError != "" {
				require.Error(s.T(), err, "RespondWorkflowTaskCompleted should return an error contains `%v`", tc.RespondWorkflowTaskError)
				require.Contains(s.T(), err.Error(), tc.RespondWorkflowTaskError)
			} else {
				require.NoError(s.T(), err)
			}
			<-updateResultCh
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewStickySpeculativeWorkflowTask_AcceptComplete() {
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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					// This WT contains partial history because sticky was enabled.
					s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 3:
					s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(5, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
				Identity:                     tv.WorkerIdentity(),
				WorkflowTaskHandler:          wtHandler,
				MessageHandler:               msgHandler,
				Logger:                       s.Logger,
				T:                            s.T(),
			}

			// Drain existing first WT from regular task queue, but respond with sticky queue enabled response, next WT will go to sticky queue.
			_, err := poller.PollAndProcessWorkflowTask(WithRespondSticky)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				// This is to make sure that next sticky poller reach to server first.
				// And when update comes, stick poller is already available.
				time.Sleep(500 * time.Millisecond)
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow task (it is sticky).
			res, err := poller.PollAndProcessWorkflowTask(WithPollSticky, WithRetries(1), WithForceNewWorkflowTask)
			s.NoError(err)
			updateResp := res.NewTask
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, updateResp.ResetHistoryEventId)

			// Complete workflow.
			completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewStickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			// Worker gets full history because update was issued after sticky worker is gone.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
		Identity:                     tv.WorkerIdentity(),
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// Drain existing WT from regular task queue, but respond with sticky enabled response to enable stick task queue.
	_, err := poller.PollAndProcessWorkflowTask(WithRespondSticky, WithRetries(1))
	s.NoError(err)

	s.Logger.Info("Sleep 10+ seconds to make sure stickyPollerUnavailableWindow time has passed.")
	time.Sleep(10*time.Second + 100*time.Millisecond)
	s.Logger.Info("Sleep 10+ seconds is done.")

	// Now send an update. It should try sticky task queue first, but got "StickyWorkerUnavailable" error
	// and resend it to normal.
	// This can be observed in wtHandler: if history is partial => sticky task queue is used.
	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow task from non-sticky task queue.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_FirstNormalScheduledWorkflowTask_Reject() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
			return nil, nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, task.History)
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(2, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
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
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled // First normal WT was scheduled before update and therefore all 3 events have to be written even if update was rejected.
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Empty completed WT. No new events were created after it.
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_Reject() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return nil, nil
		case 3:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled // Speculative WT was dropped and history starts from 5 again.
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, updateResp.ResetHistoryEventId)

	// Send signal to create WT.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
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
  5 WorkflowExecutionSignaled // Speculative WT is not present in the history.
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewNormalWorkflowTask_Reject() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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
  6 WorkflowTaskScheduled // Normal WT because there is ActivityTaskScheduled(5) event.
  7 WorkflowTaskStarted
`, task.History)
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  6 WorkflowTaskScheduled // Normal WT (6-8) presents in the history even though update was rejected.
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_1stAccept_2ndAccept_2ndComplete_1stComplete() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
			return s.UpdateAcceptCommands(tv, "1"), nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted // 1st update is accepted.
  6 WorkflowTaskScheduled // New normal WT is created because of the 2nd update.
  7 WorkflowTaskStarted`, task.History)
			return s.UpdateAcceptCommands(tv, "2"), nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted // 2nd update is accepted.
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
			return s.UpdateCompleteCommands(tv, "2"), nil
		case 4:
			s.EqualHistory(`
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted // 2nd update is completed.
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted`, task.History)
			return s.UpdateCompleteCommands(tv, "1"), nil
		case 5:
			s.EqualHistory(`
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted // 1st update is completed.
 18 WorkflowTaskScheduled
 19 WorkflowTaskStarted`, task.History)
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
			upd1Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd1RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg = task.Messages[0]
			upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("2"), decodeString(s, upd2Request.GetInput().GetArgs()))
			s.EqualValues(6, upd2RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd2RequestMsg)
			return s.UpdateCompleteMessages(tv, upd2RequestMsg, "2"), nil
		case 4:
			s.NotNil(upd1RequestMsg)
			return s.UpdateCompleteMessages(tv, upd1RequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh2 <- s.sendUpdateNoError(tv, "2")
	}()

	// Poll for WT2 which 2nd update. Accept update2.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateAcceptResp2 := res.NewTask
	s.NotNil(updateAcceptResp2)
	s.EqualValues(0, updateAcceptResp2.ResetHistoryEventId)

	// Complete update2 in WT3.
	updateCompleteResp2, err := poller.HandlePartialWorkflowTask(updateAcceptResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp2)
	updateResult2 := <-updateResultCh2
	s.EqualValues("success-result-of-"+tv.UpdateID("2"), decodeString(s, updateResult2.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp2.ResetHistoryEventId)

	// Complete update1 in WT4.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateCompleteResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WF in WT5.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), false)
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
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 6} // WTScheduled event which delivered update to the worker.
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 9}
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5}
 18 WorkflowTaskScheduled
 19 WorkflowTaskStarted
 20 WorkflowTaskCompleted
 21 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_1stAccept_2ndReject_1stComplete() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
			return s.UpdateAcceptCommands(tv, "1"), nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted // 1st update is accepted.
  6 WorkflowTaskScheduled // Normal WT because of WorkflowExecutionUpdateAccepted(5) event.
  7 WorkflowTaskStarted
`, task.History)
			// Message handler rejects 2nd update.
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)
			return s.UpdateCompleteCommands(tv, "1"), nil
		case 4:
			s.EqualHistory(`
 11 WorkflowTaskCompleted
 12 WorkflowExecutionUpdateCompleted // 1st update is completed.
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted`, task.History)
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
			upd1Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd1RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg := task.Messages[0]
			upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("2"), decodeString(s, upd2Request.GetInput().GetArgs()))
			s.EqualValues(6, upd2RequestMsg.GetEventId())
			return s.UpdateRejectMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd1RequestMsg)
			return s.UpdateCompleteMessages(tv, upd1RequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh2 <- s.sendUpdateNoError(tv, "2")
	}()

	// Poll for WT2 which 2nd update. Reject update2.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateRejectResp2 := res.NewTask
	s.NotNil(updateRejectResp2)
	s.EqualValues(0, updateRejectResp2.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

	updateResult2 := <-updateResultCh2
	s.Equal("rejection-of-"+tv.UpdateID("2"), updateResult2.GetOutcome().GetFailure().GetMessage())

	// Complete update1 in WT3.
	updateCompleteResp1, err := poller.HandlePartialWorkflowTask(updateRejectResp2.GetWorkflowTask(), true)
	s.NoError(err)
	s.NotNil(updateCompleteResp1)
	updateResult1 := <-updateResultCh1
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateCompleteResp1.ResetHistoryEventId)

	// Complete WT4.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateCompleteResp1.GetWorkflowTask(), false)
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
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted // WT which had rejected update.
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5}
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_FailSpeculativeWorkflowTask() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCommands(tv, "1"), nil
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted`, task.History)
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
			s.EqualValues(5, updRequestMsg.GetEventId())

			// Emulate bug in worker/SDK update handler code. Return malformed acceptance response.
			return []*protocolpb.Message{
				{
					Id:                 tv.MessageID("update-accepted", "1"),
					ProtocolInstanceId: tv.Any().String(),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  nil, // must not be nil.
					}),
				},
			}, nil
		case 3:
			// 2nd attempt has same updates attached to it.
			updRequestMsg := task.Messages[0]
			s.EqualValues(8, updRequestMsg.GetEventId())
			wtHandlerCalls++ // because it won't be called for case 3 but counter should be in sync.
			// Fail WT one more time. Although 2nd attempt is normal WT, it is also transient and shouldn't appear in the history.
			// Returning error will cause the poller to fail WT.
			return nil, errors.New("malformed request")
		case 4:
			// 3rd attempt UpdateWorkflowExecution call has timed out but the
			// update is still running
			updRequestMsg := task.Messages[0]
			s.EqualValues(8, updRequestMsg.GetEventId())
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
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
					Args: tv.Any().Payloads(),
				},
			},
		})
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), "UpdateWorkflowExecution must timeout after 2 seconds")
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// Try to accept update in workflow: get malformed response.
	_, err = poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.Contains(err.Error(), "wasn't found")
	// New normal (but transient) WT will be created but not returned.

	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Try to accept update in workflow 2nd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask()
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Wait for UpdateWorkflowExecution to timeout.
	// This does NOT remove update from registry
	<-updateResultCh

	// Try to accept update in workflow 3rd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask()
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Complete workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_ConvertStartedSpeculativeWorkflowTaskToNormal_BecauseOfBufferedSignal() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT. Events 5 and 6 are written into the history when signal is received.
  6 WorkflowTaskStarted
`, task.History)
			// Send signal which will be buffered. This will persist MS and speculative WT must be converted to normal.
			err := s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
			s.NoError(err)
			return nil, nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionSignaled // It was buffered and got to the history after WT is completed.
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)
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

			s.EqualValues(5, updRequestMsg.GetEventId())

			// Update is rejected but corresponding speculative WT will be in the history anyway, because it was converted to normal due to buffered signal.
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // Update was rejected on speculative WT, but events 5-7 are in the history because of buffered signal.
  8 WorkflowExecutionSignaled
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_ConvertScheduledSpeculativeWorkflowTaskToNormal_BecauseOfSignal() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // It was initially speculative WT but was already converted to normal when signal was received.
  6 WorkflowExecutionSignaled
  7 WorkflowTaskStarted`, task.History)
			return nil, nil
		case 3:
			s.EqualHistory(`
  8 WorkflowTaskCompleted
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)
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

			// Update is rejected but corresponding speculative WT was already converted to normal,
			// and will be in the history anyway.
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()
	// This is to make sure that update gets to the server before the next Signal call.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Send signal which will NOT be buffered because speculative WT is not started yet (only scheduled).
	// This will persist MS and speculative WT must be converted to normal.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted // Update was rejected but WT events 5,7,8 are in the history because of signal.
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StartToCloseTimeoutSpeculativeWorkflowTask() {
	tv := testvars.New(s.T().Name())

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.Any().String(),
		Namespace:           s.namespace,
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second), // Important!
	}

	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	tv = tv.WithRunID(startResp.GetRunId())

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			// Emulate slow worker: sleep little more than WT timeout.
			time.Sleep(request.WorkflowTaskTimeout.AsDuration() + 100*time.Millisecond)
			// This doesn't matter because WT times out before update is applied.
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 3:
			// Speculative WT timed out and retried as normal WT.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskTimedOut
  8 WorkflowTaskScheduled {"Attempt":2 } // Transient WT.
  9 WorkflowTaskStarted`, task.History)
			commands := append(s.UpdateAcceptCompleteCommands(tv, "1"),
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
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			// Update is still in registry and was sent again.
			updRequestMsg := task.Messages[0]

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Try to process update in workflow, but it takes more than WT timeout. So, WT times out.
	_, err = poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.Error(err)
	s.Equal("Workflow task not found.", err.Error())

	// New normal WT was created on server after speculative WT has timed out.
	// It will accept and complete update first and workflow itself with the same WT.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskTimedOut // Timeout of speculative WT writes events 5-7
  8 WorkflowTaskScheduled {"Attempt":2 }
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 8} // WTScheduled event which delivered update to the worker.
 12 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 11} 
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_ScheduleToStartTimeoutSpeculativeWorkflowTask() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			// Speculative WT timed out on sticky task queue. Server sent full history with sticky timeout event.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT.
  8 WorkflowTaskStarted`, task.History)
			return nil, nil
		case 3:
			s.EqualHistory(`
  9 WorkflowTaskCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:                     tv.WorkerIdentity(),
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// Drain first WT and respond with sticky enabled response to enable sticky task queue.
	_, err := poller.PollAndProcessWorkflowTask(WithRespondSticky, WithRetries(1))
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// To make sure that update got to the server before test start to wait for StickyScheduleToStartTimeout.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	s.Logger.Info("Wait for sticky timeout to fire. Sleep poller.StickyScheduleToStartTimeout+ seconds.", tag.NewDurationTag("StickyScheduleToStartTimeout", poller.StickyScheduleToStartTimeout))
	time.Sleep(poller.StickyScheduleToStartTimeout + 100*time.Millisecond)
	s.Logger.Info("Sleep is done.")

	// Try to process update in workflow, poll from normal task queue.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	s.NotNil(updateResp)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT was written into the history because of timeout.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Second attempt WT is normal WT (clear stickiness reset attempts count).
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted // Normal WT is completed and events are in the history even update was rejected.
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_ScheduleToStartTimeoutSpeculativeWorkflowTask_NormalTaskQueue() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled {"TaskQueue": {"Kind": 1}} // Speculative WT timed out on normal(1) task queue.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT is scheduled.
  8 WorkflowTaskStarted
`, task.History)
			return nil, nil
		case 3:
			s.EqualHistory(`
  9 WorkflowTaskCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(7, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain existing WT from normal task queue.
	_, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)

	// Now send an update. It will create a speculative WT on normal task queue,
	// which will time out in 5 seconds and create new normal WT.
	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// To make sure that update got to the server before test start to wait for SpeculativeWorkflowTaskScheduleToStartTimeout.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// TODO: it would be nice to shutdown matching before sending an update to emulate case which is actually being tested here.
	//  But test infrastructure doesn't support it. 5 seconds sleep will cause same observable effect.
	s.Logger.Info("Sleep 5+ seconds to make sure tasks.SpeculativeWorkflowTaskScheduleToStartTimeout time has passed.")
	time.Sleep(5*time.Second + 100*time.Millisecond)
	s.Logger.Info("Sleep 5+ seconds is done.")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled {"TaskQueue": {"Kind": 1}} // Speculative WT timed out on normal(1) task queue.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT is scheduled. Even update was rejected, WT is in the history.
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			// Terminate workflow while speculative WT is running.
			_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace:         s.namespace,
				WorkflowExecution: tv.WorkflowExecution(),
				Reason:            tv.Any().String(),
			})
			s.NoError(err)

			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
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
					Args: tv.Any().Payloads(),
				},
			},
		})
		assert.Error(s.T(), err1)
		var notFound *serviceerror.NotFound
		assert.ErrorAs(s.T(), err1, &notFound)
		assert.Equal(s.T(), "workflow execution already completed", err1.Error())
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// Process update in workflow.
	_, err = poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.Error(err)
	s.IsType(err, (*serviceerror.NotFound)(nil))
	s.ErrorContains(err, "Workflow task not found.")
	<-updateResultCh

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT was converted to normal WT during termination.
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowExecutionTerminated`, events)

	msResp, err := s.adminClient.DescribeMutableState(NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	s.EqualValues(7, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WTFailed event")
}

func (s *FunctionalSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
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
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
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
					Args: tv.Any().Payloads(),
				},
			},
		})
		assert.Error(s.T(), err1)
		var notFound *serviceerror.NotFound
		assert.ErrorAs(s.T(), err1, &notFound)
		assert.Equal(s.T(), "workflow execution already completed", err1.Error())
		assert.Nil(s.T(), updateResponse)
		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()
	// This is to make sure that update gets to the server before the next Terminate call.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Terminate workflow after speculative WT is scheduled but not started.
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: tv.WorkflowExecution(),
		Reason:            tv.Any().String(),
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
  5 WorkflowExecutionTerminated // Speculative WTScheduled event is not written to history if WF is terminated.
`, events)

	msResp, err := s.adminClient.DescribeMutableState(NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	s.EqualValues(5, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WFTerminated event")
}

func (s *FunctionalSuite) TestUpdateWorkflow_CompleteWorkflow_AbortUpdates() {
	type testCase struct {
		Name          string
		Description   string
		UpdateErr     string
		UpdateFailure string
		Commands      func(tv *testvars.TestVars) []*commandpb.Command
		Messages      func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message
	}
	testCases := []testCase{
		{
			Name:          "admitted",
			Description:   "update in stateAdmitted must get an error",
			UpdateErr:     "workflow execution already completed",
			UpdateFailure: "",
			Commands:      func(_ *testvars.TestVars) []*commandpb.Command { return nil },
			Messages:      func(_ *testvars.TestVars, _ *protocolpb.Message) []*protocolpb.Message { return nil },
		},
		{
			Name:          "accepted",
			Description:   "update in stateAccepted must get an error",
			UpdateErr:     "workflow execution already completed",
			UpdateFailure: "",
			Commands:      func(tv *testvars.TestVars) []*commandpb.Command { return s.UpdateAcceptCommands(tv, "1") },
			Messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateAcceptMessages(tv, updRequestMsg, "1")
			},
		},
		{
			Name:          "completed",
			Description:   "completed update must not be affected by workflow completion",
			UpdateErr:     "",
			UpdateFailure: "",
			Commands:      func(tv *testvars.TestVars) []*commandpb.Command { return s.UpdateAcceptCompleteCommands(tv, "1") },
			Messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1")
			},
		},
		{
			Name:          "rejected",
			Description:   "rejected update must be rejected with rejection from workflow",
			UpdateErr:     "",
			UpdateFailure: "rejection-of-", // Rejection from workflow.
			Commands:      func(tv *testvars.TestVars) []*commandpb.Command { return nil },
			Messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateRejectMessages(tv, updRequestMsg, "1")
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
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
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
			s.NoError(err)

			updateResultCh := make(chan struct{})
			go func(tc testCase) {
				halfSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 500*time.Millisecond)
				defer cancel()

				resp, err1 := s.engine.UpdateWorkflowExecution(halfSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
					Namespace:         s.namespace,
					WorkflowExecution: tv.WorkflowExecution(),
					Request: &updatepb.Request{
						Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
						Input: &updatepb.Input{
							Name: tv.Any().String(),
							Args: tv.Any().Payloads(),
						},
					},
				})

				if tc.UpdateErr != "" {
					assert.Error(s.T(), err1, tc.Description)
					assert.Contains(s.T(), err1.Error(), tc.UpdateErr, tc.Description)
				} else {
					assert.NoError(s.T(), err1, tc.Description)
				}

				if tc.UpdateFailure != "" {
					assert.NotNil(s.T(), resp.GetOutcome().GetFailure(), tc.Description)
					assert.Contains(s.T(), resp.GetOutcome().GetFailure().GetMessage(), tc.UpdateFailure, tc.Description)
				} else {
					assert.Nil(s.T(), resp.GetOutcome().GetFailure(), tc.Description)
				}

				updateResultCh <- struct{}{}
			}(tc) // To prevent capturing of tc range value by reference.

			// Complete workflow.
			_, err = poller.PollAndProcessWorkflowTask()
			s.NoError(err)
			<-updateResultCh

			// Check that update didn't block workflow completion.
			descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: tv.WorkflowExecution(),
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_Heartbeat() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Events (5 and 6) are for speculative WT, but they won't disappear after reject because speculative WT is converted to normal during heartbeat.
  6 WorkflowTaskStarted
`, task.History)
			// Heartbeat from speculative WT (no messages, no commands).
			return nil, nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowTaskScheduled // New WT (after heartbeat) is normal and won't disappear from the history after reject.
  9 WorkflowTaskStarted
`, task.History)
			// Reject update.
			return nil, nil
		case 4:
			s.EqualHistory(`
 10 WorkflowTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted`, task.History)
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
	var updRequestMsg *protocolpb.Message
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			return nil, nil
		case 2:
			s.Len(task.Messages, 1)
			updRequestMsg = task.Messages[0]
			s.EqualValues(5, updRequestMsg.GetEventId())
			return nil, nil
		case 3:
			s.Empty(task.Messages)
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Heartbeat from workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	heartbeatResp := res.NewTask

	// Reject update from workflow.
	updateResp, err := poller.HandlePartialWorkflowTask(heartbeatResp.GetWorkflowTask(), true)
	s.NoError(err)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId, "no reset of event ID should happened after update rejection because of heartbeat")

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // Heartbeat response.
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted // After heartbeat new normal WT was created and events are written into the history even update is rejected.
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewScheduledSpeculativeWorkflowTaskLost_BecauseOfShardMove() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
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
			s.Empty(task.Messages, "update must be lost due to shard reload")
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		halfSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 500*time.Millisecond)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(halfSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.Any().String(),
					Args: tv.Any().Payloads(),
				},
			},
		})
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)

		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// Close shard, Speculative WT with update will be lost.
	s.closeShard(tv.WorkflowID())

	// Ensure, there is no new WT.
	pollCtx, cancel := context.WithTimeout(NewContext(), common.MinLongPollTimeout+100*time.Millisecond)
	defer cancel()
	pollResponse, err := s.engine.PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.Nil(pollResponse.Messages)

	// Wait for update API call to timeout.
	<-updateResultCh

	// Send signal to schedule new WT.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewStartedSpeculativeWorkflowTaskLost_BecauseOfShardMove() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT. Events 5 and 6 will be lost.
  6 WorkflowTaskStarted
`, task.History)

			// Close shard. NotFound error will be returned to RespondWorkflowTaskCompleted.
			s.closeShard(tv.WorkflowID())

			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 3:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
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
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			s.Empty(task.Messages, "update must be lost due to shard reload")
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		halfSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 500*time.Millisecond)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(halfSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.Any().String(),
					Args: tv.Any().Payloads(),
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
	_, err = poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.ErrorContains(err, "Workflow task not found")

	<-updateResultCh

	// Send signal to schedule new WT.
	err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
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
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_FirstNormalWorkflowTask_UpdateResurrectedAfterShardMove() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
`, task.History)
			// Close shard. Update registry is lost but update will be resurrected in registry from acceptance message.
			s.closeShard(tv.WorkflowID())
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
`, task.History)
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
			s.EqualValues(2, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 2:
			s.Empty(task.Messages, "update must be processed and not delivered again")
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh := make(chan struct{})
	updateWorkflowFn := func() {
		halfSecondTimeoutCtx, cancel := context.WithTimeout(NewContext(), 500*time.Millisecond)
		defer cancel()

		updateResponse, err1 := s.engine.UpdateWorkflowExecution(halfSecondTimeoutCtx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: tv.WorkflowExecution(),
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{
					Name: tv.Any().String(),
					Args: tv.Any().Payloads(),
				},
			},
		})

		// Even if update was resurrected on server and completed, original initiator
		// lost connection to it and times out. Following poll for update results returns them right away.
		assert.Error(s.T(), err1)
		assert.True(s.T(), common.IsContextDeadlineExceededErr(err1), err1)
		assert.Nil(s.T(), updateResponse)

		updateResultCh <- struct{}{}
	}
	go updateWorkflowFn()

	// To make sure that first WFT has update message.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Process update in workflow. Update won't be found on server but will be resurrected from acceptance message and completed.
	res, err := poller.PollAndProcessWorkflowTask(WithForceNewWorkflowTask)
	s.NoError(err)
	s.NotNil(res)

	updateResp := res.NewTask
	<-updateResultCh
	pollResult, err := s.pollUpdate(tv, "1", &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
	s.NoError(err)
	s.NotNil(pollResult)
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, pollResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

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
}

func (s *FunctionalSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_DeduplicateID() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
			s.Len(task.Messages, 1, "2nd update must be deduplicated by ID")
			updRequestMsg := task.Messages[0]

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			s.Empty(task.Messages, "2nd update must be deduplicated by ID")
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
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Send second update with the same ID.
	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh2 <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	updateResult2 := <-updateResultCh2
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult2.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted  {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_DeduplicateID() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			// Send second update with the same ID when WT is started but not completed.
			go func() {
				updateResultCh2 <- s.sendUpdateNoError(tv, "1")
			}()
			runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted`, task.History)
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
			s.Len(task.Messages, 1, "2nd update should not has reached server yet")
			updRequestMsg := task.Messages[0]
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			s.Empty(task.Messages, "2nd update must be deduplicated by ID ")
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
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	updateResult2 := <-updateResultCh2
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult2.GetOutcome().GetSuccess()))

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted  {"AcceptedEventId": 8}
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_CompletedSpeculativeWorkflowTask_DeduplicateID() {
	testCases := []struct {
		Name       string
		CloseShard bool
	}{
		{
			Name:       "no shard reload",
			CloseShard: false,
		},
		{
			Name:       "with shard reload",
			CloseShard: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T().Name())

			tv = s.startWorkflow(tv)

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
				case 3:
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
					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				case 3:
					s.Empty(task.Messages, "2nd update must be deduplicated by ID ")
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
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
			s.NoError(err)

			updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh <- s.sendUpdateNoError(tv, "1")
			}()

			// Process update in workflow.
			_, err = poller.PollAndProcessWorkflowTask()
			s.NoError(err)
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))

			if tc.CloseShard {
				// Close shard to make sure that for completed updates deduplication works even after shard reload.
				s.closeShard(tv.WorkflowID())
			}

			// Send second update with the same ID.
			updateResultCh2 := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
			go func() {
				updateResultCh2 <- s.sendUpdateNoError(tv, "1")
			}()

			// Ensure, there is no new WT.
			pollCtx, cancel := context.WithTimeout(NewContext(), common.MinLongPollTimeout+100*time.Millisecond)
			defer cancel()
			pollResponse, err := s.engine.PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.namespace,
				TaskQueue: tv.TaskQueue(),
				Identity:  tv.WorkerIdentity(),
			})
			s.NoError(err)
			s.Nil(pollResponse.Messages, "there must be no new WT")

			updateResult2 := <-updateResultCh2
			s.EqualValues(
				"success-result-of-"+tv.UpdateID("1"),
				decodeString(s, updateResult2.GetOutcome().GetSuccess()),
				"results of the first update must be available")

			// Send signal to schedule new WT.
			err = s.sendSignal(s.namespace, tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
			s.NoError(err)

			// Complete workflow.
			completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
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
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowExecutionSignaled
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
		})
	}
}

func (s *FunctionalSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_CloseShard_DifferentStartedId_Rejected() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Shard is reloaded, speculative WT is disappeared from server.
		Another update come in and second speculative WT is scheduled but not dispatched yet.
		An activity completes, it converts the 2nd speculative WT into normal one.
		The first speculative WT responds back, server fails request it because WorkflowTaskStarted event Id is mismatched.
		The second speculative WT responds back and server completes it.
	*/

	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Schedule activity.
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
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First WT will schedule activity and create a new WT.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	wt1Resp := res.NewTask

	// Drain 2nd WT (which is force created as requested) to make all events seen by SDK so following update can be speculative.
	_, err = poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
	s.NoError(err)
	s.EqualValues(0, wt1Resp.ResetHistoryEventId)

	// Send 1st update. It will create 3rd WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Poll 3rd speculative WT with 1st update.
	wt3, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt3)
	s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
	s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 1st update")
	s.EqualValues(10, wt3.StartedEventId)
	s.EqualValues(9, wt3.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt3.History)

	// Close shard, this will clear mutable state and speculative WT will disappear.
	s.closeShard(tv.WorkflowID())

	// Send 2nd update (with SAME updateId). This will create a 4th WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Before polling for the 4th speculative WT, process activity. This will convert 4th speculative WT to normal WT.
	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// Poll the 4th WT (not speculative anymore) but must have 2nd update.
	wt4, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt4)
	s.NotEmpty(wt4.TaskToken, "4th workflow task must have valid task token")
	s.Len(wt4.Messages, 1, "4th workflow task must have a message with 2nd update")
	s.EqualValues(12, wt4.StartedEventId)
	s.EqualValues(11, wt4.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 ActivityTaskStarted
	 11 ActivityTaskCompleted
	 12 WorkflowTaskStarted`, wt4.History)

	// Now try to complete 3rd WT (speculative). It should fail because WorkflowTaskStarted event Id is mismatched.
	_, err = s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt3.TaskToken,
		Commands:  s.UpdateAcceptCommands(tv, "1"),
		Messages:  s.UpdateAcceptMessages(tv, wt3.Messages[0], "1"),
	})
	s.Error(err)
	s.Contains(err.Error(), "Workflow task not found")

	// Complete 4th WT. It should succeed.
	_, err = s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt4.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages: s.UpdateAcceptMessages(tv, wt4.Messages[0], "1"),
	})
	s.NoError(err)

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
	  9 WorkflowTaskScheduled
	 10 ActivityTaskStarted {"ScheduledEventId":5}
	 11 ActivityTaskCompleted
	 12 WorkflowTaskStarted
	 13 WorkflowTaskCompleted
	 14 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":11}
	 15 ActivityTaskScheduled
	`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_CloseShard_SameStartedId_SameUpdateId_Accepted() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Shard is reloaded, speculative WT is disappeared from server.
		Another update come in and second speculative WT is dispatched to worker with same WT scheduled/started Id and update Id.
		The first speculative WT respond back, server reject it because startTime is different.
		The second speculative WT respond back, server accept it.
	*/
	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Schedule activity.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("1"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First WT will schedule activity and create a new WT.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	wt1Resp := res.NewTask

	// Drain 2nd WT (which is force created as requested) to make all events seem by SDK so following update can be speculative.
	_, err = poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
	s.NoError(err)
	s.EqualValues(0, wt1Resp.ResetHistoryEventId)

	// Send 1st update. It will create 3rd WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Poll 3rd speculative WT with 1st update.
	wt3, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt3)
	s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
	s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 1st update")
	s.EqualValues(10, wt3.StartedEventId)
	s.EqualValues(9, wt3.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt3.History)

	// Close shard, this will clear mutable state and speculative WT will disappear.
	s.closeShard(tv.WorkflowID())

	// Send 2nd update (with SAME updateId). This will create a 4th WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Poll for the 4th speculative WT.
	wt4, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt4)
	s.NotEmpty(wt4.TaskToken, "4th workflow task must have valid task token")
	s.Len(wt4.Messages, 1, "4th workflow task must have a message with 1st update")
	s.EqualValues(10, wt4.StartedEventId)
	s.EqualValues(9, wt4.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt4.History)

	// Now try to complete 3rd (speculative) WT, it should fail.
	_, err = s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt3.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("2"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages: s.UpdateAcceptMessages(tv, wt3.Messages[0], "1"),
	})
	s.IsType(&serviceerror.NotFound{}, err)

	// Try to complete 4th WT, it should succeed
	_, err = s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt4.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("2"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages: s.UpdateAcceptMessages(tv, wt4.Messages[0], "1"),
	})
	s.NoError(err, "2nd speculative WT should be completed because it has same WT scheduled/started Id and startTime matches the accepted message is valid (same update Id)")

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
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted
	 11 WorkflowTaskCompleted
	 12 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":9}
	 13 ActivityTaskScheduled
	`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_ClearMutableState_Accepted() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Mutable state cleared, speculative WT and update registry are disappeared from server.
		Another update come in, and second speculative WT is dispatched to worker with same WT scheduled/started Id but different update Id.
		The first speculative WT responds back, server rejected it (different start time).
		The second speculative WT responds back, server accepted it.
	*/

	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)

	testCtx := NewContext()
	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Schedule activity.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("1"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First WT will schedule activity and create a new WT.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	wt1Resp := res.NewTask

	// Drain 2nd WT (which is force created as requested) to make all events seen by SDK so following update can be speculative.
	_, err = poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
	s.NoError(err)
	s.EqualValues(0, wt1Resp.ResetHistoryEventId)

	// Send 1st update. It will create 3rd WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Poll 3rd speculative WT with 1st update.
	wt3, err := s.engine.PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt3)
	s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
	s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 1st update")
	s.EqualValues(10, wt3.StartedEventId)
	s.EqualValues(9, wt3.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt3.History)

	// DescribeMutableState will clear MS, cause the speculative WT and update registry to disappear.
	_, err = s.adminClient.DescribeMutableState(testCtx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)

	// Make sure UpdateWorkflowExecution call for the update "1" is retried and new (4th) WFT is created as speculative.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Send 2nd update (with DIFFERENT updateId). It re-use already create 4th WFT.
	go func() {
		_, _ = s.sendUpdate(tv, "2")
	}()

	// Make sure that updateID=2 reached server (and added to the 4th WFT) before WFT is polled.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	// Poll the 4th speculative WT.
	wt4, err := s.engine.PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt4)
	s.NotEmpty(wt4.TaskToken, "4th workflow task must have valid task token")
	s.Len(wt4.Messages, 2, "4th workflow task must have a message with 1st and 2nd updates")
	s.EqualValues(10, wt4.StartedEventId)
	s.EqualValues(9, wt4.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt4.History)

	// Now try to complete 3rd speculative WT, it should fail because start time does not match.
	_, err = s.engine.RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt3.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("2"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages:              s.UpdateAcceptMessages(tv, wt3.Messages[0], "1"),
		ReturnNewWorkflowTask: true,
	})
	s.IsType(&serviceerror.NotFound{}, err)

	// Complete of the 4th WT should succeed. It must accept both updates.
	wt5Resp, err := s.engine.RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt4.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			append(
				s.UpdateAcceptCommands(tv, "2"),
				&commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv.ActivityID("3"),
						ActivityType:           tv.ActivityType(),
						TaskQueue:              tv.TaskQueue(),
						ScheduleToCloseTimeout: tv.InfiniteTimeout(),
					}},
				},
				&commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv.ActivityID("4"),
						ActivityType:           tv.ActivityType(),
						TaskQueue:              tv.TaskQueue(),
						ScheduleToCloseTimeout: tv.InfiniteTimeout(),
					}},
				},
			)...),
		Messages: append(
			s.UpdateAcceptMessages(tv, wt4.Messages[0], "1"),
			s.UpdateAcceptMessages(tv, wt4.Messages[1], "2")...),
		ReturnNewWorkflowTask: true,
	})
	s.NoError(err)
	s.NotNil(wt5Resp)
	wt5 := wt5Resp.WorkflowTask
	s.Nil(wt5)

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
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted
	 11 WorkflowTaskCompleted
	 12 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":9}
	 13 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":9}
	 14 ActivityTaskScheduled
	 15 ActivityTaskScheduled
	`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_SameStartedId_DifferentUpdateId_Rejected() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Shard is reloaded, speculative WT and update registry are disappeared from server.
		Another update come in (with different update Id), and second speculative WT is dispatched to worker.
		The first speculative WT responds back, server fails WT because start time different.
		The second speculative WT responds back, server reject it.
	*/

	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)

	testCtx := NewContext()
	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Schedule activity.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("1"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First WT will schedule activity and create a new WT.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1), WithForceNewWorkflowTask)
	s.NoError(err)
	wt1Resp := res.NewTask

	// Drain 2nd WT (which is force created as requested) to make all events seen by SDK so following update can be speculative.
	_, err = poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
	s.NoError(err)
	s.EqualValues(0, wt1Resp.ResetHistoryEventId)

	// send update wf request, this will trigger speculative wft
	go func() {
		_, _ = s.sendUpdate(tv, "1")
	}()

	// Poll 3rd speculative WT.
	wt3, err := s.engine.PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt3)
	s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
	s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 1st update")
	s.EqualValues(10, wt3.StartedEventId)
	s.EqualValues(9, wt3.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt3.History)

	// Close shard, this will clear mutable state and update registry, and speculative WT3 will disappear.
	s.closeShard(tv.WorkflowID())

	// Send 2nd update (with DIFFERENT updateId). This will create a 4th WT as speculative.
	go func() {
		_, _ = s.sendUpdate(tv, "2")
	}()

	// Poll the 4th speculative WT which must have 2nd update.
	wt4, err := s.engine.PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt4)
	s.NotEmpty(wt4.TaskToken, "4th workflow task must have valid task token")
	s.Len(wt4.Messages, 1, "4th workflow task must have a message with 1st update")
	s.EqualValues(10, wt4.StartedEventId)
	s.EqualValues(9, wt4.Messages[0].GetEventId())
	s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted
	  8 WorkflowTaskCompleted
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted`, wt4.History)

	// Now try to complete 3rd speculative WT, it should fail.
	_, err = s.engine.RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt3.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "1"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("2"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages: s.UpdateAcceptMessages(tv, wt3.Messages[0], "1"),
	})
	s.Error(err, "Must fail because start time is different.")
	s.Contains(err.Error(), "Workflow task not found")

	// Now try to complete 4th speculative WT. It should also fail, because the previous attempt already mark the WT as failed.
	_, err = s.engine.RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: wt4.TaskToken,
		Commands: append(
			s.UpdateAcceptCommands(tv, "2"),
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("3"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}),
		Messages: s.UpdateAcceptMessages(tv, wt4.Messages[0], "2"),
	})
	s.NoError(err)

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
	  9 WorkflowTaskScheduled
	 10 WorkflowTaskStarted
	 11 WorkflowTaskCompleted
	 12 WorkflowExecutionUpdateAccepted
	 13 ActivityTaskScheduled
	`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_WorkerSkippedProcessing_RejectByServer() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	update2ResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			go func() {
				update2ResultCh <- s.sendUpdateNoError(tv, "2")
			}()
			// To make sure that gets to the sever while this WT is running.
			runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)
			return nil, nil
		case 3:
			s.EqualHistory(`
  4 WorkflowTaskCompleted // Speculative WT was dropped and history starts from 4 again.
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, task.History)
			commands := append(s.UpdateAcceptCompleteCommands(tv, "2"),
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
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.EqualValues(5, updRequestMsg.GetEventId())

			// Don't process update in WT.
			return nil, nil
		case 3:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("2"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.EqualValues(5, updRequestMsg.GetEventId())
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "2"), nil
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
		Identity:            "old_worker",
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Process 2nd WT which ignores update message.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("Workflow Update is rejected because it wasn't processed by worker. Probably, Workflow Update is not supported by the worker.", updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, updateResp.ResetHistoryEventId)

	// Process 3rd WT which completes 2nd update and workflow.
	update2Resp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(update2Resp)
	update2Result := <-update2ResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("2"), decodeString(s, update2Result.GetOutcome().GetSuccess()))

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // 1st speculative WT is not present in the history. This is 2nd speculative WT.
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_UpdateMessageInLastWFT() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableUpdateWorkflowExecutionAsyncAccepted.Key(): true,
	}
	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)

	messageId := "my-message-id"

	poller := &TaskPoller{
		Engine:    s.engine,
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			completeWorkflowCommand := &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			}
			return append(s.UpdateAcceptCommands(tv, messageId), completeWorkflowCommand), nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			return s.UpdateAcceptMessages(tv, task.Messages[0], messageId), nil
		},
		Logger: s.Logger,
		T:      s.T(),
	}

	updateResponse := make(chan error)
	pollResponse := make(chan error)
	go func() {
		_, err := s.sendUpdateWaitPolicyAccepted(tv, tv.UpdateID())
		updateResponse <- err
	}()
	go func() {
		// Blocks until the update request causes a WFT to be dispatched; then sends the update complete message
		// required for the update request to return.
		_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
		pollResponse <- err
	}()
	s.NoError(<-updateResponse)
	s.NoError(<-pollResponse)

	s.HistoryRequire.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 WorkflowExecutionUpdateAccepted
	6 WorkflowExecutionCompleted
	`, s.getHistory(s.namespace, tv.WorkflowExecution()))
}

func (s *FunctionalSuite) TestUpdateWorkflow_NewSpeculativeWorkflowTask_QueryFailureClearsWFContext() {
	tv := testvars.New(s.T().Name())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), decodeString(s, updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
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
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	updateResultCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	go func() {
		updateResultCh <- s.sendUpdateNoError(tv, "1")
	}()

	// Wait for update go through and speculative WFT to be created.
	runtime.WaitGoRoutineWithFn(s.T(), ((*update.Update)(nil)).WaitLifecycleStage, 1*time.Second)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryFn := func(resCh chan<- QueryResult) {
		// There is no query handler, and query timeout is ok for this test.
		// But first query must not time out before 2nd query reached server,
		// because 2 queries overflow the query buffer (default size 1),
		// which leads to clearing of WF context.
		shortCtx, cancel := context.WithTimeout(NewContext(), 100*time.Millisecond)
		defer cancel()
		queryResp, err := s.engine.QueryWorkflow(shortCtx, &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: tv.WorkflowExecution(),
			Query: &querypb.WorkflowQuery{
				QueryType: tv.Any().String(),
			},
		})
		resCh <- QueryResult{Resp: queryResp, Err: err}
	}

	query1ResultCh := make(chan QueryResult)
	query2ResultCh := make(chan QueryResult)
	go queryFn(query1ResultCh)
	go queryFn(query2ResultCh)
	query1Res := <-query1ResultCh
	query2Res := <-query2ResultCh
	s.Error(query1Res.Err)
	s.Error(query2Res.Err)
	s.Nil(query1Res.Resp)
	s.Nil(query2Res.Resp)

	var queryBufferFullErr *serviceerror.ResourceExhausted
	if common.IsContextDeadlineExceededErr(query1Res.Err) {
		s.True(common.IsContextDeadlineExceededErr(query1Res.Err), "one of query errors must be CDE")
		s.ErrorAs(query2Res.Err, &queryBufferFullErr, "one of query errors must `query buffer is full`")
		s.Contains(query2Res.Err.Error(), "query buffer is full", "one of query errors must `query buffer is full`")
	} else {
		s.ErrorAs(query1Res.Err, &queryBufferFullErr, "one of query errors must `query buffer is full`")
		s.Contains(query1Res.Err.Error(), "query buffer is full", "one of query errors must `query buffer is full`")
		s.True(common.IsContextDeadlineExceededErr(query2Res.Err), "one of query errors must be CDE")
	}

	// "query buffer is full" error clears WF context. If update registry is not cleared together with context (old behaviour),
	// then update stays there but speculative WFT which supposed to deliver it, is cleared.
	// Subsequent retry attempts of "UpdateWorkflowExecution" API wouldn't help, because update is deduped by registry,
	// and new WFT is not created. Update is not delivered to the worker until new WFT is created.
	// If registry is cleared together with WF context (current behaviour), retries of "UpdateWorkflowExecution"
	// will create new update and WFT.

	// Wait to make sure that UpdateWorkflowExecution call is retried, update and speculative WFT are recreated.
	time.Sleep(500 * time.Millisecond)

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), decodeString(s, updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.getHistory(s.namespace, tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Was speculative WT...
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // ...and events were written to the history when WT completes.  
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
`, events)
}

func (s *FunctionalSuite) TestUpdateWorkflow_AdmittedUpdatesAreSentToWorkerInOrderOfAdmission() {
	// If our implementation is not in fact ordering updates correctly, then it may be ordering them
	// non-deterministically. This number should be high enough that the false-negative rate of the test is low, but
	// must not exceed our limit on number of in-flight updates. If we were picking a random ordering then the
	// false-negative rate would be 1/(nUpdates!).
	nUpdates := 20
	s.testCluster.host.dcClient.OverrideValue(s.T(), dynamicconfig.WorkflowExecutionMaxInFlightUpdates, nUpdates)

	tv := testvars.New(s.T().Name())
	tv = s.startWorkflow(tv)
	for i := 0; i < nUpdates; i++ {
		updateId := fmt.Sprint(i)
		go func() { _, _ = s.sendUpdate(tv, updateId) }()
		s.Eventually(func() bool {
			resp, err := s.pollUpdate(tv, updateId, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED})
			if err == nil {
				s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Stage)
				return true
			}
			var notFoundErr *serviceerror.NotFound
			s.ErrorAs(err, &notFoundErr) // poll beat send in race
			return false
		}, time.Second, 10*time.Millisecond, fmt.Sprintf("update %s did not reach Admitted stage", updateId))
	}

	nCalls := 0
	poller := &TaskPoller{
		Engine:    s.engine,
		Namespace: s.namespace,
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			return []*commandpb.Command{}, nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			s.Len(task.Messages, nUpdates)
			for i, m := range task.Messages {
				s.Equal(tv.UpdateID(fmt.Sprint(i)), m.ProtocolInstanceId)
			}
			nCalls++
			return []*protocolpb.Message{}, nil
		},
		Logger: s.Logger,
		T:      s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	s.Equal(1, nCalls)
}
