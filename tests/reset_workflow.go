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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api/resetworkflow"
)

func (s *FunctionalSuite) TestResetWorkflow() {
	id := "functional-reset-workflow-test"
	wt := "functional-reset-workflow-test-type"
	tq := "functional-reset-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	activityData := int32(1)
	activityCount := 3
	isFirstTaskProcessed := false
	isSecondTaskProcessed := false
	var firstActivityCompletionEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !isFirstTaskProcessed {
			// Schedule 3 activities on first workflow task
			isFirstTaskProcessed = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			var scheduleActivityCommands []*commandpb.Command
			for i := 1; i <= activityCount; i++ {
				scheduleActivityCommands = append(scheduleActivityCommands, &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             strconv.Itoa(i),
						ActivityType:           &commonpb.ActivityType{Name: "ResetActivity"},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(100 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(5 * time.Second),
					}},
				})
			}

			return scheduleActivityCommands, nil
		} else if !isSecondTaskProcessed {
			// Confirm one activity completion on second workflow task
			isSecondTaskProcessed = true
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
					firstActivityCompletionEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil

	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Process first workflow task to schedule activities
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// Reset workflow execution
	_, err = s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process second activity", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process third activity", tag.Error(err))
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process final workflow task", tag.Error(err))
	s.NoError(err)

	s.NotNil(firstActivityCompletionEvent)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplyAll() {
	s.testResetWorkflowReapply(
		"exclude-none-reapply-all",
		[]enumspb.ResetReapplyExcludeType{},
		enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplySignal() {
	s.testResetWorkflowReapply(
		"exclude-none-reapply-signal",
		[]enumspb.ResetReapplyExcludeType{},
		enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeNoneReapplyNone() {
	s.testResetWorkflowReapply(
		"exclude-none-reapply-none",
		[]enumspb.ResetReapplyExcludeType{},
		enumspb.RESET_REAPPLY_TYPE_NONE,
	)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplyAll() {
	s.testResetWorkflowReapply(
		"exclude-signal-reapply-all",
		[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplySignal() {
	s.testResetWorkflowReapply(
		"exclude-signal-reapply-signal",
		[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	)
}

func (s *FunctionalSuite) TestResetWorkflow_ExcludeSignalReapplyNone() {
	s.testResetWorkflowReapply(
		"exclude-signal-reapply-none",
		[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		enumspb.RESET_REAPPLY_TYPE_NONE,
	)
}

func (s *FunctionalSuite) testResetWorkflowReapply(
	testName string,
	reapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
	reapplyType enumspb.ResetReapplyType,
) {
	totalSignals := 3
	totalUpdates := 3
	resetToEventID := int64(totalSignals + totalUpdates + 1)

	tv := testvars.New(s.T().Name())
	workflowID := fmt.Sprintf("functional-reset-workflow-test-%s", testName)
	workflowTypeName := fmt.Sprintf("functional-reset-workflow-test-%s-type", testName)
	taskQueueName := fmt.Sprintf("functional-reset-workflow-test-%s-taskqueue", testName)
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	runID := we.RunId

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	invocation := 0
	commandsCompleted := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		invocation++

		// First invocation is first workflow task; then come `totalSignals` signals, followed by `totalUpdates`
		// updates.
		if invocation <= totalSignals+totalUpdates+1 {
			return []*commandpb.Command{}, nil
		}

		commandsCompleted = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	messagesCompleted := false
	messageHandlerInvocation := 0
	messageHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		// First invocation is first workflow task; then come `totalSignals` signals, followed by `totalUpdates`
		// updates.
		messageHandlerInvocation++

		if messageHandlerInvocation <= totalSignals+1 {
			return []*protocolpb.Message{}, nil
		} else if messageHandlerInvocation <= totalSignals+totalUpdates+1 {

			return []*protocolpb.Message{
				{
					Id:                 tv.MessageID("update-accepted"),
					ProtocolInstanceId: tv.UpdateID(fmt.Sprintf("%d", messageHandlerInvocation-totalSignals-1)),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
						AcceptedRequestMessageId:         fmt.Sprintf("accept-message-%d", messageHandlerInvocation),
						AcceptedRequestSequencingEventId: int64(messageHandlerInvocation),
						AcceptedRequest:                  nil,
					}),
				},
			}, nil
		}

		messagesCompleted = true
		return []*protocolpb.Message{}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      messageHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.testResetWorkflowReapplySendSignals(totalSignals, workflowID, runID, identity, poller)
	s.testResetWorkflowReapplySendUpdates(totalUpdates, workflowID, runID, identity, poller, tv)

	// TODO (dan) these should both be s.True(...)
	fmt.Println(commandsCompleted)
	fmt.Println(messagesCompleted)

	// reset
	resp, err := s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.New(),
		ResetReapplyType:          reapplyType,
		ResetReapplyExcludeTypes:  reapplyExcludeTypes,
	})
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resp.RunId,
	})
	signalCount := 0
	updateCount := 0
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalCount++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REQUESTED:
			updateCount++
		}
	}

	var shouldReapplySignals = true
	var shouldReapplyUpdates = true
	for _, excludeType := range resetworkflow.GetResetReapplyExcludeTypes(reapplyExcludeTypes, reapplyType) {
		switch excludeType {
		case enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL:
			shouldReapplySignals = false
		case enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE:
			shouldReapplyUpdates = false
		}
	}
	if shouldReapplySignals {
		s.Equal(totalSignals, signalCount)
	} else {
		s.Equal(0, signalCount)
	}
	if shouldReapplyUpdates {
		s.Equal(totalUpdates, updateCount)
	} else {
		s.Equal(0, updateCount)
	}
}

func (s *FunctionalSuite) testResetWorkflowReapplySendSignals(
	totalSignals int,
	workflowId, runId, identity string,
	poller *TaskPoller,
) {
	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		SignalName: "signal-name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Data: []byte("random data"),
		}}},
		Identity: identity,
	}

	for i := 0; i < totalSignals; i++ {
		signalRequest.RequestId = uuid.New()
		_, err := s.engine.SignalWorkflowExecution(NewContext(), signalRequest)
		s.NoError(err)

		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}
}

func (s *FunctionalSuite) testResetWorkflowReapplySendUpdates(
	totalUpdates int,
	workflowId, runId, identity string,
	poller *TaskPoller,
	tv *testvars.TestVars,
) {
	newUpdateRequest := func(updateId string) *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runId,
			},
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateId)},
				Input: &updatepb.Input{
					Name: tv.HandlerName(),
					Args: payloads.EncodeString("args-value-of-" + tv.UpdateID(updateId)),
				},
			},
		}
	}

	for i := 0; i < totalUpdates; i++ {
		_, err := s.engine.UpdateWorkflowExecution(NewContext(), newUpdateRequest(fmt.Sprintf("%d", i+1)))
		s.NoError(err)

		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}
}

func (s *FunctionalSuite) TestResetWorkflow_ReapplyBufferAll() {
	workflowID := "functional-reset-workflow-test-reapply-buffer-all"
	workflowTypeName := "functional-reset-workflow-test-reapply-buffer-all-type"
	taskQueueName := "functional-reset-workflow-test-reapply-buffer-all-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *FunctionalSuite) TestResetWorkflow_ReapplyBufferNone() {
	workflowID := "functional-reset-workflow-test-reapply-buffer-none"
	workflowTypeName := "functional-reset-workflow-test-reapply-buffer-none-type"
	taskQueueName := "functional-reset-workflow-test-reapply-buffer-none-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *FunctionalSuite) testResetWorkflowReapplyBuffer(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	reapplyType enumspb.ResetReapplyType,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	runID := we.RunId

	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Data: []byte("random data"),
		}}},
		Identity: identity,
	}

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	resetRunID := ""
	workflowComplete := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if len(resetRunID) == 0 {
			signalRequest.RequestId = uuid.New()
			_, err := s.engine.SignalWorkflowExecution(NewContext(), signalRequest)
			s.NoError(err)

			// events layout
			//  1. WorkflowExecutionStarted
			//  2. WorkflowTaskScheduled
			//  3. WorkflowTaskStarted
			//  x. WorkflowExecutionSignaled

			resp, err := s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Reason:                    "reset execution from test",
				WorkflowTaskFinishEventId: 3,
				RequestId:                 uuid.New(),
				ResetReapplyType:          reapplyType,
			})
			s.NoError(err)
			resetRunID = resp.RunId

			return []*commandpb.Command{}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err) // due to workflow termination (reset)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resetRunID,
	})
	signalCount := 0
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			signalCount++
		}
	}

	switch reapplyType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		s.Equal(1, signalCount)
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		s.Equal(0, signalCount)
	default:
		panic(fmt.Sprintf("unknown reset reapply type: %v", reapplyType))
	}

}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_Schedule() {
	workflowID := "functional-reset-workflow-test-schedule"
	workflowTypeName := "functional-reset-workflow-test-schedule-type"
	taskQueueName := "functional-reset-workflow-test-schedule-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 3)
}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_ScheduleToStart() {
	workflowID := "functional-reset-workflow-test-schedule-to-start"
	workflowTypeName := "functional-reset-workflow-test-schedule-to-start-type"
	taskQueueName := "functional-reset-workflow-test-schedule-to-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 4)
}

func (s *FunctionalSuite) TestResetWorkflow_WorkflowTask_Start() {
	workflowID := "functional-reset-workflow-test-start"
	workflowTypeName := "functional-reset-workflow-test-start-type"
	taskQueueName := "functional-reset-workflow-test-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 5)
}

func (s *FunctionalSuite) testResetWorkflowRangeScheduleToStart(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	resetToEventID int64,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
			{Data: []byte("random signal payload")},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// workflow logic
	workflowComplete := false
	isWorkflowTaskProcessed := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !isWorkflowTaskProcessed {
			isWorkflowTaskProcessed = true
			return []*commandpb.Command{}, nil
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil

	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	_, err = s.engine.ResetWorkflowExecution(NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
}
