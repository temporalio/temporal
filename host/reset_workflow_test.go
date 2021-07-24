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

package host

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestResetWorkflow() {
	id := "integration-reset-workflow-test"
	wt := "integration-reset-workflow-test-type"
	tq := "integration-reset-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(100 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
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
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndProcessWorkflowTask(false, false)
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

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("Poll and process final workflow task", tag.Error(err))
	s.NoError(err)

	s.NotNil(firstActivityCompletionEvent)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestResetWorkflow_ReapplyAll() {
	workflowID := "integration-reset-workflow-test-reapply-all"
	workflowTypeName := "integration-reset-workflow-test-reapply-all-type"
	taskQueueName := "integration-reset-workflow-test-reapply-all-taskqueue"

	s.testResetWorkflowReapply(workflowID, workflowTypeName, taskQueueName, 4, 3, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *integrationSuite) TestResetWorkflow_ReapplyNone() {
	workflowID := "integration-reset-workflow-test-reapply-none"
	workflowTypeName := "integration-reset-workflow-test-reapply-none-type"
	taskQueueName := "integration-reset-workflow-test-reapply-none-taskqueue"

	s.testResetWorkflowReapply(workflowID, workflowTypeName, taskQueueName, 4, 3, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *integrationSuite) testResetWorkflowReapply(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	resetToEventID int64,
	totalSignals int,
	reapplyType enumspb.ResetReapplyType,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	invocation := 0
	completed := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		invocation++
		if invocation <= totalSignals {
			// fist invocation is first workflow task,
			// next few are triggered by signals
			return []*commandpb.Command{}, nil
		}

		completed = true
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	for i := 0; i < totalSignals; i++ {
		signalRequest.RequestId = uuid.New()
		_, err = s.engine.SignalWorkflowExecution(NewContext(), signalRequest)
		s.NoError(err)

		_, err = poller.PollAndProcessWorkflowTask(true, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	s.True(completed)

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
	})
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resp.RunId,
	})
	signalCount := 0
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			signalCount++
		}
	}

	switch reapplyType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		s.Equal(totalSignals, signalCount)
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		s.Equal(0, signalCount)
	default:
		panic(fmt.Sprintf("unknown reset reapply type: %v", reapplyType))
	}

}

func (s *integrationSuite) TestResetWorkflow_ReapplyBufferAll() {
	workflowID := "integration-reset-workflow-test-reapply-buffer-all"
	workflowTypeName := "integration-reset-workflow-test-reapply-buffer-all-type"
	taskQueueName := "integration-reset-workflow-test-reapply-buffer-all-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *integrationSuite) TestResetWorkflow_ReapplyBufferNone() {
	workflowID := "integration-reset-workflow-test-reapply-buffer-none"
	workflowTypeName := "integration-reset-workflow-test-reapply-buffer-none-type"
	taskQueueName := "integration-reset-workflow-test-reapply-buffer-none-taskqueue"

	s.testResetWorkflowReapplyBuffer(workflowID, workflowTypeName, taskQueueName, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *integrationSuite) testResetWorkflowReapplyBuffer(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	reapplyType enumspb.ResetReapplyType,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err) // due to workflow termination (reset)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
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

func (s *integrationSuite) TestResetWorkflow_WorkflowTask_Schedule() {
	workflowID := "integration-reset-workflow-test-schedule"
	workflowTypeName := "integration-reset-workflow-test-schedule-type"
	taskQueueName := "integration-reset-workflow-test-schedule-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 3)
}

func (s *integrationSuite) TestResetWorkflow_WorkflowTask_ScheduleToStart() {
	workflowID := "integration-reset-workflow-test-schedule-to-start"
	workflowTypeName := "integration-reset-workflow-test-schedule-to-start-type"
	taskQueueName := "integration-reset-workflow-test-schedule-to-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 4)
}

func (s *integrationSuite) TestResetWorkflow_WorkflowTask_Start() {
	workflowID := "integration-reset-workflow-test-start"
	workflowTypeName := "integration-reset-workflow-test-start-type"
	taskQueueName := "integration-reset-workflow-test-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 5)
}

func (s *integrationSuite) testResetWorkflowRangeScheduleToStart(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	resetToEventID int64,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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

	_, err = poller.PollAndProcessWorkflowTask(false, false)
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

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
}
