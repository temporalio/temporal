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
	"errors"
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

func (s *integrationSuite) TestStickyTimeout_NonTransientWorkflowTask() {
	id := "integration-sticky-timeout-non-transient-workflow-task"
	wt := "integration-sticky-timeout-non-transient-command-type"
	tl := "integration-sticky-timeout-non-transient-workflow-taskqueue"
	stl := "integration-sticky-timeout-non-transient-workflow-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{}
	stickyTaskQueue.Name = stl
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			// send a signal on third failure to be buffered, forcing a non-transient workflow task when buffer is flushed
			/*if failureCount == 3 {
				err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:            s.namespace,
					WorkflowExecution: workflowExecution,
					SignalName:        "signalB",
					Input:             codec.EncodeString("signal input"),
					Identity:          identity,
					RequestId:         uuid.New(),
				})
				s.NoError(err)
			}*/
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.namespace, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	workflowTaskFailed := false
	events := s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			workflowTaskFailed = true
			break
		}
	}
	s.True(workflowTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, 3)
	s.NoError(err)

	// Assert for single workflow task failed and workflow completion
	failedWorkflowTasks := 0
	workflowComplete := false
	events = s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			failedWorkflowTasks++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedWorkflowTasks, "Mismatched failed workflow tasks count")
}

func (s *integrationSuite) TestStickyTaskqueueResetThenTimeout() {
	id := "integration-reset-sticky-fire-schedule-to-start-timeout"
	wt := "integration-reset-sticky-fire-schedule-to-start-timeout-type"
	tl := "integration-reset-sticky-fire-schedule-to-start-timeout-taskqueue"
	stl := "integration-reset-sticky-fire-schedule-to-start-timeout-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{}
	stickyTaskQueue.Name = stl
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	// Reset sticky taskqueue before sticky workflow task starts
	_, err = s.engine.ResetStickyTaskQueue(NewContext(), &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: s.namespace,
		Execution: workflowExecution,
	})
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.namespace, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	workflowTaskFailed := false
	events := s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			workflowTaskFailed = true
			break
		}
	}
	s.True(workflowTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, 3)
	s.NoError(err)

	// Assert for single workflow task failed and workflow completion
	failedWorkflowTasks := 0
	workflowComplete := false
	events = s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			failedWorkflowTasks++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedWorkflowTasks, "Mismatched failed workflow tasks count")
}
