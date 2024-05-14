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
	"errors"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestStickyTimeout_NonTransientWorkflowTask() {
	id := "functional-sticky-timeout-non-transient-workflow-task"
	wt := "functional-sticky-timeout-non-transient-command-type"
	tl := "functional-sticky-timeout-non-transient-workflow-taskqueue"
	stl := "functional-sticky-timeout-non-transient-workflow-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
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
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTask(WithRespondSticky)
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
				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut {"TimeoutType":2} // enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
  9 WorkflowTaskScheduled`, events)
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(i))
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
		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	events := s.getHistory(s.namespace, workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed`, events)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(3))
	s.NoError(err)

	events = s.getHistory(s.namespace, workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed  // Two WFTs have failed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed // Two WFTs have failed
 16 WorkflowTaskScheduled
 17 WorkflowTaskStarted
 18 WorkflowTaskCompleted
 19 WorkflowExecutionCompleted // Workflow has completed`, events)
}

func (s *FunctionalSuite) TestStickyTaskqueueResetThenTimeout() {
	id := "functional-reset-sticky-fire-schedule-to-start-timeout"
	wt := "functional-reset-sticky-fire-schedule-to-start-timeout-type"
	tl := "functional-reset-sticky-fire-schedule-to-start-timeout-taskqueue"
	stl := "functional-reset-sticky-fire-schedule-to-start-timeout-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
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
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTask(WithRespondSticky)
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
				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut {"TimeoutType":2} // enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START 
  9 WorkflowTaskScheduled`, events)
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(i))
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
		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	events := s.getHistory(s.namespace, workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed`, events)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithRespondSticky, WithExpectedAttemptCount(3))
	s.NoError(err)

	events = s.getHistory(s.namespace, workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed  // Two WFTs have failed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed // Two WFTs have failed
 16 WorkflowTaskScheduled
 17 WorkflowTaskStarted
 18 WorkflowTaskCompleted
 19 WorkflowExecutionCompleted // Workflow has completed`, events)
}
