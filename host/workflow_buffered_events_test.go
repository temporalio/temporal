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
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestRateLimitBufferedEvents() {
	id := "integration-rate-limit-buffered-events-test"
	wt := "integration-rate-limit-buffered-events-test-type"
	tl := "integration-rate-limit-buffered-events-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
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
	workflowComplete := false
	signalsSent := false
	signalCount := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, h *historypb.History) ([]*commandpb.Command, error) {

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}

		if !signalsSent {
			signalsSent = true
			// Buffered Signals
			for i := 0; i < 100; i++ {
				buf := new(bytes.Buffer)
				err := binary.Write(buf, binary.LittleEndian, byte(i))
				s.NoError(err)
				s.Nil(s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity))
			}

			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.LittleEndian, byte(101))
			s.NoError(err)
			signalErr := s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity)
			s.NoError(signalErr)

			// this command will be ignored as workflow task has already failed
			return []*commandpb.Command{}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task to send 101 signals, the last signal will force fail workflow task and flush buffered events.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Equal("Workflow task not found.", err.Error())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(101, signalCount) // check that all 101 signals are received.
}

func (s *integrationSuite) TestBufferedEvents() {
	id := "integration-buffered-events-test"
	wt := "integration-buffered-events-test-type"
	tl := "integration-buffered-events-test-taskqueue"
	identity := "worker1"
	signalName := "buffered-signal"

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

	// workflow logic
	workflowComplete := false
	signalSent := false
	var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 && signalEvent == nil {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task, which sends signal and the signal event should be buffered to append after first workflow task closed
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// check history, the signal event should be after the complete workflow task
	histResp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	s.NotNil(histResp.History.Events)
	s.True(len(histResp.History.Events) >= 6)
	s.Equal(histResp.History.Events[3].GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
	s.Equal(histResp.History.Events[4].GetEventType(), enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	s.Equal(histResp.History.Events[5].GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestBufferedEventsOutOfOrder() {
	id := "integration-buffered-events-out-of-order-test"
	wt := "integration-buffered-events-out-of-order-test-type"
	tl := "integration-buffered-events-out-of-order-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(20 * time.Second),
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
	workflowComplete := false
	firstWorkflowTask := false
	secondWorkflowTask := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info(fmt.Sprintf("Workflow called: first: %v, second: %v, complete: %v\n", firstWorkflowTask, secondWorkflowTask, workflowComplete))

		if !firstWorkflowTask {
			firstWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
			}, {
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "Activity-1",
					ActivityType:           &commonpb.ActivityType{Name: "ActivityType"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeString("some random activity input"),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(100 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(100 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(100 * time.Second),
				}},
			}}, nil
		}

		if !secondWorkflowTask {
			secondWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
			}}, nil
		}

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task, which will schedule an activity and add marker
	_, task, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		true,
		false,
		false,
		false,
		0,
		1,
		true,
		nil)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// This will cause activity start and complete to be buffered
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("pollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// second workflow task, completes another local activity and forces flush of buffered activity events
	newWorkflowTask := task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(task)

	// third workflow task, which will close workflow
	newWorkflowTask = task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Nil(task.WorkflowTask)

	events := s.getHistory(s.namespace, workflowExecution)
	var scheduleEvent, startedEvent, completedEvent *historypb.HistoryEvent
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			scheduleEvent = event
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			startedEvent = event
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			completedEvent = event
		}
	}

	s.NotNil(scheduleEvent)
	s.NotNil(startedEvent)
	s.NotNil(completedEvent)
	s.True(startedEvent.GetEventId() < completedEvent.GetEventId())
	s.Equal(scheduleEvent.GetEventId(), startedEvent.GetActivityTaskStartedEventAttributes().GetScheduledEventId())
	s.Equal(scheduleEvent.GetEventId(), completedEvent.GetActivityTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(startedEvent.GetEventId(), completedEvent.GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	s.True(workflowComplete)
}
