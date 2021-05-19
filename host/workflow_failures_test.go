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
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestWorkflowTimeout() {
	startTime := time.Now().UTC()

	id := "integration-workflow-timeout"
	wt := "integration-workflow-timeout-type"
	tl := "integration-workflow-timeout-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false

	time.Sleep(time.Second)

GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			s.Logger.Warn("Execution not timedout yet. Last event: " + lastEvent.GetEventType().String())
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)

	startFilter := &filterpb.StartTimeFilter{
		EarliestTime: &startTime,
		LatestTime:   timestamp.TimePtr(time.Now().UTC()),
	}

	closedCount := 0
ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.Logger.Info("Closed WorkflowExecution is not yet visibile")
			time.Sleep(1000 * time.Millisecond)
			continue ListClosedLoop
		}
		break ListClosedLoop
	}
	s.Equal(1, closedCount)
}

func (s *integrationSuite) TestWorkflowTaskFailed() {
	id := "integration-workflowtask-failed-test"
	wt := "integration-workflowtask-failed-test-type"
	tl := "integration-workflowtask-failed-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

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
	activityScheduled := false
	activityData := int32(1)
	failureCount := 10
	signalCount := 0
	sendSignal := false
	lastWorkflowTaskTime := time.Time{}
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}
		// Some signals received on this workflow task
		if signalCount == 1 {
			return []*commandpb.Command{}, nil
		}

		// Send signals during workflow task
		if sendSignal {
			s.sendSignal(s.namespace, workflowExecution, "signalC", nil, identity)
			s.sendSignal(s.namespace, workflowExecution, "signalD", nil, identity)
			s.sendSignal(s.namespace, workflowExecution, "signalE", nil, identity)
			sendSignal = false
		}

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if failureCount > 0 {
			// Otherwise decrement failureCount and keep failing workflow tasks
			failureCount--
			return nil, errors.New("Workflow panic")
		}

		workflowComplete = true
		time.Sleep(time.Second)
		s.Logger.Warn(fmt.Sprintf("PrevStarted: %v, StartedEventID: %v, Size: %v", previousStartedEventID, startedEventID,
			len(history.Events)))
		lastWorkflowTaskEvent := history.Events[startedEventID-1]
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastWorkflowTaskEvent.GetEventType())
		lastWorkflowTaskTime = timestamp.TimeValue(lastWorkflowTaskEvent.GetEventTime())
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

	// Make first workflow task to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// process activity
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// fail workflow task 5 times
	for i := 1; i <= 5; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}

	err = s.sendSignal(s.namespace, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// process signal
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, signalCount)

	// send another signal to trigger workflow task
	err = s.sendSignal(s.namespace, workflowExecution, "signalB", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// fail workflow task 2 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}
	s.Equal(3, signalCount)

	// now send a signal during failed workflow task
	sendSignal = true
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, 3)
	s.NoError(err)
	s.Equal(4, signalCount)

	// fail workflow task 1 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}
	s.Equal(12, signalCount)

	// Make complete workflow workflow task
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 3)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(16, signalCount)

	events := s.getHistory(s.namespace, workflowExecution)
	var lastEvent *historypb.HistoryEvent
	var lastWorkflowTaskStartedEvent *historypb.HistoryEvent
	lastIdx := 0
	for i, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
			lastWorkflowTaskStartedEvent = e
			lastIdx = i
		}
		lastEvent = e
	}
	s.NotNil(lastEvent)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
	s.Logger.Info(fmt.Sprintf("Last workflow task time: %v, Last Workflow task history timestamp: %v, Complete timestamp: %v",
		lastWorkflowTaskTime, lastWorkflowTaskStartedEvent.GetEventTime(), lastEvent.GetEventTime()))
	s.Equal(lastWorkflowTaskTime, timestamp.TimeValue(lastWorkflowTaskStartedEvent.GetEventTime()))
	s.True(timestamp.TimeValue(lastEvent.GetEventTime()).Sub(lastWorkflowTaskTime) >= time.Second)

	s.Equal(2, len(events)-lastIdx-1)
	workflowTaskCompletedEvent := events[lastIdx+1]
	workflowCompletedEvent := events[lastIdx+2]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, workflowTaskCompletedEvent.GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, workflowCompletedEvent.GetEventType())
}

func (s *integrationSuite) TestRespondWorkflowTaskCompleted_ReturnsErrorIfInvalidArgument() {
	id := "integration-respond-workflow-task-completed-test"
	wt := "integration-respond-workflow-task-completed-test-type"
	tq := "integration-respond-workflow-task-completed-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tq},
		Input:              nil,
		WorkflowRunTimeout: timestamp.DurationPtr(100 * time.Second),
		Identity:           identity,
	}

	we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.NotNil(we0)

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "", // Marker name is missing.
					Details:    nil,
					Header:     nil,
					Failure:    nil,
				}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadRecordMarkerAttributes: MarkerName is not set on command.", err.Error())

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.GetRunId(),
		},
	})

	s.NoError(err)
	s.NotNil(resp)

	// Last event is WORKFLOW_TASK_FAILED.
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, resp.History.Events[len(resp.History.Events)-1].GetEventType())
}
