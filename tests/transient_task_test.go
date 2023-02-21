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
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestTransientWorkflowTaskTimeout() {
	id := "integration-transient-workflow-task-timeout-test"
	wt := "integration-transient-workflow-task-timeout-test-type"
	tl := "integration-transient-workflow-task-timeout-test-taskqueue"
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
		WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
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
	failWorkflowTask := true
	signalCount := 0
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("Workflow panic")
		}

		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
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

	// First workflow task immediately fails and schedules a transient workflow task
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now send a signal when transient workflow task is scheduled
	err = s.sendSignal(s.namespace, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// Drop workflow task to cause a workflow task timeout
	_, err = poller.PollAndProcessWorkflowTask(true, true)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now process signal and complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 2)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.Equal(1, signalCount)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestTransientWorkflowTaskHistorySize() {
	id := "integration-transient-workflow-task-history-size-test"
	wt := "integration-transient-workflow-task-history-size-test-type"
	tl := "integration-transient-workflow-task-history-size-test-taskqueue"
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
		WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// start with 2mb limit
	s.testCluster.host.dcClient.OverrideValue(dynamicconfig.HistorySizeSuggestContinueAsNew, 2*1024*1024)

	// workflow logic
	stage := 0
	workflowComplete := false
	largeValue := make([]byte, 1024*1024)
	// record the values that we see for completed tasks here
	type fields struct {
		size    int64
		suggest bool
	}
	var sawFields []fields
	// record value for failed wft
	var failedTaskSawSize int64
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		// find workflow task started event
		event := history.Events[len(history.Events)-1]
		s.Equal(event.GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)
		attrs := event.GetWorkflowTaskStartedEventAttributes()
		s.Logger.Info("wtHandler", tag.Counter(stage))

		stage++
		switch stage {
		case 1:
			s.Less(attrs.HistorySizeBytes, int64(1024*1024))
			s.False(attrs.SuggestContinueAsNew)
			// record a large marker
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "big marker",
					Details:    map[string]*commonpb.Payloads{"value": payloads.EncodeBytes(largeValue)},
				}},
			}}, nil

		case 2:
			s.Greater(attrs.HistorySizeBytes, int64(1024*1024))
			s.False(attrs.SuggestContinueAsNew)
			// record another large marker
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "big marker",
					Details:    map[string]*commonpb.Payloads{"value": payloads.EncodeBytes(largeValue)},
				}},
			}}, nil

		case 3:
			s.Greater(attrs.HistorySizeBytes, int64(2048*1024))
			s.True(attrs.SuggestContinueAsNew)
			failedTaskSawSize = attrs.HistorySizeBytes
			// fail workflow task and we'll get a transient one
			return nil, errors.New("oops")

		case 4:
			// we might not get the same value but it shouldn't be smaller, and not too much larger
			s.GreaterOrEqual(attrs.HistorySizeBytes, failedTaskSawSize)
			s.Less(attrs.HistorySizeBytes, failedTaskSawSize+10000)
			s.False(attrs.SuggestContinueAsNew)
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return nil, nil

		case 5:
			// we should get just a little larger
			prevSize := sawFields[len(sawFields)-1].size
			s.Greater(attrs.HistorySizeBytes, prevSize)
			s.Less(attrs.HistorySizeBytes, prevSize+10000)
			s.False(attrs.SuggestContinueAsNew) // now false

			workflowComplete = true
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		}

		return nil, errors.New("bad stage")
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

	// stage 1
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.sendSignal(s.namespace, workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// stage 2
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.sendSignal(s.namespace, workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// stage 3: this one fails with a panic
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// change the dynamic config so that SuggestContinueAsNew should now be false. the current
	// workflow task should still see true, but the next one will see false.
	s.testCluster.host.dcClient.OverrideValue(dynamicconfig.HistorySizeSuggestContinueAsNew, 8*1024*1024)

	// stage 4
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.sendSignal(s.namespace, workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// drop workflow task to cause a workflow task timeout
	_, err = poller.PollAndProcessWorkflowTask(true, true)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// stage 5
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)

	// check history
	// we should have 4 workflow task completed events
	allEvents := s.getHistory(s.namespace, workflowExecution)
	var completedEvents []*historypb.HistoryEvent
	for _, event := range allEvents {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			completedEvents = append(completedEvents, event)
		}
	}
	s.Equal(4, len(completedEvents))
	for i, event := range completedEvents {
		// find the corresponding started event, and make sure it matches the values we
		// recorded in the workflow
		completedAttrs := event.GetWorkflowTaskCompletedEventAttributes()
		startedEvent := allEvents[completedAttrs.StartedEventId-1]
		startedAttrs := startedEvent.GetWorkflowTaskStartedEventAttributes()
		s.Equal(sawFields[i].size, startedAttrs.HistorySizeBytes)
		s.Equal(sawFields[i].suggest, startedAttrs.SuggestContinueAsNew)
	}
}

func (s *integrationSuite) TestNoTransientWorkflowTaskAfterFlushBufferedEvents() {
	id := "integration-no-transient-workflow-task-after-flush-buffered-events-test"
	wt := "integration-no-transient-workflow-task-after-flush-buffered-events-test-type"
	tl := "integration-no-transient-workflow-task-after-flush-buffered-events-test-taskqueue"
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

	// workflow logic
	workflowComplete := false
	continueAsNewAndSignal := false
	wtHandler := func(execution *commonpb.WorkflowExecution, workflowType *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !continueAsNewAndSignal {
			continueAsNewAndSignal = true
			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal-1",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
					Input:               nil,
					WorkflowRunTimeout:  timestamp.DurationPtr(1000 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(100 * time.Second),
				}},
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// fist workflow task, this try to do a continue as new but there is a buffered event,
	// so it will fail and create a new workflow task
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("UnhandledCommand", err.Error())

	// second workflow task, which will complete the workflow
	// this expect the workflow task to have attempt == 1
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}
