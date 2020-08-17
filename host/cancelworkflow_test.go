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
	"strconv"
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

func (s *integrationSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "integration-request-cancel-workflow-test"
	wt := "integration-request-cancel-workflow-test-type"
	tl := "integration-request-cancel-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

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

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(int(activityCounter)),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	_, err = s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.CancellationAlreadyRequested{}, err)

	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 3; i++ {
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
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		var details string
		err = payloads.Decode(cancelledEventAttributes.GetDetails(), &details)
		s.NoError(err)
		s.Equal("Cancelled", details)
		executionCancelled = true
		break GetHistoryLoop
	}
	s.True(executionCancelled)
}

func (s *integrationSuite) TestRequestCancelWorkflowCommandExecution() {
	id := "integration-cancel-workflow-command-test"
	wt := "integration-cancel-workflow-command-test-type"
	tl := "integration-cancel-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

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

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.foreignNamespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(int(activityCounter)),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
				Namespace:  s.foreignNamespace,
				WorkflowId: id,
				RunId:      we2.RunId,
			}},
		}}, nil
	}

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

	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	foreignwtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.foreignNamespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: foreignwtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Cancel the foreign workflow with this workflow task request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	cancellationSent := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
			s.Logger.Info("Cancellation still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		externalWorkflowExecutionCancelRequestedEvent := lastEvent.GetExternalWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), externalWorkflowExecutionCancelRequestedEvent.InitiatedEventId)
		s.Equal(id, externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.WorkflowId)
		s.Equal(we2.RunId, externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.RunId)

		cancellationSent = true
		break
	}

	s.True(cancellationSent)

	// Accept cancellation.
	_, err = foreignPoller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.foreignNamespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we2.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		var details string
		err = payloads.Decode(cancelledEventAttributes.GetDetails(), &details)
		s.NoError(err)
		s.Equal("Cancelled", details)
		executionCancelled = true

		// Find cancel requested event and verify it.
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range history.Events {
			if x.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				cancelRequestEvent = x
			}
		}

		s.NotNil(cancelRequestEvent)
		cancelRequestEventAttributes := cancelRequestEvent.GetWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), cancelRequestEventAttributes.ExternalInitiatedEventId)
		s.Equal(id, cancelRequestEventAttributes.ExternalWorkflowExecution.WorkflowId)
		s.Equal(we.RunId, cancelRequestEventAttributes.ExternalWorkflowExecution.RunId)

		break GetHistoryLoop
	}
	s.True(executionCancelled)
}

func (s *integrationSuite) TestRequestCancelWorkflowCommandExecution_UnKnownTarget() {
	id := "integration-cancel-unknown-workflow-command-test"
	wt := "integration-cancel-unknown-workflow-command-test-type"
	tl := "integration-cancel-unknown-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

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

	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(int(activityCounter)),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
				Namespace:  s.foreignNamespace,
				WorkflowId: "workflow_not_exist",
				RunId:      we.RunId,
			}},
		}}, nil
	}

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

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Cancel the foreign workflow with this workflow task request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	cancellationSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		requestCancelExternalWorkflowExecutionFailedEvetn := lastEvent.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()
		s.Equal(int64(intiatedEventID), requestCancelExternalWorkflowExecutionFailedEvetn.InitiatedEventId)
		s.Equal("workflow_not_exist", requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.WorkflowId)
		s.Equal(we.RunId, requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.RunId)

		cancellationSentFailed = true
		break
	}

	s.True(cancellationSentFailed)
}

func (s *integrationSuite) TestImmediateChildCancellation_WorkflowTaskFailed() {
	id := "integration-immediate-child-cancellation-worflow-task-failed-test"
	wt := "integration-immediate-child-cancellation-worflow-task-failed-test-type"
	tl := "integration-immediate-child-cancellation-worflow-task-failed-test-taskqueue"
	childWorkflowID := "integration-immediate-child-cancellation-worflow-task-failed-child-test"
	childTaskQueue := "integration-immediate-child-cancellation-worflow-task-failed-child-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

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

	s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Identity:  identity,
		RequestId: uuid.New(),
	})

	childCancelled := false
	var initiatedEvent *historypb.HistoryEvent
	var requestCancelEvent *historypb.HistoryEvent
	var workflowtaskFailedEvent *historypb.HistoryEvent
	workflowComplete := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !childCancelled {
			startEvent := history.Events[0]
			if startEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				return nil, errors.New("first event is not workflow execution started")
			}

			workflowTaskScheduledEvent := history.Events[1]
			if workflowTaskScheduledEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return nil, errors.New("second event is not workflow task scheduled")
			}

			cancelRequestedEvent := history.Events[2]
			if cancelRequestedEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				return nil, errors.New("third event is not cancel requested")
			}

			// Schedule and cancel child workflow in the same decision
			childCancelled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:    s.namespace,
					WorkflowId:   childWorkflowID,
					WorkflowType: &commonpb.WorkflowType{Name: "childTypeA"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: childTaskQueue},
					Input:        payloads.EncodeBytes([]byte{1}),
				}},
			}, {
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace:         s.namespace,
					WorkflowId:        childWorkflowID,
					ChildWorkflowOnly: true,
				}},
			}}, nil
		}

		if previousStartedEventID != 0 {
			return nil, errors.New("previous started decision moved unexpectedly after first failed workflow task")
		}
		// Validate child workflow as cancelled
		for _, event := range history.Events[previousStartedEventID:] {
			s.Logger.Info(fmt.Sprintf("Processing EventID: %v, Event: %v", event.GetEventId(), event))
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
				initiatedEvent = event
			case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
				requestCancelEvent = event
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
				workflowtaskFailedEvent = event
			}
		}

		if initiatedEvent != nil {
			return nil, errors.New("start child workflow command accepted from previous workflow task")
		}

		if requestCancelEvent != nil {
			return nil, errors.New("request cancel command accepted from previous workflow task")
		}

		if workflowtaskFailedEvent == nil {
			return nil, errors.New("workflow task failed event not found due to previous bad commands")
		}

		taskFailure := workflowtaskFailedEvent.GetWorkflowTaskFailedEventAttributes().GetFailure()
		if taskFailure.GetMessage() != "BadRequestCancelExternalWorkflowExecutionAttributes: Start and RequestCancel for child workflow is not allowed in same workflow task." {
			return nil, errors.New("unexpected workflow task failure")
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
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	s.Logger.Info("Process first workflow task which starts and request cancels child workflow")
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadRequestCancelExternalWorkflowExecutionAttributes: Start and RequestCancel for child workflow is not allowed in same workflow task.", err.Error())

	s.printWorkflowHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
	})

	s.Logger.Info("Process second workflow task which observes child workflow is cancelled and completes")
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	s.printWorkflowHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
	})

	_, err = s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
		},
	})
	if err == nil {
		s.printWorkflowHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
		})
	}
	s.Logger.Error("Describe error", tag.Error(err))
	s.Error(err, "Child workflow execution started instead of getting cancelled")
	s.IsType(&serviceerror.NotFound{}, err, "Error is not of type 'NotFound'")

	s.True(workflowComplete)
}
