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
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestCancelTimer() {
	id := "integration-cancel-timer-test"
	wt := "integration-cancel-timer-test-type"
	tl := "integration-cancel-timer-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := 2000 * time.Second
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: &timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.namespace,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
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
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       s.namespace,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalDelivered = true
		case enumspb.EVENT_TYPE_TIMER_CANCELED:
			timerCancelled = true
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			s.Fail("timer got fired")
		}
	}
}

func (s *integrationSuite) TestCancelTimer_CancelFiredAndBuffered() {
	id := "integration-cancel-timer-fired-and-buffered-test"
	wt := "integration-cancel-timer-fired-and-buffered-test-type"
	tl := "integration-cancel-timer-fired-and-buffered-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := 4 * time.Second
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: &timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.namespace,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			time.Sleep(2 * timer)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
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
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       s.namespace,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalDelivered = true
		case enumspb.EVENT_TYPE_TIMER_CANCELED:
			timerCancelled = true
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			s.Fail("timer got fired")
		}
	}
}
