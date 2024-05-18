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
	"strings"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/consts"
)

func (s *FunctionalSuite) TestSignalWorkflow() {
	id := "functional-signal-workflow-test"
	wt := "functional-signal-workflow-test-type"
	tl := "functional-signal-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Send a signal to non-exist workflow
	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	_, err0 := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      uuid.New(),
		},
		SignalName: "failed signal",
		Input:      nil,
		Identity:   identity,
		Header:     header,
	})
	s.NotNil(err0)
	s.IsType(&serviceerror.NotFound{}, err0)

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
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
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

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		Header:     header,
	})
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.ProtoEqual(header, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Header)

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = payloads.EncodeString("another signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: "failed signal 1",
		Input:      nil,
		Identity:   identity,
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *FunctionalSuite) TestSignalWorkflow_DuplicateRequest() {
	id := "functional-signal-workflow-test-duplicate"
	wt := "functional-signal-workflow-test-duplicate-type"
	tl := "functional-signal-workflow-test-duplicate-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	numOfSignaledEvent := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			numOfSignaledEvent = 0
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
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

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	requestID := uuid.New()
	signalReqest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		RequestId:  requestID,
	}
	_, err = s.engine.SignalWorkflowExecution(NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.Equal(1, numOfSignaledEvent)

	// Send another signal with same request id
	_, err = s.engine.SignalWorkflowExecution(NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(0, numOfSignaledEvent)
}

func (s *FunctionalSuite) TestSignalExternalWorkflowCommand() {
	id := "functional-signal-external-workflow-test"
	wt := "functional-signal-external-workflow-test-type"
	tl := "functional-signal-external-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.foreignNamespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we2.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
				Header:     signalHeader,
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
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

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	foreignwtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(foreignActivityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}
		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 ExternalWorkflowExecutionSignaled {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, we2.RunId, id), historyEvents)

	// Process signal in workflow for foreign workflow
	_, err = foreignPoller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.ProtoEqual(signalHeader, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Header)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *FunctionalSuite) TestSignalWorkflow_Cron_NoWorkflowTaskCreated() {
	id := "functional-signal-workflow-test-cron"
	wt := "functional-signal-workflow-test-cron-type"
	tl := "functional-signal-workflow-test-cron-taskqueue"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
		CronSchedule:        cronSpec,
	}
	now := time.Now().UTC()

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// workflow logic
	var workflowTaskDelay time.Duration
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		workflowTaskDelay = time.Now().UTC().Sub(now)

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

	// Make first command to schedule activity
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowTaskDelay > time.Second*2)
}

func (s *FunctionalSuite) TestSignalWorkflow_NoWorkflowTaskCreated() {
	id := "functional-signal-workflow-test-skip-wft"
	wt := "functional-signal-workflow-test-skip-wft-type"
	tl := "functional-signal-workflow-test-skip-wft-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	workflowComplete := false
	// workflow logic
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !workflowComplete {
			workflowComplete = true
			return []*commandpb.Command{}, nil
		}

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

	// process start task
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal which should NOT generate a new wft
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName:               "signal without task",
		Input:                    nil,
		Identity:                 identity,
		SkipGenerateWorkflowTask: true,
	})
	s.NoError(err)

	// Send second signal which should generate a new wft
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName:               "signal with task",
		Input:                    nil,
		Identity:                 identity,
		SkipGenerateWorkflowTask: false,
	})
	s.NoError(err)

	// process signal and complete workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionCompleted`, historyEvents)
}

func (s *FunctionalSuite) TestSignalWorkflow_WorkflowCloseAttempted() {
	id := "functional-signal-workflow-workflow-close-attempted-test"
	wt := "functional-signal-workflow-workflow-close-attempted-test-type"
	tl := "functional-signal-workflow-workflow-close-attempted-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	attemptCount := 1
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if attemptCount == 1 {
			_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
				SignalName: "buffered-signal",
				Identity:   identity,
				RequestId:  uuid.New(),
			})
			s.NoError(err)
		}

		if attemptCount == 2 {
			ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(time.Second)
			_, err := s.engine.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
				SignalName: "rejected-signal",
				Identity:   identity,
				RequestId:  uuid.New(),
			})
			s.Error(err)
			s.Error(consts.ErrWorkflowClosing, err)
		}

		attemptCount++
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

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *FunctionalSuite) TestSignalExternalWorkflowCommand_WithoutRunID() {
	id := "functional-signal-external-workflow-test-without-run-id"
	wt := "functional-signal-external-workflow-test-without-run-id-type"
	tl := "functional-signal-external-workflow-test-without-run-id-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.foreignNamespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					// No RunID in command
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
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

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	foreignwtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(foreignActivityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}

		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 ExternalWorkflowExecutionSignaled {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, id), historyEvents)

	// Process signal in workflow for foreign workflow
	_, err = foreignPoller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *FunctionalSuite) TestSignalExternalWorkflowCommand_UnKnownTarget() {
	id := "functional-signal-unknown-workflow-command-test"
	wt := "functional-signal-unknown-workflow-command-test-type"
	tl := "functional-signal-unknown-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow_not_exist",
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}
		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 SignalExternalWorkflowExecutionFailed {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"workflow_not_exist"}}
 12 WorkflowTaskScheduled`, we.RunId), historyEvents)
}

func (s *FunctionalSuite) TestSignalExternalWorkflowCommand_SignalSelf() {
	id := "functional-signal-self-workflow-command-test"
	wt := "functional-signal-self-workflow-command-test-type"
	tl := "functional-signal-self-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
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
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		break
	}
	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 SignalExternalWorkflowExecutionFailed {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, we.RunId, id), historyEvents)
}

func (s *FunctionalSuite) TestSignalWithStartWorkflow() {
	id := "functional-signal-with-start-workflow-test"
	wt := "functional-signal-with-start-workflow-test-type"
	tl := "functional-signal-with-start-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	// Start a workflow
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
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent, startedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			signalEvent = nil
			startedEvent = nil
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
				}
			}
			if signalEvent != nil && startedEvent != nil {
				return []*commandpb.Command{}, nil
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

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send a signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wfIDReusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.New(),
		Namespace:             s.namespace,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		Header:                header,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		SignalName:            signalName,
		SignalInput:           signalInput,
		Identity:              identity,
		WorkflowIdReusePolicy: wfIDReusePolicy,
	}
	resp, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.False(resp.Started)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = payloads.EncodeString("signal to terminate input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id

	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.True(resp.Started)
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.True(startedEvent != nil)
	s.ProtoEqual(header, startedEvent.GetWorkflowExecutionStartedEventAttributes().Header)

	// Send signal to not existed workflow
	id = "functional-signal-with-start-workflow-test-non-exist"
	signalName = "signal to non exist"
	signalInput = payloads.EncodeString("signal to non exist input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	s.True(resp.Started)
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	listOpenRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{
			ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			},
		},
	}

	// Assert visibility is correct
	s.Eventually(
		func() bool {
			listResp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), listOpenRequest)
			s.NoError(err)
			return len(listResp.Executions) == 1
		},
		waitForESToSettle,
		100*time.Millisecond,
	)

	// Terminate workflow execution and assert visibility is correct
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "kill workflow",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	s.Eventually(
		func() bool {
			listResp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), listOpenRequest)
			s.NoError(err)
			return len(listResp.Executions) == 0
		},
		waitForESToSettle,
		100*time.Millisecond,
	)

	listClosedRequest := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	}
	listClosedResp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), listClosedRequest)
	s.NoError(err)
	s.Equal(1, len(listClosedResp.Executions))
}

func (s *FunctionalSuite) TestSignalWithStartWorkflow_ResolveIDDeduplication() {
	id := "functional-signal-with-start-workflow-id-reuse-test"
	wt := "functional-signal-with-start-workflow-id-reuse-test-type"
	tl := "functional-signal-with-start-workflow-id-reuse-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start a workflow
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

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
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

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
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

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// test WorkflowIdReusePolicy: RejectDuplicate
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.New(),
		Namespace:             s.namespace,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		SignalName:            signalName,
		SignalInput:           signalInput,
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err := s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "reject duplicate workflow Id"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "allow duplicate workflow Id if last run failed"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicate
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.Started)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test WorkflowIdReusePolicyAllowDuplicateFailedOnly",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.Started)

	// test WorkflowIdReusePolicy: TerminateIfRunning (for backwards compatibility)
	prevRunID := resp.RunId
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.Started)

	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: prevRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

	// test WorkflowIdConflictPolicy: TerminateExisting (replaced TerminateIfRunning)
	prevRunID = resp.RunId
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.Started)

	descResp, err = s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: prevRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

	descResp, err = s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resp.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
}

func (s *FunctionalSuite) TestSignalWithStartWorkflow_StartDelay() {
	id := "functional-signal-with-start-workflow-start-delay-test"
	wt := "functional-signal-with-start-workflow-start-delay-test-type"
	tl := "functional-signal-with-start-workflow-start-delay-test-taskqueue"
	stickyTq := "functional-signal-with-start-workflow-start-delay-test-sticky-taskqueue"
	identity := "worker1"

	startDelay := 3 * time.Second

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")

	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		WorkflowStartDelay:  durationpb.New(startDelay),
	}

	reqStartTime := time.Now()
	we0, startErr := s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(startErr)

	var signalEvent *historypb.HistoryEvent
	delayEndTime := time.Now()

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		delayEndTime = time.Now()

		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalEvent = event
			}
		}

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		StickyTaskQueue:     &taskqueuepb.TaskQueue{Name: stickyTq, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, pollErr := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(pollErr)
	s.GreaterOrEqual(delayEndTime.Sub(reqStartTime), startDelay)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	descResp, descErr := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.RunId,
		},
	})
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
}

func (s *FunctionalSuite) TestSignalWithStartWorkflow_NoWorkflowTaskCreated() {
	id := "functional-signal-with-start-workflow-no-wft-test"
	wt := "functional-signal-with-start-workflow-no-wft-test-type"
	tl := "functional-signal-with-start-workflow-no-wft-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	we0, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we0.RunId))

	workflowComplete := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !workflowComplete {
			workflowComplete = true
			return []*commandpb.Command{}, nil
		}

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
		StickyTaskQueue:     taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// process start task
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")

	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                s.namespace,
		WorkflowId:               id,
		WorkflowType:             &commonpb.WorkflowType{Name: wt},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    nil,
		WorkflowRunTimeout:       durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
		SignalName:               signalName,
		SignalInput:              signalInput,
		SkipGenerateWorkflowTask: true,
		Identity:                 identity,
	}

	we1, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)

	// Send second signal which should generate a new wft
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we1.RunId,
		},
		SignalName:               "signal with task",
		Input:                    nil,
		Identity:                 identity,
		SkipGenerateWorkflowTask: false,
	})
	s.NoError(err)

	// process signal and complete workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we1.RunId,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionCompleted`, historyEvents)
}
