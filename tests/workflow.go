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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *FunctionalSuite) TestStartWorkflowExecution() {
	wt := "functional-start-workflow-test-type"
	tl := "functional-start-workflow-test-taskqueue"

	makeRequest := func() *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.New(),
			Namespace:          s.namespace,
			WorkflowId:         s.randomizeStr(s.T().Name()),
			WorkflowType:       &commonpb.WorkflowType{Name: wt},
			TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:              nil,
			WorkflowRunTimeout: durationpb.New(100 * time.Second),
			Identity:           "worker1",
		}
	}

	s.Run("start", func() {
		request := makeRequest()
		we, err := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err)
		s.True(we.Started)

		// Validate the default value for WorkflowTaskTimeoutSeconds
		historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: request.WorkflowId,
			RunId:      we.RunId,
		})
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1,"WorkflowTaskTimeout":{"Nanos":0,"Seconds":10}}
  2 WorkflowTaskScheduled`, historyEvents)
	})

	s.Run("start twice - same request", func() {
		request := makeRequest()

		we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err0)
		s.True(we0.Started)

		we1, err1 := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err1)
		s.True(we1.Started)

		s.Equal(we0.RunId, we1.RunId)
	})

	s.Run("fail when already started", func() {
		request := makeRequest()
		we, err := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err)
		s.True(we.Started)

		request.RequestId = uuid.New()

		we2, err := s.engine.StartWorkflowExecution(NewContext(), request)
		s.Error(err)
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		s.ErrorAs(err, &alreadyStarted)
		s.Nil(we2)
	})
}

func (s *FunctionalSuite) TestStartWorkflowExecution_Terminate() {
	testCases := []struct {
		name                     string
		WorkflowIdReusePolicy    enumspb.WorkflowIdReusePolicy
		WorkflowIdConflictPolicy enumspb.WorkflowIdConflictPolicy
	}{
		{
			"TerminateIfRunning id workflow reuse policy",
			enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
			enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
		},
		{
			"TerminateExisting id workflow conflict policy",
			enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
			enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		},
	}

	for i, tc := range testCases {
		s.Run(tc.name, func() {
			id := fmt.Sprintf("functional-start-workflow-terminate-test-%v", i)

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.New(),
				Namespace:          s.namespace,
				WorkflowId:         id,
				WorkflowType:       &commonpb.WorkflowType{Name: "functional-start-workflow-terminate-test-type"},
				TaskQueue:          &taskqueuepb.TaskQueue{Name: "functional-start-workflow-terminate-test-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(100 * time.Second),
				Identity:           "worker1",
			}

			we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
			s.NoError(err0)

			request.RequestId = uuid.New()
			request.WorkflowIdReusePolicy = tc.WorkflowIdReusePolicy
			request.WorkflowIdConflictPolicy = tc.WorkflowIdConflictPolicy
			we1, err1 := s.engine.StartWorkflowExecution(NewContext(), request)
			s.NoError(err1)
			s.NotEqual(we0.RunId, we1.RunId)

			descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we0.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

			descResp, err = s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we1.RunId,
				},
			})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
		})
	}
}

func (s *FunctionalSuite) TestStartWorkflowExecutionWithDelay() {
	id := "functional-start-workflow-with-delay-test"
	wt := "functional-start-workflow-with-delay-test-type"
	tl := "functional-start-workflow-with-delay-test-taskqueue"
	stickyTq := "functional-start-workflow-with-delay-test-sticky-taskqueue"
	identity := "worker1"

	startDelay := 3 * time.Second

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           identity,
		WorkflowStartDelay: durationpb.New(startDelay),
	}

	reqStartTime := time.Now()
	we0, startErr := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(startErr)

	delayEndTime := time.Now()
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		delayEndTime = time.Now()
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

func (s *FunctionalSuite) TestTerminateWorkflow() {
	id := "functional-terminate-workflow-test"
	wt := "functional-terminate-workflow-test-type"
	tl := "functional-terminate-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:   "terminate reason",
		Details:  payloads.EncodeString("terminate details"),
		Identity: identity,
	})
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			s.Logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}
		break GetHistoryLoop
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`, historyEvents)

	newExecutionStarted := false
StartNewExecutionLoop:
	for i := 0; i < 10; i++ {
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

		newExecution, err := s.engine.StartWorkflowExecution(NewContext(), request)
		if err != nil {
			s.Logger.Warn("Start New Execution failed. Error", tag.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue StartNewExecutionLoop
		}

		s.Logger.Info("New Execution Started with the same ID", tag.WorkflowID(id),
			tag.WorkflowRunID(newExecution.RunId))
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *FunctionalSuite) TestSequentialWorkflow() {
	id := "functional-sequential-workflow-test"
	wt := "functional-sequential-workflow-test-type"
	tl := "functional-sequential-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

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

	workflowComplete := false
	activityCount := int32(10)
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

	expectedActivity := int32(1)
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, task.WorkflowExecution.WorkflowId)
		s.Equal(activityName, task.ActivityType.Name)
		id, _ := strconv.Atoi(task.ActivityId)
		s.Equal(int(expectedActivity), id)
		s.Equal(expectedActivity, s.decodePayloadsByteSliceInt32(task.Input))
		expectedActivity++

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
		if i%2 == 0 {
			err = poller.PollAndProcessActivityTask(false)
		} else { // just for testing respondActivityTaskCompleteByID
			err = poller.PollAndProcessActivityTaskWithID(false)
		}
		s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestCompleteWorkflowTaskAndCreateNewOne() {
	id := "functional-complete-workflow-task-create-new-test"
	wt := "functional-complete-workflow-task-create-new-test-type"
	tl := "functional-complete-workflow-task-create-new-test-taskqueue"
	identity := "worker1"

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

	commandCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if commandCount < 2 {
			commandCount++
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "test-marker",
				}},
			}}, nil
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
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	res, err := poller.PollAndProcessWorkflowTask(WithForceNewWorkflowTask)
	s.NoError(err)
	newTask := res.NewTask
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	s.Equal(int64(3), newTask.WorkflowTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.WorkflowTask.GetStartedEventId())
	s.Equal(4, len(newTask.WorkflowTask.History.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, newTask.WorkflowTask.History.Events[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, newTask.WorkflowTask.History.Events[1].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, newTask.WorkflowTask.History.Events[2].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, newTask.WorkflowTask.History.Events[3].GetEventType())
}

func (s *FunctionalSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
	id := "functional-timeouts-workflow-test"
	wt := "functional-timeouts-workflow-test-type"
	tl := "functional-timeouts-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

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

	workflowComplete := false
	activityCount := int32(4)
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
					ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(1 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		s.Logger.Info("Completing enums")

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, task.WorkflowExecution.WorkflowId)
		s.Equal(activityName, task.ActivityType.Name)
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(task.ActivityId))
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 8; i++ {
		dropWorkflowTask := (i%2 == 0)
		s.Logger.Info("Calling Workflow Task", tag.Counter(i))
		var err error
		if dropWorkflowTask {
			_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithDropTask)
		} else {
			_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory, WithExpectedAttemptCount(2))
		}
		if err != nil {
			s.PrintHistoryEventsCompact(s.getHistory(s.namespace, &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			}))
		}
		s.True(err == nil || err == errNoTasks, err)
		if !dropWorkflowTask {
			s.Logger.Info("Calling PollAndProcessActivityTask", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || err == errNoTasks)
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestWorkflowRetry() {
	id := "functional-wf-retry-test"
	wt := "functional-wf-retry-type"
	tl := "functional-wf-retry-taskqueue"
	identity := "worker1"

	initialInterval := 1 * time.Second
	backoffCoefficient := 1.5
	maximumAttempts := 5
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
		RetryPolicy: &commonpb.RetryPolicy{
			// Intentionally test server-initialization of Initial Interval value (which should be 1 second)
			MaximumAttempts:        int32(maximumAttempts),
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     backoffCoefficient,
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	attemptCount := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		attemptCount++
		if attemptCount > maximumAttempts {
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("succeed-after-retry"),
					}},
				}}, nil
		}
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func(execution *commonpb.WorkflowExecution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: execution,
		})
	}

	for i := 1; i <= maximumAttempts; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
		events := s.getHistory(s.namespace, executions[i-1])
		if i == maximumAttempts {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"Attempt":%d}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, i), events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"Attempt":%d}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, i), events)
		}

		dweResponse, err := describeWorkflowExecution(executions[i-1])
		s.NoError(err)
		backoff := time.Duration(0)
		if i > 1 {
			backoff = time.Duration(initialInterval.Seconds()*math.Pow(backoffCoefficient, float64(i-2))) * time.Second
			// retry backoff cannot larger than MaximumIntervalInSeconds
			if backoff > time.Second {
				backoff = time.Second
			}
		}
		expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().AsTime().Add(backoff)
		s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))
	}

	// Check run id links
	for i := 0; i < maximumAttempts; i++ {
		events := s.getHistory(s.namespace, executions[i])
		if i == 0 {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":""}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i+1].RunId), events)
		} else if i == maximumAttempts-1 {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, executions[i-1].RunId), events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted {"ContinuedExecutionRunId":"%s"}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed {"NewExecutionRunId":"%s"}`, executions[i-1].RunId, executions[i+1].RunId), events)
		}

		// Test get history from old SDKs
		// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
		// See comment in workflowHandler.go:GetWorkflowExecutionHistory
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		oldSDKCtx := headers.SetVersionsForTests(ctx, "1.3.1", headers.ClientNameJavaSDK, headers.SupportedServerVersions, "")
		resp, err := s.engine.GetWorkflowExecutionHistory(oldSDKCtx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              s.namespace,
			Execution:              executions[i],
			MaximumPageSize:        5,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		})
		cancel()
		s.NoError(err)
		events = resp.History.Events
		if i == maximumAttempts-1 {
			s.EqualHistoryEvents(`
  5 WorkflowExecutionCompleted {"NewExecutionRunId":""}`, events)
		} else {
			s.EqualHistoryEvents(fmt.Sprintf(`
  5 WorkflowExecutionContinuedAsNew {"NewExecutionRunId":"%s"}`, executions[i+1].RunId), events)
		}
	}
}

func (s *FunctionalSuite) TestWorkflowRetryFailures() {
	id := "functional-wf-retry-failures-test"
	wt := "functional-wf-retry-failures-type"
	tl := "functional-wf-retry-failures-taskqueue"
	identity := "worker1"

	workflowImpl := func(attempts int, errorReason string, nonRetryable bool, executions *[]*commonpb.WorkflowExecution) workflowTaskHandler {
		attemptCount := 1

		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			*executions = append(*executions, task.WorkflowExecution)
			attemptCount++
			if attemptCount > attempts {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("succeed-after-retry"),
						}},
					}}, nil
			}
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
						// Reason:  "retryable-error",
						Failure: failure.NewServerFailure(errorReason, nonRetryable),
					}},
				}}, nil
		}

		return wtHandler
	}

	// Fail using attempt
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
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution
	wtHandler := workflowImpl(5, "retryable-error", false, &executions)
	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(s.namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":2}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[2])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":3}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	// Fail error reason
	request = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}

	we, err0 = s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	executions = []*commonpb.WorkflowExecution{}
	wtHandler = workflowImpl(5, "bad-bug", true, &executions)
	poller = &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)
}

func (s *FunctionalSuite) TestExecuteMultiOperation() {
	runMultiOp := func(
		tv *testvars.TestVars,
		request *workflowservice.ExecuteMultiOperationRequest,
	) (resp *workflowservice.ExecuteMultiOperationResponse, retErr error) {
		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		poller := &TaskPoller{
			Engine:    s.engine,
			Namespace: s.namespace,
			TaskQueue: tv.TaskQueue(),
			Identity:  tv.WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				return nil, nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				if len(task.Messages) > 0 {
					updRequestMsg := task.Messages[0]
					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				}
				return nil, nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		// issue multi operation request
		done := make(chan struct{})
		go func() {
			resp, retErr = s.engine.ExecuteMultiOperation(NewContext(), request)
			done <- struct{}{}
		}()

		_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
		s.NoError(err)

		// wait for request to complete
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-ctx.Done():
			s.Fail("timed out waiting for result of ExecuteMultiOperation")
		case <-done:
		}

		// make sure there's no lock contention
		s.Empty(capture.Snapshot()[metrics.TaskWorkflowBusyCounter.Name()])

		return
	}

	s.Run("StartWorkflow + UpdateWorkflow", func() {
		runUpdateWithStart := func(
			tv *testvars.TestVars,
			startReq *workflowservice.StartWorkflowExecutionRequest,
			updateReq *workflowservice.UpdateWorkflowExecutionRequest,
		) (*workflowservice.ExecuteMultiOperationResponse, error) {
			resp, err := runMultiOp(tv,
				&workflowservice.ExecuteMultiOperationRequest{
					Namespace: s.namespace,
					Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
						{
							Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
								StartWorkflow: startReq,
							},
						},
						{
							Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
								UpdateWorkflow: updateReq,
							},
						},
					},
				})

			if err == nil {
				s.Len(resp.Responses, 2)

				startRes := resp.Responses[0].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow).StartWorkflow
				s.NotZero(startRes.RunId)

				updateRes := resp.Responses[1].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow).UpdateWorkflow
				s.NotNil(updateRes.Outcome)
				s.NotZero(updateRes.Outcome.String())
			}

			return resp, err
		}

		startWorkflowReq := func(tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
			return &workflowservice.StartWorkflowExecutionRequest{
				Namespace:    s.namespace,
				WorkflowId:   tv.WorkflowID(),
				WorkflowType: tv.WorkflowType(),
				TaskQueue:    tv.TaskQueue(),
				Identity:     tv.WorkerIdentity(),
			}
		}

		updateWorkflowReq := func(tv *testvars.TestVars) *workflowservice.UpdateWorkflowExecutionRequest {
			return &workflowservice.UpdateWorkflowExecutionRequest{
				Namespace: s.namespace,
				Request: &updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
					Input: &updatepb.Input{Name: tv.Any().String(), Args: tv.Any().Payloads()},
				},
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
				WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
			}
		}

		s.Run("workflow is not running", func() {
			tv := testvars.New(s.T().Name())

			_, err := runUpdateWithStart(tv, startWorkflowReq(tv), updateWorkflowReq(tv))
			s.NoError(err)
		})

		s.Run("workflow is running", func() {

			s.Run("workflow id reuse policy use-existing: only send update", func() {
				tv := testvars.New(s.T().Name())

				_, err := s.engine.StartWorkflowExecution(NewContext(), startWorkflowReq(tv))
				s.NoError(err)

				req := startWorkflowReq(tv)
				req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
				_, err = runUpdateWithStart(tv, req, updateWorkflowReq(tv))

				s.NoError(err)
			})

			s.Run("workflow id reuse policy terminate-existing: terminate workflow first, then start and update", func() {
				tv := testvars.New(s.T().Name())

				initReq := startWorkflowReq(tv)
				initReq.TaskQueue.Name = initReq.TaskQueue.Name + "-init" // avoid race condition with poller
				initWF, err := s.engine.StartWorkflowExecution(NewContext(), initReq)
				s.NoError(err)

				req := startWorkflowReq(tv)
				req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
				_, err = runUpdateWithStart(tv, req, updateWorkflowReq(tv))

				s.NoError(err)

				descResp, err := s.engine.DescribeWorkflowExecution(NewContext(),
					&workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: s.namespace,
						Execution: &commonpb.WorkflowExecution{WorkflowId: req.WorkflowId, RunId: initWF.RunId},
					})

				s.NoError(err)
				s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)
			})

			s.Run("workflow id reuse policy fail: abort multi operation", func() {
				tv := testvars.New(s.T().Name())

				_, err := s.engine.StartWorkflowExecution(NewContext(), startWorkflowReq(tv))
				s.NoError(err)

				req := startWorkflowReq(tv)
				req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
				_, err = runUpdateWithStart(tv, req, updateWorkflowReq(tv))

				s.NotNil(err)
				s.Equal(err.Error(), "MultiOperation could not be executed.")
				errs := err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				s.Contains(errs[0].Error(), "Workflow execution is already running")
				s.Equal("Operation was aborted.", errs[1].Error())
			})
		})
	})
}
