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
	"math"
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
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *functionalSuite) TestStartWorkflowExecution() {
	id := "functional-start-workflow-test"
	wt := "functional-start-workflow-test-type"
	tl := "functional-start-workflow-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           identity,
	}

	we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	// Validate the default value for WorkflowTaskTimeoutSeconds
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.RunId,
		},
	})
	s.NoError(err)
	history := historyResponse.History
	startedEvent := history.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.Equal(10*time.Second, timestamp.DurationValue(startedEvent.GetWorkflowTaskTimeout()))

	we1, err1 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)

	newRequest := &workflowservice.StartWorkflowExecutionRequest{
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
	we2, err2 := s.engine.StartWorkflowExecution(NewContext(), newRequest)
	s.NotNil(err2)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err2)
	s.Nil(we2)
}

func (s *functionalSuite) TestStartWorkflowExecution_TerminateIfRunning() {
	id := "functional-start-workflow-terminate-if-running-test"
	wt := "functional-start-workflow-terminate-if-running-test-type"
	tl := "functional-start-workflow-terminate-if-running-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           identity,
	}

	we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	request.RequestId = uuid.New()
	request.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
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
}

func (s *functionalSuite) TestStartWorkflowExecutionWithDelay() {
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
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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

func (s *functionalSuite) TestTerminateWorkflow() {
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
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	terminateReason := "terminate reason"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:   terminateReason,
		Details:  terminateDetails,
		Identity: identity,
	})
	s.NoError(err)

	executionTerminated := false
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
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			s.Logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
		s.Equal(terminateReason, terminateEventAttributes.Reason)
		s.ProtoEqual(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}

	s.True(executionTerminated)

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

func (s *functionalSuite) TestSequentialWorkflow() {
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
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, execution.WorkflowId)
		s.Equal(activityName, activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		s.Equal(expectedActivity, s.decodePayloadsByteSliceInt32(input))
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

func (s *functionalSuite) TestCompleteWorkflowTaskAndCreateNewOne() {
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
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

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

func (s *functionalSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
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

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.EqualValues(id, execution.WorkflowId)
		s.Equal(activityName, activityType.Name)
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(activityID))
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
			historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
			})
			s.NoError(err)
			history := historyResponse.History
			common.PrettyPrint(history.Events)
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

func (s *functionalSuite) TestWorkflowRetry() {
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

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		executions = append(executions, execution)
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
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[len(events)-1].GetEventType())
		} else {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
		}
		s.Equal(int32(i), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

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
		// backwards
		startEvent := events[0]
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startEvent.GetEventType())
		attrs := startEvent.GetWorkflowExecutionStartedEventAttributes()
		if i == 0 {
			s.Equal("", attrs.ContinuedExecutionRunId)
		} else {
			s.Equal(executions[i-1].RunId, attrs.ContinuedExecutionRunId)
		}
		// forwards
		if i == maximumAttempts-1 {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[len(events)-1].GetEventType())
			attrs := events[len(events)-1].GetWorkflowExecutionCompletedEventAttributes()
			s.Equal("", attrs.NewExecutionRunId)
		} else {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
			attrs := events[len(events)-1].GetWorkflowExecutionFailedEventAttributes()
			s.Equal(executions[i+1].RunId, attrs.NewExecutionRunId)
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
		s.Equal(1, len(events))
		if i == maximumAttempts-1 {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[0].GetEventType())
			attrs := events[0].GetWorkflowExecutionCompletedEventAttributes()
			s.Equal("", attrs.NewExecutionRunId)
		} else {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[0].GetEventType())
			attrs := events[0].GetWorkflowExecutionContinuedAsNewEventAttributes()
			s.Equal(executions[i+1].RunId, attrs.NewExecutionRunId)
		}
	}
}

func (s *functionalSuite) TestWorkflowRetryFailures() {
	id := "functional-wf-retry-failures-test"
	wt := "functional-wf-retry-failures-type"
	tl := "functional-wf-retry-failures-taskqueue"
	identity := "worker1"

	workflowImpl := func(attempts int, errorReason string, nonRetryable bool, executions *[]*commonpb.WorkflowExecution) workflowTaskHandler {
		attemptCount := 1

		wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
			previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
			*executions = append(*executions, execution)
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
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[1])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(2), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[2])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(3), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

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
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())
}
