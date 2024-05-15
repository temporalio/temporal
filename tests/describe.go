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
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *FunctionalSuite) TestDescribeWorkflowExecution() {
	id := "functional-describe-wfe-test"
	wt := "functional-describe-wfe-test-type"
	tq := "functional-describe-wfe-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}
	dweResponse, err := describeWorkflowExecution()
	s.NoError(err)
	wfInfo := dweResponse.WorkflowExecutionInfo
	s.Nil(wfInfo.CloseTime)
	s.Nil(wfInfo.ExecutionDuration)
	s.Equal(int64(2), wfInfo.HistoryLength) // WorkflowStarted, WorkflowTaskScheduled
	s.Equal(wfInfo.GetStartTime(), wfInfo.GetExecutionTime())
	s.Equal(tq, wfInfo.TaskQueue)
	s.Greater(wfInfo.GetHistorySizeBytes(), int64(0))
	s.Empty(wfInfo.GetParentNamespaceId())
	s.Nil(wfInfo.GetParentExecution())
	s.NotNil(wfInfo.GetRootExecution())
	s.Equal(id, wfInfo.RootExecution.GetWorkflowId())
	s.Equal(we.RunId, wfInfo.RootExecution.GetRunId())

	// workflow logic
	workflowComplete := false
	signalSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task to schedule new activity
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Nil(wfInfo.CloseTime)
	s.Nil(wfInfo.ExecutionDuration)
	s.Equal(int64(5), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.PendingActivities))
	s.Equal("test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
	s.True(timestamp.TimeValue(dweResponse.PendingActivities[0].GetLastHeartbeatTime()).IsZero())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Equal(int64(8), wfInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, WorkflowTaskScheduled
	s.Equal(0, len(dweResponse.PendingActivities))

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, wfInfo.GetStatus())
	s.NotNil(wfInfo.CloseTime)
	s.NotNil(wfInfo.ExecutionDuration)
	s.Equal(
		wfInfo.GetCloseTime().AsTime().Sub(wfInfo.ExecutionTime.AsTime()),
		wfInfo.ExecutionDuration.AsDuration(),
	)
	s.Equal(int64(11), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowCompleted
}

func (s *FunctionalSuite) TestDescribeTaskQueue() {
	workflowID := "functional-get-poller-history"
	wt := "functional-get-poller-history-type"
	tl := "functional-get-poller-history-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
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

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	testDescribeTaskQueue := func(namespace string, taskqueue *taskqueuepb.TaskQueue, taskqueueType enumspb.TaskQueueType) []*taskqueuepb.PollerInfo {
		responseInner, errInner := s.engine.DescribeTaskQueue(NewContext(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     namespace,
			TaskQueue:     taskqueue,
			TaskQueueType: taskqueueType,
		})

		s.NoError(errInner)
		return responseInner.Pollers
	}

	before := time.Now().UTC()

	// when no one polling on the taskqueue (activity or workflow), there shall be no poller information
	tq := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	pollerInfos := testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Empty(pollerInfos)

	_, errWorkflowTask := poller.PollAndProcessWorkflowTask()
	s.NoError(errWorkflowTask)
	pollerInfos = testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())

	errActivity := poller.PollAndProcessActivityTask(false)
	s.NoError(errActivity)
	pollerInfos = testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
	pollerInfos = testDescribeTaskQueue(s.namespace, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
}
