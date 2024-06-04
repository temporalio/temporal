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
	"errors"
	"math/rand"
	"time"

	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/service/history/consts"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "functional-heartbeat-test"
	wt := "functional-heartbeat-test-type"
	tl := "functional-heartbeat-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
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

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("activity-input"),
					Header:                 header,
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(task.ActivityId), tag.Counter(i))
			_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				Namespace: s.namespace,
				TaskToken: task.TaskToken,
				Details:   payloads.EncodeString("details"),
			})
			s.NoError(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == errNoTasks)

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(1, activityExecutedCount)

	// go over history and verify that the activity task scheduled event has header on it
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"Header":{"Fields":{"tracing":{"Data":"\"sample data\""}}} }
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, events)
}

func (s *FunctionalSuite) TestActivityRetry() {
	id := "functional-activity-retry-test"
	wt := "functional-activity-retry-type"
	tl := "functional-activity-retry-taskqueue"
	identity := "worker1"
	identity2 := "worker2"
	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"

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

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *historypb.HistoryEvent

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "A",
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeString("1"),
						ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
						ScheduleToStartTimeout: durationpb.New(4 * time.Second),
						StartToCloseTimeout:    durationpb.New(4 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:        durationpb.New(1 * time.Second),
							MaximumAttempts:        3,
							MaximumInterval:        durationpb.New(1 * time.Second),
							NonRetryableErrorTypes: []string{"bad-bug"},
							BackoffCoefficient:     1,
						},
					}}},
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "B",
						ActivityType:           &commonpb.ActivityType{Name: timeoutActivityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: "no_worker_taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeString("2"),
						ScheduleToCloseTimeout: durationpb.New(5 * time.Second),
						ScheduleToStartTimeout: durationpb.New(5 * time.Second),
						StartToCloseTimeout:    durationpb.New(5 * time.Second),
						HeartbeatTimeout:       durationpb.New(0 * time.Second),
					}}},
			}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				switch event.GetEventType() {
				case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
					switch event.GetActivityTaskScheduledEventAttributes().GetActivityId() {
					case "A":
						activityAScheduled = event
					case "B":
						activityBScheduled = event
					}

				case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
					if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == activityAScheduled.GetEventId() {
						activityAFailed = event
					}

				case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
					if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == activityBScheduled.GetEventId() {
						activityBTimeout = event
					}
				}
			}
		}

		if activityAFailed != nil && activityBTimeout != nil {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		return []*commandpb.Command{}, nil
	}

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		var err error
		if activityExecutedCount == 0 {
			err = errors.New("bad-luck-please-retry")
		} else if activityExecutedCount == 1 {
			err = temporal.NewNonRetryableApplicationError("bad-bug", "", nil)
		}
		activityExecutedCount++
		return nil, false, err
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

	poller2 := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == errNoTasks, err)

	descResp, err := describeWorkflowExecution()
	s.NoError(err)
	for _, pendingActivity := range descResp.GetPendingActivities() {
		if pendingActivity.GetActivityId() == "A" {
			s.NotNil(pendingActivity.GetLastFailure().GetApplicationFailureInfo())
			expectedErrString := "bad-luck-please-retry"
			s.Equal(expectedErrString, pendingActivity.GetLastFailure().GetMessage())
			s.False(pendingActivity.GetLastFailure().GetApplicationFailureInfo().GetNonRetryable())
			s.Equal(identity, pendingActivity.GetLastWorkerIdentity())
		}
	}

	err = poller2.PollAndProcessActivityTask(false)
	s.True(err == nil || err == errNoTasks, err)

	descResp, err = describeWorkflowExecution()
	s.NoError(err)
	for _, pendingActivity := range descResp.GetPendingActivities() {
		if pendingActivity.GetActivityId() == "A" {
			s.NotNil(pendingActivity.GetLastFailure().GetApplicationFailureInfo())
			expectedErrString := "bad-bug"
			s.Equal(expectedErrString, pendingActivity.GetLastFailure().GetMessage())
			s.True(pendingActivity.GetLastFailure().GetApplicationFailureInfo().GetNonRetryable())
			s.Equal(identity2, pendingActivity.GetLastWorkerIdentity())
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := 0; i < 3; i++ {
		s.False(workflowComplete)

		s.Logger.Info("Processing workflow task:", tag.Counter(i))
		_, err := poller.PollAndProcessWorkflowTask(WithRetries(1))
		if err != nil {
			s.PrintHistoryEvents(s.getHistory(s.namespace, &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.GetRunId(),
			}))
		}
		s.NoError(err, "Poll for workflow task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
	s.True(activityExecutedCount == 2)
}

func (s *FunctionalSuite) TestActivityRetry_Infinite() {
	id := "functional-activity-retry-test"
	wt := "functional-activity-retry-type"
	tl := "functional-activity-retry-taskqueue"
	identity := "worker1"
	activityName := "activity_retry"

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

	workflowComplete := false
	activitiesScheduled := false

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          "A",
						ActivityType:        &commonpb.ActivityType{Name: activityName},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:               payloads.EncodeString("1"),
						StartToCloseTimeout: durationpb.New(100 * time.Second),
						RetryPolicy: &commonpb.RetryPolicy{
							MaximumAttempts:    0,
							BackoffCoefficient: 1,
						},
					}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	activityExecutedCount := 0
	activityExecutedLimit := 4
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())

		var err error
		if activityExecutedCount < activityExecutedLimit {
			err = errors.New("retry-error")
		} else if activityExecutedCount == activityExecutedLimit {
			err = nil
		}
		activityExecutedCount++
		return nil, false, err
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	for i := 0; i <= activityExecutedLimit; i++ {
		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)
	}

	_, err = poller.PollAndProcessWorkflowTask(WithRetries(1))
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestActivityHeartBeatWorkflow_Timeout() {
	id := "functional-heartbeat-timeout-test"
	wt := "functional-heartbeat-timeout-test-type"
	tl := "functional-heartbeat-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

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

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		s.Logger.Info("Calling WorkflowTask Handler", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

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
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
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

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		// Timing out more than HB time.
		time.Sleep(2 * time.Second)
		activityExecutedCount++
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	// Not s.ErrorIs() because error goes through RPC.
	s.IsType(consts.ErrActivityTaskNotFound, err)
	s.Equal(consts.ErrActivityTaskNotFound.Error(), err.Error())

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestTryActivityCancellationFromWorkflow() {
	id := "functional-activity-cancellation-test"
	wt := "functional-activity-cancellation-test-type"
	tl := "functional-activity-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

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

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			activityScheduledID = task.StartedEventId + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(0 * time.Second),
				}},
			}}, nil
		}

		if requestCancellation {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: activityScheduledID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityCanceled := false
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(task.ActivityId), tag.Counter(i))
			response, err := s.engine.RecordActivityTaskHeartbeat(NewContext(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: s.namespace,
					TaskToken: task.TaskToken,
					Details:   payloads.EncodeString("details"),
				})
			if response != nil && response.CancelRequested {
				activityCanceled = true
				return payloads.EncodeString("Activity Cancelled"), true, nil
			}
			s.NoError(err)
			time.Sleep(10 * time.Millisecond)
		}
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks, err)

	cancelCh := make(chan struct{})
	go func() {
		s.Logger.Info("Trying to cancel the task in a different thread")
		// Send signal so that worker can send an activity cancel
		_, err1 := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			SignalName: "my signal",
			Input:      nil,
			Identity:   identity,
		})
		s.NoError(err1)

		scheduleActivity = false
		requestCancellation = true
		_, err2 := poller.PollAndProcessWorkflowTask()
		s.NoError(err2)
		close(cancelCh)
	}()

	s.Logger.Info("Start activity.")
	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == errNoTasks, err)

	s.Logger.Info("Waiting for cancel to complete.", tag.WorkflowRunID(we.RunId))
	<-cancelCh
	s.True(activityCanceled, "Activity was not cancelled.")
	s.Logger.Info("Activity cancelled.", tag.WorkflowRunID(we.RunId))
}

func (s *FunctionalSuite) TestActivityCancellationNotStarted() {
	id := "functional-activity-notstarted-cancellation-test"
	wt := "functional-activity-notstarted-cancellation-test-type"
	tl := "functional-activity-notstarted-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_notstarted"

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

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity")
			activityScheduledID = task.StartedEventId + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(0 * time.Second),
				}},
			}}, nil
		}

		if requestCancellation {
			s.Logger.Info("Requesting cancellation")
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: activityScheduledID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// dummy activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Fail("activity should not run")
		return nil, false, nil
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

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks)

	// Send signal so that worker can send an activity cancel
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
	})
	s.NoError(err)

	// Process signal in workflow and send request cancellation
	scheduleActivity = false
	requestCancellation = true
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)

	scheduleActivity = false
	requestCancellation = false
	_, err = poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks)
}

func (s *ClientFunctionalSuite) TestActivityHeartbeatDetailsDuringRetry() {
	// Latest reported heartbeat on activity should be available throughout workflow execution or until activity succeeds.
	// 1. Start workflow with single activity
	// 2. First invocation of activity sets heartbeat details and times out.
	// 3. Second invocation triggers retriable error.
	// 4. Next invocations succeed.
	// 5. Test should start polling for heartbeat details once first heartbeat was reported.
	// 6. Once workflow completes -- we're done.

	activityTimeout := time.Second

	activityExecutedCount := 0
	heartbeatDetails := 7771 // any value
	heartbeatDetailsPayload, err := payloads.Encode(heartbeatDetails)
	s.NoError(err)
	activityFn := func(ctx context.Context) error {
		var err error
		if activityExecutedCount == 0 {
			activity.RecordHeartbeat(ctx, heartbeatDetails)
			time.Sleep(activityTimeout + time.Second)
		} else if activityExecutedCount == 1 {
			time.Sleep(activityTimeout / 2)
			err = errors.New("retryable-error")
		}

		if activityExecutedCount > 0 {
			s.True(activity.HasHeartbeatDetails(ctx))
			var details int
			s.NoError(activity.GetHeartbeatDetails(ctx, &details))
			s.Equal(details, heartbeatDetails)
		}

		activityExecutedCount++
		return err
	}

	var err1 error

	activityId := "heartbeat_retry"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)

		err1 = f1.Get(ctx1, nil)

		return nil
	}

	s.worker.RegisterActivity(activityFn)
	s.worker.RegisterWorkflow(workflowFn)

	wfId := "functional-test-heartbeat-details-during-retry"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	runId := workflowRun.GetRunID()

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wfId,
				RunId:      runId,
			},
		})
	}

	time.Sleep(time.Second) // wait for the timeout to trigger

	for dweResult, dweErr := describeWorkflowExecution(); dweResult.GetWorkflowExecutionInfo().GetCloseTime() == nil; dweResult, dweErr = describeWorkflowExecution() {
		s.NoError(dweErr)
		s.NotNil(dweResult.GetWorkflowExecutionInfo())
		s.LessOrEqual(len(dweResult.PendingActivities), 1)

		if dweResult.PendingActivities != nil && len(dweResult.PendingActivities) == 1 {
			details := dweResult.PendingActivities[0].GetHeartbeatDetails()
			s.Equal(heartbeatDetailsPayload, details)
		}

		time.Sleep(time.Millisecond * 100)
	}

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	s.NoError(err1)
}

// TestActivityHeartBeat_RecordIdentity verifies that the identity of the worker sending the heartbeat
// is recorded in pending activity info and returned in describe workflow API response. This happens
// only when the worker identity is not sent when a poller picks the task.
func (s *FunctionalSuite) TestActivityHeartBeat_RecordIdentity() {
	id := "functional-heartbeat-identity-record"
	workerIdentity := "70df788a-b0b2-4113-a0d5-130f13889e35"
	activityName := "activity_timer"

	taskQueue := &taskqueuepb.TaskQueue{Name: "functional-heartbeat-identity-record-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "functional-heartbeat-identity-record-type"},
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            workerIdentity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	workflowComplete := false
	workflowNextCmd := enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		switch workflowNextCmd {
		case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
			workflowNextCmd = enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.IntToString(rand.Int()),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeString("activity-input"),
					Header:                 header,
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(60 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(60 * time.Second),
				}},
			}}, nil
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}
		panic("Unexpected workflow state")
	}

	activityStartedSignal := make(chan bool) // Used by activity channel to signal the start so that the test can verify empty identity.
	heartbeatSignalChan := make(chan bool)   // Activity task sends heartbeat when signaled on this chan. It also signals back on the same chan after sending the heartbeat.
	endActivityTask := make(chan bool)       // Activity task completes when signaled on this chan. This is to force the task to be in pending state.
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		activityStartedSignal <- true // signal the start of activity task.
		<-heartbeatSignalChan         // wait for signal before sending heartbeat.
		_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.namespace,
			TaskToken: task.TaskToken,
			Details:   payloads.EncodeString("details"),
			Identity:  workerIdentity, // explicitly set the worker identity in the heartbeat request
		})
		s.NoError(err)
		heartbeatSignalChan <- true // signal that the heartbeat is sent.

		<-endActivityTask // wait for signal before completing the task
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            "", // Do not send the worker identity.
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// execute workflow task so that an activity can be enqueued.
	_, err = poller.PollAndProcessWorkflowTask()
	s.True(err == nil || err == errNoTasks)

	// execute activity task which waits for signal before sending heartbeat.
	go func() {
		err := poller.PollAndProcessActivityTask(false)
		s.True(err == nil || err == errNoTasks)
	}()

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}
	<-activityStartedSignal // wait for the activity to start

	// Verify that the worker identity is empty.
	descRespBeforeHeartbeat, err := describeWorkflowExecution()
	s.NoError(err)
	s.Empty(descRespBeforeHeartbeat.PendingActivities[0].LastWorkerIdentity)

	heartbeatSignalChan <- true // ask the activity to send a heartbeat.
	<-heartbeatSignalChan       // wait for the heartbeat to be sent (to prevent the test from racing to describe the workflow before the heartbeat is sent)

	// Verify that the worker identity is set now.
	descRespAfterHeartbeat, err := describeWorkflowExecution()
	s.NoError(err)
	s.Equal(workerIdentity, descRespAfterHeartbeat.PendingActivities[0].LastWorkerIdentity)

	// unblock the activity task
	endActivityTask <- true

	// ensure that the workflow is complete.
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *FunctionalSuite) TestActivityTaskCompleteForceCompletion() {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.testCluster.GetHost().FrontendGRPCAddress(),
		Namespace: s.namespace,
	})
	s.NoError(err)

	activityInfo := make(chan activity.Info, 1)
	taskQueue := s.randomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, sdkClient, taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	ctx := NewContext()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.New(),
		TaskQueue: taskQueue,
	}
	run, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = sdkClient.CompleteActivityByID(ctx, s.namespace, run.GetID(), run.GetRunID(), ai.ActivityID, nil, nil)
	s.NoError(err)

	// Ensure the activity is completed and the workflow is unblcked.
	s.NoError(run.Get(ctx, nil))
}

func (s *FunctionalSuite) TestActivityTaskCompleteRejectCompletion() {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.testCluster.GetHost().FrontendGRPCAddress(),
		Namespace: s.namespace,
	})
	s.NoError(err)

	activityInfo := make(chan activity.Info, 1)
	taskQueue := s.randomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, sdkClient, taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	ctx := NewContext()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.New(),
		TaskQueue: taskQueue,
	}
	run, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = sdkClient.CompleteActivity(ctx, ai.TaskToken, nil, nil)
	var svcErr *serviceerror.NotFound
	s.ErrorAs(err, &svcErr, "invalid activityID or activity already timed out or invoking workflow is completed")
}

func (s *FunctionalSuite) mockWorkflowWithErrorActivity(activityInfo chan<- activity.Info, sdkClient sdkclient.Client, taskQueue string) (worker.Worker, func(ctx workflow.Context) error) {
	mockErrorActivity := func(ctx context.Context) error {
		ai := activity.GetInfo(ctx)
		activityInfo <- ai
		return errors.New("mock error of an activity")
	}
	wf := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 3 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				// Add long initial interval to make sure the next attempt is not scheduled
				// before the test gets a chance to complete the activity via API call.
				InitialInterval: 2 * time.Minute,
			},
		}
		ctx2 := workflow.WithActivityOptions(ctx, ao)
		var mockErrorResult error
		err := workflow.ExecuteActivity(ctx2, mockErrorActivity).Get(ctx2, &mockErrorResult)
		if err != nil {
			return err
		}
		return mockErrorResult
	}

	workflowType := "test"
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	w.RegisterActivity(mockErrorActivity)
	return w, wf
}
