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
	"context"
	"encoding/binary"
	"errors"
	"time"

	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/service/history/consts"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/matching"
)

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "integration-heartbeat-test"
	wt := "integration-heartbeat-test-type"
	tl := "integration-heartbeat-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

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
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
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
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					Header:                 header,
					ScheduleToCloseTimeout: timestamp.DurationPtr(15 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(1 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(15 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: payloads.EncodeString("details")})
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(1, activityExecutedCount)

	// go over history and verify that the activity task scheduled event has header on it
	events := s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			s.Equal(header, event.GetActivityTaskScheduledEventAttributes().Header)
		}
	}
}

func (s *integrationSuite) TestActivityRetry() {
	id := "integration-activity-retry-test"
	wt := "integration-activity-retry-type"
	tl := "integration-activity-retry-taskqueue"
	identity := "worker1"
	identity2 := "worker2"
	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"

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

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *historypb.HistoryEvent

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "A",
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
						Input:                  payloads.EncodeString("1"),
						ScheduleToCloseTimeout: timestamp.DurationPtr(4 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(4 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(4 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:        timestamp.DurationPtr(1 * time.Second),
							MaximumAttempts:        3,
							MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
							NonRetryableErrorTypes: []string{"bad-bug"},
							BackoffCoefficient:     1,
						},
					}}},
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "B",
						ActivityType:           &commonpb.ActivityType{Name: timeoutActivityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: "no_worker_taskqueue"},
						Input:                  payloads.EncodeString("2"),
						ScheduleToCloseTimeout: timestamp.DurationPtr(5 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(5 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(5 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(0 * time.Second),
					}}},
			}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

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
	s.True(err == nil || err == matching.ErrNoTasks, err)

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
		_, err := poller.PollAndProcessWorkflowTaskWithoutRetry(false, false)
		if err != nil {
			s.printWorkflowHistory(s.namespace, &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.GetRunId(),
			})
		}
		s.NoError(err, "Poll for workflow task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
	s.True(activityExecutedCount == 2)
}

func (s *integrationSuite) TestActivityRetry_Infinite() {
	id := "integration-activity-retry-test"
	wt := "integration-activity-retry-type"
	tl := "integration-activity-retry-taskqueue"
	identity := "worker1"
	activityName := "activity_retry"

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

	workflowComplete := false
	activitiesScheduled := false

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          "A",
						ActivityType:        &commonpb.ActivityType{Name: activityName},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
						Input:               payloads.EncodeString("1"),
						StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())

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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	for i := 0; i <= activityExecutedLimit; i++ {
		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)
	}

	_, err = poller.PollAndProcessWorkflowTaskWithoutRetry(false, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Timeout() {
	id := "integration-heartbeat-timeout-test"
	wt := "integration-heartbeat-timeout-test-type"
	tl := "integration-heartbeat-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

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

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

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
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(15 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(1 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(15 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	// Not s.ErrorIs() because error goes through RPC.
	s.IsType(consts.ErrActivityTaskNotFound, err)
	s.Equal(consts.ErrActivityTaskNotFound.Error(), err.Error())

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestTryActivityCancellationFromWorkflow() {
	id := "integration-activity-cancellation-test"
	wt := "integration-activity-cancellation-test-type"
	tl := "integration-activity-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

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

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			activityScheduledID = startedEventID + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(15 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(15 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(0 * time.Second),
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			response, err := s.engine.RecordActivityTaskHeartbeat(NewContext(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: payloads.EncodeString("details")})
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

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
		_, err2 := poller.PollAndProcessWorkflowTask(false, false)
		s.NoError(err2)
		close(cancelCh)
	}()

	s.Logger.Info("Start activity.")
	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	s.Logger.Info("Waiting for cancel to complete.", tag.WorkflowRunID(we.RunId))
	<-cancelCh
	s.True(activityCanceled, "Activity was not cancelled.")
	s.Logger.Info("Activity cancelled.", tag.WorkflowRunID(we.RunId))
}

func (s *integrationSuite) TestActivityCancellationNotStarted() {
	id := "integration-activity-notstarted-cancellation-test"
	wt := "integration-activity-notstarted-cancellation-test-type"
	tl := "integration-activity-notstarted-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_notstarted"

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

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity")
			activityScheduledID = startedEventID + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(15 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(15 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(0 * time.Second),
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
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

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
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	scheduleActivity = false
	requestCancellation = false
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)
}

func (s *clientIntegrationSuite) TestActivityHeartbeatDetailsDuringRetry() {
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

	wfId := "integration-test-heartbeat-details-during-retry"
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
