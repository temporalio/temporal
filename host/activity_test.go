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
	"math/rand"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal"
	enumspb "go.temporal.io/temporal-proto/enums/v1"

	commonpb "go.temporal.io/temporal-proto/common/v1"
	decisionpb "go.temporal.io/temporal-proto/decision/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	taskqueuepb "go.temporal.io/temporal-proto/taskqueue/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		Header:                     header,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					Header:                        header,
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       1,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
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

func (s *integrationSuite) TestActivityHeartbeatDetailsDuringRetry() {
	id := "integration-heartbeat-details-retry-test"
	wt := "integration-heartbeat-details-retry-type"
	tl := "integration-heartbeat-details-retry-taskqueue"
	identity := "worker1"
	activityName := "activity_heartbeat_retry"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return []*decisionpb.Decision{
				{
					DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "0",
						ActivityType:                  &commonpb.ActivityType{Name: activityName},
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
						Input:                         nil,
						ScheduleToCloseTimeoutSeconds: 4,
						ScheduleToStartTimeoutSeconds: 4,
						StartToCloseTimeoutSeconds:    4,
						HeartbeatTimeoutSeconds:       1,
						RetryPolicy: &commonpb.RetryPolicy{
							InitialIntervalInSeconds: 1,
							MaximumAttempts:          3,
							MaximumIntervalInSeconds: 1,
							BackoffCoefficient:       1,
						},
					},
					}},
			}, nil
		}

		workflowComplete = true
		s.Logger.Info("Completing Workflow")
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	heartbeatDetails := payloads.EncodeString("details")
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())

		var err error
		if activityExecutedCount == 0 {
			s.Logger.Info("Heartbeating for activity:", tag.WorkflowActivityID(activityID))
			_, err = s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: heartbeatDetails})
			s.NoError(err)
			// Trigger heartbeat timeout and retry
			time.Sleep(time.Second * 2)
		} else if activityExecutedCount == 1 {
			// return an error and retry
			err = errors.New("retryable-error")
		}

		activityExecutedCount++
		return nil, false, err
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
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

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err, err)

	for i := 0; i != 3; i++ {
		err = poller.PollAndProcessActivityTask(false)
		if i == 0 {
			// first time, hearbeat timeout, respond activity complete will fail
			s.Error(err)
		} else {
			// second time, retryable error
			s.NoError(err)
		}

		dweResponse, err := describeWorkflowExecution()
		s.NoError(err)

		pendingActivities := dweResponse.GetPendingActivities()
		if i == 2 {
			// third time, complete activity, no pending info
			s.Equal(0, len(pendingActivities))
		} else {
			s.Equal(1, len(pendingActivities))
			pendingActivity := pendingActivities[0]

			s.Equal(int32(3), pendingActivity.GetMaximumAttempts())
			s.Equal(int32(i+1), pendingActivity.GetAttempt())
			s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pendingActivity.GetState())
			if i == 0 {
				s.Equal(failure.NewTimeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT), pendingActivity.GetLastFailure())
			} else { // i == 1
				expectedErrString := "retryable-error"
				s.NotNil(pendingActivity.GetLastFailure().GetApplicationFailureInfo())
				s.Equal(expectedErrString, pendingActivity.GetLastFailure().GetMessage())
				s.False(pendingActivity.GetLastFailure().GetApplicationFailureInfo().GetNonRetryable())
			}
			s.Equal(identity, pendingActivity.GetLastWorkerIdentity())

			scheduledTS := pendingActivity.ScheduledTimestamp
			lastHeartbeatTS := pendingActivity.LastHeartbeatTimestamp
			expirationTS := pendingActivity.ExpirationTimestamp
			s.NotZero(scheduledTS)
			s.NotZero(lastHeartbeatTS)
			s.NotZero(expirationTS)
			s.Zero(pendingActivity.LastStartedTimestamp)
			s.True(scheduledTS > lastHeartbeatTS)
			s.True(expirationTS > scheduledTS)

			s.Equal(heartbeatDetails, pendingActivity.GetHeartbeatDetails())
		}
	}

	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.True(err == nil, err)

	s.True(workflowComplete)
	s.Equal(3, activityExecutedCount)
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *historypb.HistoryEvent

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*decisionpb.Decision{
				{
					DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "A",
						ActivityType:                  &commonpb.ActivityType{Name: activityName},
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
						Input:                         payloads.EncodeString("1"),
						ScheduleToCloseTimeoutSeconds: 4,
						ScheduleToStartTimeoutSeconds: 4,
						StartToCloseTimeoutSeconds:    4,
						HeartbeatTimeoutSeconds:       1,
						RetryPolicy: &commonpb.RetryPolicy{
							InitialIntervalInSeconds: 1,
							MaximumAttempts:          3,
							MaximumIntervalInSeconds: 1,
							NonRetryableErrorTypes:   []string{"bad-bug"},
							BackoffCoefficient:       1,
						},
					}}},
				{
					DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "B",
						ActivityType:                  &commonpb.ActivityType{Name: timeoutActivityName},
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: "no_worker_taskqueue"},
						Input:                         payloads.EncodeString("2"),
						ScheduleToCloseTimeoutSeconds: 5,
						ScheduleToStartTimeoutSeconds: 5,
						StartToCloseTimeoutSeconds:    5,
						HeartbeatTimeoutSeconds:       0,
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
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		return []*decisionpb.Decision{}, nil
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	poller2 := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
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

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

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

		s.Logger.Info("Processing decision task:", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTaskWithoutRetry(false, false)
		if err != nil {
			s.printWorkflowHistory(s.namespace, &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.GetRunId(),
			})
		}
		s.NoError(err, "Poll for decision task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
	s.True(activityExecutedCount == 2)
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

		s.Logger.Info("Calling DecisionTask Handler", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityTimeouts() {
	id := "integration-activity-timeout-test"
	wt := "integration-activity-timeout-test-type"
	tl := "integration-activity-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "timeout_activity"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  300,
		WorkflowTaskTimeoutSeconds: 2,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	activitiesMap := map[int64]*historypb.HistoryEvent{}
	failWorkflow := false
	failReason := ""
	var activityATimedout, activityBTimedout, activityCTimedout, activityDTimedout bool
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "A",
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: "NoWorker"},
					Input:                         payloads.EncodeString("ScheduleToStart"),
					ScheduleToCloseTimeoutSeconds: 35,
					ScheduleToStartTimeoutSeconds: 3, // ActivityID A is expected to timeout using ScheduleToStart
					StartToCloseTimeoutSeconds:    30,
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "B",
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeString("ScheduleClose"),
					ScheduleToCloseTimeoutSeconds: 7, // ActivityID B is expected to timeout using ScheduleClose
					ScheduleToStartTimeoutSeconds: 5,
					StartToCloseTimeoutSeconds:    10,
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "C",
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeString("StartToClose"),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    5, // ActivityID C is expected to timeout using StartToClose
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "D",
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeString("Heartbeat"),
					ScheduleToCloseTimeoutSeconds: 35,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       3, // ActivityID D is expected to timeout using Heartbeat
				}}},
			}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
					activitiesMap[event.GetEventId()] = event
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT {
					timeoutEvent := event.GetActivityTaskTimedOutEventAttributes()
					scheduledEvent, ok := activitiesMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						return []*decisionpb.Decision{{
							DecisionType: enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION,
							Attributes: &decisionpb.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &decisionpb.FailWorkflowExecutionDecisionAttributes{
								Failure: failure.NewServerFailure("ScheduledEvent not found", false),
							}},
						}}, nil
					}

					switch timeoutEvent.GetFailure().GetTimeoutFailureInfo().GetTimeoutType() {
					case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "A" {
							activityATimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID A is expected to timeout with ScheduleToStart"
						}
					case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "B" {
							activityBTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID B is expected to timeout with ScheduleToClose"
						}
					case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "C" {
							activityCTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID C is expected to timeout with StartToClose"
						}
					case enumspb.TIMEOUT_TYPE_HEARTBEAT:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "D" {
							activityDTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID D is expected to timeout with Heartbeat"
						}
					}
				}
			}
		}

		if failWorkflow {
			s.Logger.Error("Failing workflow")
			workflowComplete = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &decisionpb.FailWorkflowExecutionDecisionAttributes{
					Failure: failure.NewServerFailure(failReason, false),
				}},
			}}, nil
		}

		if activityATimedout && activityBTimedout && activityCTimedout && activityDTimedout {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		return []*decisionpb.Decision{}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		var timeoutType string
		err := payloads.Decode(input, &timeoutType)
		s.NoError(err)
		switch timeoutType {
		case "ScheduleToStart":
			s.Fail("Activity A not expected to be started")
		case "ScheduleClose":
			s.Logger.Info("Sleeping activityB for 6 seconds")
			time.Sleep(7 * time.Second)
		case "StartToClose":
			s.Logger.Info("Sleeping activityC for 6 seconds")
			time.Sleep(8 * time.Second)
		case "Heartbeat":
			s.Logger.Info("Starting hearbeat activity")
			go func() {
				for i := 0; i < 6; i++ {
					s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
					_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
						TaskToken: taskToken, Details: payloads.EncodeString(string(i))})
					s.NoError(err)
					time.Sleep(1 * time.Second)
				}
				s.Logger.Info("End Heartbeating")
			}()
			s.Logger.Info("Sleeping hearbeat activity")
			time.Sleep(10 * time.Second)
		}

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	for i := 0; i < 3; i++ {
		go func() {
			err = poller.PollAndProcessActivityTask(false)
			s.Logger.Error("Activity Processing Completed", tag.Error(err))
		}()
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := 0; i < 10; i++ {
		s.Logger.Info("Processing decision task", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.NoError(err, "Poll for decision task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityHeartbeatTimeouts() {
	id := "integration-activity-heartbeat-timeout-test"
	wt := "integration-activity-heartbeat-timeout-test-type"
	tl := "integration-activity-heartbeat-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "timeout_activity"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  70,
		WorkflowTaskTimeoutSeconds: 2,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	lastHeartbeatMap := make(map[int64]int)
	failWorkflow := false
	failReason := ""
	activityCount := 10
	activitiesTimedout := 0
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			var decisions []*decisionpb.Decision
			for i := 0; i < activityCount; i++ {
				aID := fmt.Sprintf("activity_%v", i)
				d := &decisionpb.Decision{
					DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    aID,
						ActivityType:                  &commonpb.ActivityType{Name: activityName},
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
						Input:                         payloads.EncodeString("Heartbeat"),
						ScheduleToCloseTimeoutSeconds: 60,
						ScheduleToStartTimeoutSeconds: 5,
						StartToCloseTimeoutSeconds:    60,
						HeartbeatTimeoutSeconds:       5,
					}},
				}

				decisions = append(decisions, d)
			}

			return decisions, nil
		} else if previousStartedEventID > 0 {
		ProcessLoop:
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
					lastHeartbeatMap[event.GetEventId()] = 0
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED ||
					event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
					failWorkflow = true
					failReason = "Expected activities to timeout but seeing completion instead"
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT {
					timeoutEvent := event.GetActivityTaskTimedOutEventAttributes()
					_, ok := lastHeartbeatMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						failWorkflow = true
						failReason = "ScheduledEvent not found"
						break ProcessLoop
					}

					switch timeoutEvent.GetFailure().GetTimeoutFailureInfo().GetTimeoutType() {
					case enumspb.TIMEOUT_TYPE_HEARTBEAT:
						activitiesTimedout++
						scheduleID := timeoutEvent.GetScheduledEventId()
						var details string
						err := payloads.Decode(timeoutEvent.GetFailure().GetTimeoutFailureInfo().GetLastHeartbeatDetails(), &details)
						s.NoError(err)
						lastHeartbeat, _ := strconv.Atoi(details)
						lastHeartbeatMap[scheduleID] = lastHeartbeat
					default:
						failWorkflow = true
						failReason = "Expected Heartbeat timeout but recieved another timeout"
						break ProcessLoop
					}
				}
			}
		}

		if failWorkflow {
			s.Logger.Error("Failing workflow", tag.Value(failReason))
			workflowComplete = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &decisionpb.FailWorkflowExecutionDecisionAttributes{
					Failure: failure.NewServerFailure(failReason, false),
				}},
			}}, nil
		}

		if activitiesTimedout == activityCount {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		return []*decisionpb.Decision{}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		s.Logger.Info("Starting heartbeat activity", tag.WorkflowActivityID(activityID))
		for i := 0; i < 10; i++ {
			if !workflowComplete {
				s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
				_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: payloads.EncodeString(strconv.Itoa(i))})
				if err != nil {
					s.Logger.Error("Activity heartbeat failed", tag.WorkflowActivityID(activityID), tag.Counter(i), tag.Error(err))
				}

				secondsToSleep := rand.Intn(3)
				s.Logger.Info("Activity is sleeping", tag.WorkflowActivityID(activityID), tag.Number(int64(secondsToSleep)))
				time.Sleep(time.Duration(secondsToSleep) * time.Second)
			}
		}
		s.Logger.Info("End Heartbeating", tag.WorkflowActivityID(activityID))

		s.Logger.Info("Sleeping activity before completion", tag.WorkflowActivityID(activityID))
		time.Sleep(7 * time.Second)

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	for i := 0; i < activityCount; i++ {
		go func() {
			err := poller.PollAndProcessActivityTask(false)
			s.Logger.Error("Activity Processing Completed", tag.Error(err))
		}()
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := 0; i < 10; i++ {
		s.Logger.Info("Processing decision task", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.NoError(err, "Poll for decision task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
	s.False(failWorkflow, failReason)
	s.Equal(activityCount, activitiesTimedout)
	s.Equal(activityCount, len(lastHeartbeatMap))
	for aID, lastHeartbeat := range lastHeartbeatMap {
		s.Logger.Info("Last heartbeat for activity with scheduleID", tag.Counter(int(aID)), tag.Number(int64(lastHeartbeat)))
		s.Equal(9, lastHeartbeat)
	}
}

func (s *integrationSuite) TestActivityCancellation() {
	id := "integration-activity-cancellation-test"
	wt := "integration-activity-cancellation-test-type"
	tl := "integration-activity-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduleID := int64(0)

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			activityScheduleID = startedEventID + 2
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       0,
				}},
			}}, nil
		}

		if requestCancellation {
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &decisionpb.RequestCancelActivityTaskDecisionAttributes{
					ScheduledEventId: activityScheduleID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
			response, err := s.engine.RecordActivityTaskHeartbeat(NewContext(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: payloads.EncodeString("details")})
			if response != nil && response.CancelRequested {
				return payloads.EncodeString("Activity Cancelled"), true, nil
			}
			s.NoError(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	cancelCh := make(chan struct{})

	go func() {
		s.Logger.Info("Trying to cancel the task in a different thread")
		scheduleActivity = false
		requestCancellation = true
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.True(err == nil || err == matching.ErrNoTasks, err)
		cancelCh <- struct{}{}
	}()

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	<-cancelCh
	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduleID := int64(0)

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity")
			activityScheduleID = startedEventID + 2
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       0,
				}},
			}}, nil
		}

		if requestCancellation {
			s.Logger.Info("Requesting cancellation")
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &decisionpb.RequestCancelActivityTaskDecisionAttributes{
					ScheduledEventId: activityScheduleID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
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

	// Process signal in decider and send request cancellation
	scheduleActivity = false
	requestCancellation = true
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)

	scheduleActivity = false
	requestCancellation = false
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)
}
