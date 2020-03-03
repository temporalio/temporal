// Copyright (c) 2016 Uber Technologies, Inc.
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

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/service/matching"
)

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "integration-heartbeat-test"
	wt := "integration-heartbeat-test-type"
	tl := "integration-heartbeat-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	header := &commonproto.Header{
		Fields: map[string][]byte{"tracing": []byte("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		Header:                              header,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
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
		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: []byte("details")})
			s.NoError(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	events := s.getHistory(s.domainName, &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	for _, event := range events {
		if event.GetEventType() == enums.EventTypeActivityTaskScheduled {
			s.Equal(header, event.GetActivityTaskScheduledEventAttributes().Header)
		}
	}
}

func (s *integrationSuite) TestActivityHeartbeatDetailsDuringRetry() {
	id := "integration-heartbeat-details-retry-test"
	wt := "integration-heartbeat-details-retry-type"
	tl := "integration-heartbeat-details-retry-tasklist"
	identity := "worker1"
	activityName := "activity_heartbeat_retry"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return nil, []*commonproto.Decision{
				{
					DecisionType: enums.DecisionTypeScheduleActivityTask,
					Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "0",
						ActivityType:                  &commonproto.ActivityType{Name: activityName},
						TaskList:                      &commonproto.TaskList{Name: tl},
						Input:                         nil,
						ScheduleToCloseTimeoutSeconds: 4,
						ScheduleToStartTimeoutSeconds: 4,
						StartToCloseTimeoutSeconds:    4,
						HeartbeatTimeoutSeconds:       1,
						RetryPolicy: &commonproto.RetryPolicy{
							InitialIntervalInSeconds:    1,
							MaximumAttempts:             3,
							MaximumIntervalInSeconds:    1,
							BackoffCoefficient:          1,
							ExpirationIntervalInSeconds: 100,
						},
					},
					}},
			}, nil
		}

		workflowComplete = true
		s.Logger.Info("Completing Workflow")
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	heartbeatDetails := []byte("details")
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
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
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

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
			s.Equal(enums.PendingActivityStateScheduled, pendingActivity.GetState())
			if i == 0 {
				s.Equal("temporalInternal:Timeout TimeoutTypeHeartbeat", pendingActivity.GetLastFailureReason())
				s.Nil(pendingActivity.GetLastFailureDetails())
			} else { // i == 1
				expectedErrString := "retryable-error"
				s.Equal(expectedErrString, pendingActivity.GetLastFailureReason())
				s.Equal([]byte(expectedErrString), pendingActivity.GetLastFailureDetails())
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
	tl := "integration-activity-retry-tasklist"
	identity := "worker1"
	identity2 := "worker2"
	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *commonproto.HistoryEvent

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return nil, []*commonproto.Decision{
				{
					DecisionType: enums.DecisionTypeScheduleActivityTask,
					Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "A",
						ActivityType:                  &commonproto.ActivityType{Name: activityName},
						TaskList:                      &commonproto.TaskList{Name: tl},
						Input:                         []byte("1"),
						ScheduleToCloseTimeoutSeconds: 4,
						ScheduleToStartTimeoutSeconds: 4,
						StartToCloseTimeoutSeconds:    4,
						HeartbeatTimeoutSeconds:       1,
						RetryPolicy: &commonproto.RetryPolicy{
							InitialIntervalInSeconds:    1,
							MaximumAttempts:             3,
							MaximumIntervalInSeconds:    1,
							NonRetriableErrorReasons:    []string{"bad-bug"},
							BackoffCoefficient:          1,
							ExpirationIntervalInSeconds: 100,
						},
					}}},
				{
					DecisionType: enums.DecisionTypeScheduleActivityTask,
					Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "B",
						ActivityType:                  &commonproto.ActivityType{Name: timeoutActivityName},
						TaskList:                      &commonproto.TaskList{Name: "no_worker_tasklist"},
						Input:                         []byte("2"),
						ScheduleToCloseTimeoutSeconds: 5,
						ScheduleToStartTimeoutSeconds: 5,
						StartToCloseTimeoutSeconds:    5,
						HeartbeatTimeoutSeconds:       0,
					}}},
			}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				switch event.GetEventType() {
				case enums.EventTypeActivityTaskScheduled:
					switch event.GetActivityTaskScheduledEventAttributes().GetActivityId() {
					case "A":
						activityAScheduled = event
					case "B":
						activityBScheduled = event
					}

				case enums.EventTypeActivityTaskFailed:
					if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == activityAScheduled.GetEventId() {
						activityAFailed = event
					}

				case enums.EventTypeActivityTaskTimedOut:
					if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == activityBScheduled.GetEventId() {
						activityBTimeout = event
					}
				}
			}
		}

		if activityAFailed != nil && activityBTimeout != nil {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done"),
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		var err error
		if activityExecutedCount == 0 {
			err = errors.New("bad-luck-please-retry")
		} else if activityExecutedCount == 1 {
			err = errors.New("bad-bug")
		}
		activityExecutedCount++
		return nil, false, err
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	poller2 := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
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
		if pendingActivity.GetActivityID() == "A" {
			expectedErrString := "bad-luck-please-retry"
			s.Equal(expectedErrString, pendingActivity.GetLastFailureReason())
			s.Equal([]byte(expectedErrString), pendingActivity.GetLastFailureDetails())
			s.Equal(identity, pendingActivity.GetLastWorkerIdentity())
		}
	}

	err = poller2.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	descResp, err = describeWorkflowExecution()
	s.NoError(err)
	for _, pendingActivity := range descResp.GetPendingActivities() {
		if pendingActivity.GetActivityID() == "A" {
			expectedErrString := "bad-bug"
			s.Equal(expectedErrString, pendingActivity.GetLastFailureReason())
			s.Equal([]byte(expectedErrString), pendingActivity.GetLastFailureDetails())
			s.Equal(identity2, pendingActivity.GetLastWorkerIdentity())
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := 0; i < 3; i++ {
		s.False(workflowComplete)

		s.Logger.Info("Processing decision task:", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTaskWithoutRetry(false, false)
		if err != nil {
			s.printWorkflowHistory(s.domainName, &commonproto.WorkflowExecution{
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
	tl := "integration-heartbeat-timeout-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		s.Logger.Info("Calling DecisionTask Handler", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		// Timing out more than HB time.
		time.Sleep(2 * time.Second)
		activityExecutedCount++
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	tl := "integration-activity-timeout-test-tasklist"
	identity := "worker1"
	activityName := "timeout_activity"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 300,
		TaskStartToCloseTimeoutSeconds:      2,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	activitiesMap := map[int64]*commonproto.HistoryEvent{}
	failWorkflow := false
	failReason := ""
	var activityATimedout, activityBTimedout, activityCTimedout, activityDTimedout bool
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "A",
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: "NoWorker"},
					Input:                         []byte("ScheduleToStart"),
					ScheduleToCloseTimeoutSeconds: 35,
					ScheduleToStartTimeoutSeconds: 3, // ActivityID A is expected to timeout using ScheduleToStart
					StartToCloseTimeoutSeconds:    30,
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "B",
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("ScheduleClose"),
					ScheduleToCloseTimeoutSeconds: 7, // ActivityID B is expected to timeout using ScheduleClose
					ScheduleToStartTimeoutSeconds: 5,
					StartToCloseTimeoutSeconds:    10,
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "C",
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("StartToClose"),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    5, // ActivityID C is expected to timeout using StartToClose
					HeartbeatTimeoutSeconds:       0,
				},
				}}, {
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "D",
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("Heartbeat"),
					ScheduleToCloseTimeoutSeconds: 35,
					ScheduleToStartTimeoutSeconds: 20,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       3, // ActivityID D is expected to timeout using Heartbeat
				}}},
			}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeActivityTaskScheduled {
					activitiesMap[event.GetEventId()] = event
				}

				if event.GetEventType() == enums.EventTypeActivityTaskTimedOut {
					timeoutEvent := event.GetActivityTaskTimedOutEventAttributes()
					scheduledEvent, ok := activitiesMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						return nil, []*commonproto.Decision{{
							DecisionType: enums.DecisionTypeFailWorkflowExecution,
							Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
								Reason: "ScheduledEvent not found",
							}},
						}}, nil
					}

					switch timeoutEvent.GetTimeoutType() {
					case enums.TimeoutTypeScheduleToStart:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "A" {
							activityATimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID A is expected to timeout with ScheduleToStart"
						}
					case enums.TimeoutTypeScheduleToClose:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "B" {
							activityBTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID B is expected to timeout with ScheduleToClose"
						}
					case enums.TimeoutTypeStartToClose:
						if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetActivityId() == "C" {
							activityCTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID C is expected to timeout with StartToClose"
						}
					case enums.TimeoutTypeHeartbeat:
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
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeFailWorkflowExecution,
				Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
					Reason: failReason,
				}},
			}}, nil
		}

		if activityATimedout && activityBTimedout && activityCTimedout && activityDTimedout {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done"),
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		timeoutType := string(input)
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
						TaskToken: taskToken, Details: []byte(string(i))})
					s.NoError(err)
					time.Sleep(1 * time.Second)
				}
				s.Logger.Info("End Heartbeating")
			}()
			s.Logger.Info("Sleeping hearbeat activity")
			time.Sleep(10 * time.Second)
		}

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	tl := "integration-activity-heartbeat-timeout-test-tasklist"
	identity := "worker1"
	activityName := "timeout_activity"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 70,
		TaskStartToCloseTimeoutSeconds:      2,
		Identity:                            identity,
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
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			var decisions []*commonproto.Decision
			for i := 0; i < activityCount; i++ {
				aID := fmt.Sprintf("activity_%v", i)
				d := &commonproto.Decision{
					DecisionType: enums.DecisionTypeScheduleActivityTask,
					Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    aID,
						ActivityType:                  &commonproto.ActivityType{Name: activityName},
						TaskList:                      &commonproto.TaskList{Name: tl},
						Input:                         []byte("Heartbeat"),
						ScheduleToCloseTimeoutSeconds: 60,
						ScheduleToStartTimeoutSeconds: 5,
						StartToCloseTimeoutSeconds:    60,
						HeartbeatTimeoutSeconds:       5,
					}},
				}

				decisions = append(decisions, d)
			}

			return nil, decisions, nil
		} else if previousStartedEventID > 0 {
		ProcessLoop:
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeActivityTaskScheduled {
					lastHeartbeatMap[event.GetEventId()] = 0
				}

				if event.GetEventType() == enums.EventTypeActivityTaskCompleted ||
					event.GetEventType() == enums.EventTypeActivityTaskFailed {
					failWorkflow = true
					failReason = "Expected activities to timeout but seeing completion instead"
				}

				if event.GetEventType() == enums.EventTypeActivityTaskTimedOut {
					timeoutEvent := event.GetActivityTaskTimedOutEventAttributes()
					_, ok := lastHeartbeatMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						failWorkflow = true
						failReason = "ScheduledEvent not found"
						break ProcessLoop
					}

					switch timeoutEvent.GetTimeoutType() {
					case enums.TimeoutTypeHeartbeat:
						activitiesTimedout++
						scheduleID := timeoutEvent.GetScheduledEventId()
						lastHeartbeat, _ := strconv.Atoi(string(timeoutEvent.Details))
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
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeFailWorkflowExecution,
				Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
					Reason: failReason,
				}},
			}}, nil
		}

		if activitiesTimedout == activityCount {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done"),
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Logger.Info("Starting heartbeat activity", tag.WorkflowActivityID(activityID))
		for i := 0; i < 10; i++ {
			if !workflowComplete {
				s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
				_, err := s.engine.RecordActivityTaskHeartbeat(NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: []byte(strconv.Itoa(i))})
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

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	tl := "integration-activity-cancellation-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       0,
				}},
			}}, nil
		}

		if requestCancellation {
			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRequestCancelActivityTask,
				Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
					ActivityId: strconv.Itoa(int(activityCounter)),
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			response, err := s.engine.RecordActivityTaskHeartbeat(NewContext(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: []byte("details")})
			if response != nil && response.CancelRequested {
				return []byte("Activity Cancelled"), true, nil
			}
			s.NoError(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	tl := "integration-activity-notstarted-cancellation-test-tasklist"
	identity := "worker1"
	activityName := "activity_notstarted"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity")
			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 15,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    15,
					HeartbeatTimeoutSeconds:       0,
				}},
			}}, nil
		}

		if requestCancellation {
			s.Logger.Info("Requesting cancellation")
			return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRequestCancelActivityTask,
				Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
					ActivityId: strconv.Itoa(int(activityCounter)),
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")
		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// dummy activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Fail("activity should not run")
		return nil, false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
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
	signalInput := []byte("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
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
