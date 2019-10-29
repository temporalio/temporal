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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/service/matching"
)

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "integration-heartbeat-test"
	wt := "integration-heartbeat-test-type"
	tl := "integration-heartbeat-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	header := &workflow.Header{
		Fields: map[string][]byte{"tracing": []byte("sample data")},
	}

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		Header:                              header,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					Header:                        header,
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
				},
			}}, nil
		}

		s.Logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			_, err := s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: []byte("details")})
			s.Nil(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
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

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
	s.True(activityExecutedCount == 1)

	// go over history and verify that the activity task scheduled event has header on it
	events := s.getHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(we.GetRunId()),
	})
	for _, event := range events {
		if *event.EventType == workflow.EventTypeActivityTaskScheduled {
			s.Equal(header, event.ActivityTaskScheduledEventAttributes.Header)
		}
	}
}

func (s *integrationSuite) TestActivityHeartbeatDetailsDuringRetry() {
	id := "integration-heartbeat-details-retry-test"
	wt := "integration-heartbeat-details-retry-type"
	tl := "integration-heartbeat-details-retry-tasklist"
	identity := "worker1"
	activityName := "activity_heartbeat_retry"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activitiesScheduled := false

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return nil, []*workflow.Decision{
				{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
					ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    common.StringPtr("0"),
						ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
						TaskList:                      &workflow.TaskList{Name: &tl},
						Input:                         nil,
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(4),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(4),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(4),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
						RetryPolicy: &workflow.RetryPolicy{
							InitialIntervalInSeconds:    common.Int32Ptr(1),
							MaximumAttempts:             common.Int32Ptr(3),
							MaximumIntervalInSeconds:    common.Int32Ptr(1),
							BackoffCoefficient:          common.Float64Ptr(1),
							ExpirationIntervalInSeconds: common.Int32Ptr(100),
						},
					},
				},
			}, nil
		}

		workflowComplete = true
		s.Logger.Info("Completing Workflow.")
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	activityExecutedCount := 0
	heartbeatDetails := []byte("details")
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)

		var err error
		if activityExecutedCount == 0 {
			s.Logger.Info("Heartbeating for activity:", tag.WorkflowActivityID(activityID))
			_, err = s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: heartbeatDetails})
			s.Nil(err)
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

	describeWorkflowExecution := func() (*workflow.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(createContext(), &workflow.DescribeWorkflowExecutionRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
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
			s.Nil(err)
		}

		dweResponse, err := describeWorkflowExecution()
		s.Nil(err)

		pendingActivities := dweResponse.GetPendingActivities()
		if i == 2 {
			// third time, complete activity, no pending info
			s.Equal(0, len(pendingActivities))
		} else {
			s.Equal(1, len(pendingActivities))
			pendingActivity := pendingActivities[0]

			s.Equal(int32(3), pendingActivity.GetMaximumAttempts())
			s.Equal(int32(i+1), pendingActivity.GetAttempt())
			s.Equal(workflow.PendingActivityStateScheduled, pendingActivity.GetState())
			if i == 0 {
				s.Equal("cadenceInternal:Timeout HEARTBEAT", pendingActivity.GetLastFailureReason())
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
			s.NotNil(scheduledTS)
			s.NotNil(lastHeartbeatTS)
			s.NotNil(expirationTS)
			s.Nil(pendingActivity.LastStartedTimestamp)
			s.True(*scheduledTS > *lastHeartbeatTS)
			s.True(*expirationTS > *scheduledTS)

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

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *workflow.HistoryEvent

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return nil, []*workflow.Decision{
				{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
					ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    common.StringPtr("A"),
						ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
						TaskList:                      &workflow.TaskList{Name: &tl},
						Input:                         []byte("1"),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(4),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(4),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(4),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
						RetryPolicy: &workflow.RetryPolicy{
							InitialIntervalInSeconds:    common.Int32Ptr(1),
							MaximumAttempts:             common.Int32Ptr(3),
							MaximumIntervalInSeconds:    common.Int32Ptr(1),
							NonRetriableErrorReasons:    []string{"bad-bug"},
							BackoffCoefficient:          common.Float64Ptr(1),
							ExpirationIntervalInSeconds: common.Int32Ptr(100),
						},
					},
				},
				{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
					ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    common.StringPtr("B"),
						ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(timeoutActivityName)},
						TaskList:                      &workflow.TaskList{Name: common.StringPtr("no_worker_tasklist")},
						Input:                         []byte("2"),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(5),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(5),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
					},
				}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				switch event.GetEventType() {
				case workflow.EventTypeActivityTaskScheduled:
					switch event.ActivityTaskScheduledEventAttributes.GetActivityId() {
					case "A":
						activityAScheduled = event
					case "B":
						activityBScheduled = event
					}

				case workflow.EventTypeActivityTaskFailed:
					if event.ActivityTaskFailedEventAttributes.GetScheduledEventId() == activityAScheduled.GetEventId() {
						activityAFailed = event
					}

				case workflow.EventTypeActivityTaskTimedOut:
					if event.ActivityTaskTimedOutEventAttributes.GetScheduledEventId() == activityBScheduled.GetEventId() {
						activityBTimeout = event
					}
				}
			}
		}

		if activityAFailed != nil && activityBTimeout != nil {
			s.Logger.Info("Completing Workflow.")
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done."),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
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

	describeWorkflowExecution := func() (*workflow.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(createContext(), &workflow.DescribeWorkflowExecutionRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      we.RunId,
			},
		})
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	descResp, err := describeWorkflowExecution()
	s.Nil(err)
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
	s.Nil(err)
	for _, pendingActivity := range descResp.GetPendingActivities() {
		if pendingActivity.GetActivityID() == "A" {
			expectedErrString := "bad-bug"
			s.Equal(expectedErrString, pendingActivity.GetLastFailureReason())
			s.Equal([]byte(expectedErrString), pendingActivity.GetLastFailureDetails())
			s.Equal(identity2, pendingActivity.GetLastWorkerIdentity())
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))
	for i := 0; i < 3; i++ {
		s.False(workflowComplete)

		s.Logger.Info("Processing decision task:", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTaskWithoutRetry(false, false)
		if err != nil {
			s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			})
		}
		s.Nil(err, "Poll for decision task failed.")

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

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		s.Logger.Info("Calling DecisionTask Handler: %d, %d.", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
				},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		// Timing out more than HB time.
		time.Sleep(2 * time.Second)
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
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

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityTimeouts() {
	id := "integration-activity-timeout-test"
	wt := "integration-activity-timeout-test-type"
	tl := "integration-activity-timeout-test-tasklist"
	identity := "worker1"
	activityName := "timeout_activity"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	activitiesMap := map[int64]*workflow.HistoryEvent{}
	failWorkflow := false
	failReason := ""
	var activityATimedout, activityBTimedout, activityCTimedout, activityDTimedout bool
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr("A"),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: common.StringPtr("NoWorker")},
					Input:                         []byte("ScheduleToStart"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(35),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(3), // ActivityID A is expected to timeout using ScheduleToStart
					StartToCloseTimeoutSeconds:    common.Int32Ptr(30),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr("B"),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         []byte("ScheduleClose"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(7), // ActivityID B is expected to timeout using ScheduleClose
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(10),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr("C"),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         []byte("StartToClose"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(5), // ActivityID C is expected to timeout using StartToClose
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr("D"),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         []byte("Heartbeat"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(35),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(20),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(3), // ActivityID D is expected to timeout using Heartbeat
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == workflow.EventTypeActivityTaskScheduled {
					activitiesMap[event.GetEventId()] = event
				}

				if event.GetEventType() == workflow.EventTypeActivityTaskTimedOut {
					timeoutEvent := event.ActivityTaskTimedOutEventAttributes
					scheduledEvent, ok := activitiesMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						return nil, []*workflow.Decision{{
							DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
							FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
								Reason: common.StringPtr("ScheduledEvent not found."),
							},
						}}, nil
					}

					switch timeoutEvent.GetTimeoutType() {
					case workflow.TimeoutTypeScheduleToStart:
						if scheduledEvent.ActivityTaskScheduledEventAttributes.GetActivityId() == "A" {
							activityATimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID A is expected to timeout with ScheduleToStart"
						}
					case workflow.TimeoutTypeScheduleToClose:
						if scheduledEvent.ActivityTaskScheduledEventAttributes.GetActivityId() == "B" {
							activityBTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID B is expected to timeout with ScheduleToClose"
						}
					case workflow.TimeoutTypeStartToClose:
						if scheduledEvent.ActivityTaskScheduledEventAttributes.GetActivityId() == "C" {
							activityCTimedout = true
						} else {
							failWorkflow = true
							failReason = "ActivityID C is expected to timeout with StartToClose"
						}
					case workflow.TimeoutTypeHeartbeat:
						if scheduledEvent.ActivityTaskScheduledEventAttributes.GetActivityId() == "D" {
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
			s.Logger.Error("Failing workflow.")
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
				FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
					Reason: common.StringPtr(failReason),
				},
			}}, nil
		}

		if activityATimedout && activityBTimedout && activityCTimedout && activityDTimedout {
			s.Logger.Info("Completing Workflow.")
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done."),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		timeoutType := string(input)
		switch timeoutType {
		case "ScheduleToStart":
			s.Fail("Activity A not expected to be started.")
		case "ScheduleClose":
			s.Logger.Info("Sleeping activityB for 6 seconds.")
			time.Sleep(7 * time.Second)
		case "StartToClose":
			s.Logger.Info("Sleeping activityC for 6 seconds.")
			time.Sleep(8 * time.Second)
		case "Heartbeat":
			s.Logger.Info("Starting hearbeat activity.")
			go func() {
				for i := 0; i < 6; i++ {
					s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
					_, err := s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
						TaskToken: taskToken, Details: []byte(string(i))})
					s.Nil(err)
					time.Sleep(1 * time.Second)
				}
				s.Logger.Info("End Heartbeating.")
			}()
			s.Logger.Info("Sleeping hearbeat activity.")
			time.Sleep(10 * time.Second)
		}

		return []byte("Activity Result."), false, nil
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
			s.Logger.Info("Activity Processing Completed.  Error", tag.Error(err))
		}()
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))
	for i := 0; i < 10; i++ {
		s.Logger.Info("Processing decision task: %v", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Nil(err, "Poll for decision task failed.")

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

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(70),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	lastHeartbeatMap := make(map[int64]int)
	failWorkflow := false
	failReason := ""
	activityCount := 10
	activitiesTimedout := 0
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !activitiesScheduled {
			activitiesScheduled = true
			decisions := []*workflow.Decision{}
			for i := 0; i < activityCount; i++ {
				aID := fmt.Sprintf("activity_%v", i)
				d := &workflow.Decision{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
					ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    common.StringPtr(aID),
						ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
						TaskList:                      &workflow.TaskList{Name: &tl},
						Input:                         []byte("Heartbeat"),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(60),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(60),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
					},
				}

				decisions = append(decisions, d)
			}

			return nil, decisions, nil
		} else if previousStartedEventID > 0 {
		ProcessLoop:
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == workflow.EventTypeActivityTaskScheduled {
					lastHeartbeatMap[event.GetEventId()] = 0
				}

				if event.GetEventType() == workflow.EventTypeActivityTaskCompleted ||
					event.GetEventType() == workflow.EventTypeActivityTaskFailed {
					failWorkflow = true
					failReason = "Expected activities to timeout but seeing completion instead"
				}

				if event.GetEventType() == workflow.EventTypeActivityTaskTimedOut {
					timeoutEvent := event.ActivityTaskTimedOutEventAttributes
					_, ok := lastHeartbeatMap[timeoutEvent.GetScheduledEventId()]
					if !ok {
						failWorkflow = true
						failReason = "ScheduledEvent not found."
						break ProcessLoop
					}

					switch timeoutEvent.GetTimeoutType() {
					case workflow.TimeoutTypeHeartbeat:
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
			s.Logger.Error("Failing workflow. Reason: %v", tag.Value(failReason))
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
				FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
					Reason: common.StringPtr(failReason),
				},
			}}, nil
		}

		if activitiesTimedout == activityCount {
			s.Logger.Info("Completing Workflow.")
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done."),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Logger.Info("Starting heartbeat activity. ID", tag.WorkflowActivityID(activityID))
		for i := 0; i < 10; i++ {
			if !workflowComplete {
				s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
				_, err := s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: []byte(strconv.Itoa(i))})
				if err != nil {
					s.Logger.Error("Activity heartbeat failed", tag.WorkflowActivityID(activityID), tag.Counter(i), tag.Error(err))
				}

				secondsToSleep := rand.Intn(3)
				s.Logger.Info("Activity ID '%v' sleeping for: %v seconds", tag.WorkflowActivityID(activityID), tag.Number(int64(secondsToSleep)))
				time.Sleep(time.Duration(secondsToSleep) * time.Second)
			}
		}
		s.Logger.Info("End Heartbeating.", tag.WorkflowActivityID(activityID))

		s.Logger.Info("Sleeping activity before completion", tag.WorkflowActivityID(activityID))
		time.Sleep(7 * time.Second)

		return []byte("Activity Result."), false, nil
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
			s.Logger.Info("Activity Processing Completed.", tag.Error(err))
		}()
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))
	for i := 0; i < 10; i++ {
		s.Logger.Info("Processing decision task", tag.Counter(i))
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Nil(err, "Poll for decision task failed.")

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

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}}, nil
		}

		if requestCancellation {
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
				RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
					ActivityId: common.StringPtr(strconv.Itoa(int(activityCounter))),
				},
			}}, nil
		}

		s.Logger.Info("Completing Workflow.")

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(activityID), tag.Counter(i))
			response, err := s.engine.RecordActivityTaskHeartbeat(createContext(),
				&workflow.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: []byte("details")})
			if *response.CancelRequested {
				return []byte("Activity Cancelled."), true, nil
			}
			s.Nil(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
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
		s.Logger.Info("Trying to cancel the task in a different thread.")
		scheduleActivity = false
		requestCancellation = true
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.True(err == nil || err == matching.ErrNoTasks, err)
		cancelCh <- struct{}{}
	}()

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	<-cancelCh
	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(*we.RunId))
}

func (s *integrationSuite) TestActivityCancellationNotStarted() {
	id := "integration-activity-notstarted-cancellation-test"
	wt := "integration-activity-notstarted-cancellation-test-type"
	tl := "integration-activity-notstarted-cancellation-test-tasklist"
	identity := "worker1"
	activityName := "activity_notstarted"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity.")
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}}, nil
		}

		if requestCancellation {
			s.Logger.Info("Requesting cancellation.")
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
				RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
					ActivityId: common.StringPtr(strconv.Itoa(int(activityCounter))),
				},
			}}, nil
		}

		s.Logger.Info("Completing Workflow.")
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// dummy activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
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
	signalInput := []byte("my signal input.")
	err = s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
		SignalName: common.StringPtr(signalName),
		Input:      signalInput,
		Identity:   common.StringPtr(identity),
	})
	s.Nil(err)

	// Process signal in decider and send request cancellation
	scheduleActivity = false
	requestCancellation = true
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)

	scheduleActivity = false
	requestCancellation = false
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)
}
