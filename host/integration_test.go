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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/errordetails"
	"go.temporal.io/temporal-proto/workflowservice"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/service/matching"
)

type (
	integrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		IntegrationBase
	}
)

func (s *integrationSuite) SetupSuite() {
	s.setupSuite("testdata/integration_test_cluster.yaml")
}

func (s *integrationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *integrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func TestIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(integrationSuite))
}

func (s *integrationSuite) TestStartWorkflowExecution() {
	id := "integration-start-workflow-test"
	wt := "integration-start-workflow-test-type"
	tl := "integration-start-workflow-test-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we1, err1 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err1)
	s.Equal(we0.RunId, we1.RunId)

	newRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}
	we2, err2 := s.engine.StartWorkflowExecution(NewContext(), newRequest)
	s.NotNil(err2)
	st2 := status.Convert(err2)
	s.True(errordetails.IsWorkflowExecutionAlreadyStartedFailure(st2))
	log.Errorf("Unable to start workflow execution: %v", err2)
	s.Nil(we2)
}

func (s *integrationSuite) TestTerminateWorkflow() {
	id := "integration-terminate-workflow-test"
	wt := "integration-terminate-workflow-test-type"
	tl := "integration-terminate-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	terminateReason := "terminate reason"
	terminateDetails := []byte("terminate details")
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
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
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != enums.EventTypeWorkflowExecutionTerminated {
			s.Logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
		s.Equal(terminateReason, terminateEventAttributes.Reason)
		s.Equal(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}

	s.True(executionTerminated)

	newExecutionStarted := false
StartNewExecutionLoop:
	for i := 0; i < 10; i++ {
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:                           uuid.New(),
			Domain:                              s.domainName,
			WorkflowId:                          id,
			WorkflowType:                        &commonproto.WorkflowType{Name: wt},
			TaskList:                            &commonproto.TaskList{Name: tl},
			Input:                               nil,
			ExecutionStartToCloseTimeoutSeconds: 100,
			TaskStartToCloseTimeoutSeconds:      1,
			Identity:                            identity,
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

func (s *integrationSuite) TestSequentialWorkflow() {
	id := "integration-sequential-workflow-test"
	wt := "integration-sequential-workflow-test-type"
	tl := "integration-sequential-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(10)
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
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
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

	expectedActivity := int32(1)
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.WorkflowId)
		s.Equal(activityName, activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestCompleteDecisionTaskAndCreateNewOne() {
	id := "integration-complete-decision-create-new-test"
	wt := "integration-complete-decision-create-new-test-type"
	tl := "integration-complete-decision-create-new-test-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	decisionCount := 0
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if decisionCount < 2 {
			decisionCount++
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "test-marker",
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		StickyTaskList:  &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		false,
		false,
		true,
		true,
		int64(0),
		1,
		true,
		nil)
	s.NoError(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)

	s.Equal(int64(3), newTask.DecisionTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.DecisionTask.GetStartedEventId())
	s.Equal(4, len(newTask.DecisionTask.History.Events))
	s.Equal(enums.EventTypeDecisionTaskCompleted, newTask.DecisionTask.History.Events[0].GetEventType())
	s.Equal(enums.EventTypeMarkerRecorded, newTask.DecisionTask.History.Events[1].GetEventType())
	s.Equal(enums.EventTypeDecisionTaskScheduled, newTask.DecisionTask.History.Events[2].GetEventType())
	s.Equal(enums.EventTypeDecisionTaskStarted, newTask.DecisionTask.History.Events[3].GetEventType())
}

func (s *integrationSuite) TestDecisionAndActivityTimeoutsWorkflow() {
	id := "integration-timeouts-workflow-test"
	wt := "integration-timeouts-workflow-test-type"
	tl := "integration-timeouts-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(4)
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
					ScheduleToCloseTimeoutSeconds: 1,
					ScheduleToStartTimeoutSeconds: 1,
					StartToCloseTimeoutSeconds:    1,
					HeartbeatTimeoutSeconds:       1,
				}},
			}}, nil
		}

		s.Logger.Info("Completing enums")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.WorkflowId)
		s.Equal(activityName, activityType.Name)
		s.Logger.Info("Activity ID", tag.WorkflowActivityID(activityID))
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	for i := 0; i < 8; i++ {
		dropDecisionTask := (i%2 == 0)
		s.Logger.Info("Calling Decision Task", tag.Counter(i))
		var err error
		if dropDecisionTask {
			_, err = poller.PollAndProcessDecisionTask(true, true)
		} else {
			_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
		}
		if err != nil {
			historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
				Domain: s.domainName,
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
			})
			s.NoError(err)
			history := historyResponse.History
			common.PrettyPrintHistory(history, s.Logger)
		}
		s.True(err == nil || err == matching.ErrNoTasks, "%v", err)
		if !dropDecisionTask {
			s.Logger.Info("Calling PollAndProcessActivityTask", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || err == matching.ErrNoTasks)
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestWorkflowRetry() {
	id := "integration-wf-retry-test"
	wt := "integration-wf-retry-type"
	tl := "integration-wf-retry-tasklist"
	identity := "worker1"

	initialIntervalInSeconds := 1
	backoffCoefficient := 1.5
	maximumAttempts := 5
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    int32(initialIntervalInSeconds),
			MaximumAttempts:             int32(maximumAttempts),
			MaximumIntervalInSeconds:    1,
			NonRetriableErrorReasons:    []string{"bad-bug"},
			BackoffCoefficient:          backoffCoefficient,
			ExpirationIntervalInSeconds: 100,
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonproto.WorkflowExecution

	attemptCount := 0

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		executions = append(executions, execution)
		attemptCount++
		if attemptCount == maximumAttempts {
			return nil, []*commonproto.Decision{
				{
					DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
					Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
						Result: []byte("succeed-after-retry"),
					}},
				}}, nil
		}
		return nil, []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeFailWorkflowExecution,
				Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
					Reason:  "retryable-error",
					Details: nil,
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	describeWorkflowExecution := func(execution *commonproto.WorkflowExecution) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Domain:    s.domainName,
			Execution: execution,
		})
	}

	for i := 0; i != maximumAttempts; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.True(err == nil, err)
		events := s.getHistory(s.domainName, executions[i])
		if i == maximumAttempts-1 {
			s.Equal(enums.EventTypeWorkflowExecutionCompleted, events[len(events)-1].GetEventType())
		} else {
			s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, events[len(events)-1].GetEventType())
		}
		s.Equal(int32(i), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

		dweResponse, err := describeWorkflowExecution(executions[i])
		s.NoError(err)
		backoff := time.Duration(0)
		if i > 0 {
			backoff = time.Duration(float64(initialIntervalInSeconds)*math.Pow(backoffCoefficient, float64(i-1))) * time.Second
			// retry backoff cannot larger than MaximumIntervalInSeconds
			if backoff > time.Second {
				backoff = time.Second
			}
		}
		expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime() + backoff.Nanoseconds()
		s.Equal(expectedExecutionTime, dweResponse.WorkflowExecutionInfo.GetExecutionTime())
	}
}

func (s *integrationSuite) TestWorkflowRetryFailures() {
	id := "integration-wf-retry-failures-test"
	wt := "integration-wf-retry-failures-type"
	tl := "integration-wf-retry-failures-tasklist"
	identity := "worker1"

	workflowImpl := func(attempts int, errorReason string, executions *[]*commonproto.WorkflowExecution) decisionTaskHandler {
		attemptCount := 0

		dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
			previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
			*executions = append(*executions, execution)
			attemptCount++
			if attemptCount == attempts {
				return nil, []*commonproto.Decision{
					{
						DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
						Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
							Result: []byte("succeed-after-retry"),
						}},
					}}, nil
			}
			return nil, []*commonproto.Decision{
				{
					DecisionType: enums.DecisionTypeFailWorkflowExecution,
					Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
						//Reason:  "retryable-error",
						Reason:  errorReason,
						Details: nil,
					}},
				}}, nil
		}

		return dtHandler
	}

	// Fail using attempt
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			MaximumAttempts:             3,
			MaximumIntervalInSeconds:    1,
			NonRetriableErrorReasons:    []string{"bad-bug"},
			BackoffCoefficient:          1,
			ExpirationIntervalInSeconds: 100,
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonproto.WorkflowExecution
	dtHandler := workflowImpl(5, "retryable-error", &executions)
	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)
	events := s.getHistory(s.domainName, executions[0])
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, events[len(events)-1].GetEventType())
	s.Equal(int32(0), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.domainName, executions[1])
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.domainName, executions[2])
	s.Equal(enums.EventTypeWorkflowExecutionFailed, events[len(events)-1].GetEventType())
	s.Equal(int32(2), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// Fail error reason
	request = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			MaximumAttempts:             3,
			MaximumIntervalInSeconds:    1,
			NonRetriableErrorReasons:    []string{"bad-bug"},
			BackoffCoefficient:          1,
			ExpirationIntervalInSeconds: 100,
		},
	}

	we, err0 = s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	executions = []*commonproto.WorkflowExecution{}
	dtHandler = workflowImpl(5, "bad-bug", &executions)
	poller = &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.domainName, executions[0])
	s.Equal(enums.EventTypeWorkflowExecutionFailed, events[len(events)-1].GetEventType())
	s.Equal(int32(0), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())
}

func (s *integrationSuite) TestCronWorkflow() {
	id := "integration-wf-cron-test"
	wt := "integration-wf-cron-type"
	tl := "integration-wf-cron-tasklist"
	identity := "worker1"
	cronSchedule := "@every 3s"

	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Millisecond * 500

	memo := &commonproto.Memo{
		Fields: map[string][]byte{"memoKey": []byte("memoVal")},
	}
	searchAttr := &commonproto.SearchAttributes{
		IndexedFields: map[string][]byte{
			"CustomKeywordField": []byte("1"),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		CronSchedule:                        cronSchedule, //minimum interval by standard spec is 1m (* * * * *, use non-standard descriptor for short interval for test
		Memo:                                memo,
		SearchAttributes:                    searchAttr,
	}

	startWorkflowTS := time.Now()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonproto.WorkflowExecution

	attemptCount := 0

	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		executions = append(executions, execution)
		attemptCount++
		if attemptCount == 2 {
			return nil, []*commonproto.Decision{
				{
					DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
					Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
						Result: []byte("cron-test-result"),
					}},
				}}, nil
		}
		return nil, []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeFailWorkflowExecution,
				Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
					Reason:  "cron-test-error",
					Details: nil,
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	startFilter := &commonproto.StartTimeFilter{}
	startFilter.EarliestTime = startWorkflowTS.UnixNano()
	startFilter.LatestTime = time.Now().UnixNano()

	// Sleep some time before checking the open executions.
	// This will not cost extra time as the polling for first decision task will be blocked for 3 seconds.
	time.Sleep(2 * time.Second)
	resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
		Domain:          s.domainName,
		MaximumPageSize: 100,
		StartTimeFilter: startFilter,
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	})
	s.NoError(err)
	s.Equal(1, len(resp.GetExecutions()))
	executionInfo := resp.GetExecutions()[0]
	s.Equal(targetBackoffDuration.Nanoseconds(), executionInfo.GetExecutionTime()-executionInfo.GetStartTime())

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	// Make sure the cron workflow start running at a proper time, in this case 3 seconds after the
	// startWorkflowExecution request
	backoffDuration := time.Now().Sub(startWorkflowTS)
	s.True(backoffDuration > targetBackoffDuration)
	s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	s.Equal(3, attemptCount)

	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(terminateErr)
	events := s.getHistory(s.domainName, executions[0])
	lastEvent := events[len(events)-1]
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, lastEvent.GetEventType())
	attributes := lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enums.ContinueAsNewInitiatorCronSchedule, attributes.GetInitiator())
	s.Equal("cron-test-error", attributes.GetFailureReason())
	s.Equal(0, len(attributes.GetLastCompletionResult()))
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	events = s.getHistory(s.domainName, executions[1])
	lastEvent = events[len(events)-1]
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, lastEvent.GetEventType())
	attributes = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enums.ContinueAsNewInitiatorCronSchedule, attributes.GetInitiator())
	s.Equal("", attributes.GetFailureReason())
	s.Equal("cron-test-result", string(attributes.GetLastCompletionResult()))
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	events = s.getHistory(s.domainName, executions[2])
	lastEvent = events[len(events)-1]
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, lastEvent.GetEventType())
	attributes = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enums.ContinueAsNewInitiatorCronSchedule, attributes.GetInitiator())
	s.Equal("cron-test-error", attributes.GetFailureReason())
	s.Equal("cron-test-result", string(attributes.GetLastCompletionResult()))
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	startFilter.LatestTime = time.Now().UnixNano()
	var closedExecutions []*commonproto.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == 4 {
			closedExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.NotNil(closedExecutions)
	dweResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime() + 3*time.Second.Nanoseconds()
	s.Equal(expectedExecutionTime, dweResponse.WorkflowExecutionInfo.GetExecutionTime())

	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime() < closedExecutions[j].GetStartTime()
	})
	lastExecution := closedExecutions[0]
	for i := 1; i != 4; i++ {
		executionInfo := closedExecutions[i]
		// Roundup to compare on the precision of seconds
		expectedBackoff := executionInfo.GetExecutionTime()/1000000000 - lastExecution.GetExecutionTime()/1000000000
		// The execution time calculate based on last execution close time
		// However, the current execution time is based on the current start time
		// This code is to remove the diff between current start time and last execution close time
		// TODO: Remove this line once we unify the time source
		executionTimeDiff := executionInfo.GetStartTime()/1000000000 - lastExecution.GetCloseTime()/1000000000
		// The backoff between any two executions should be multiplier of the target backoff duration which is 3 in this test
		s.Equal(int64(0), int64(expectedBackoff-executionTimeDiff)%(targetBackoffDuration.Nanoseconds()/1000000000))
		lastExecution = executionInfo
	}
}

func (s *integrationSuite) TestCronWorkflowTimeout() {
	id := "integration-wf-cron-timeout-test"
	wt := "integration-wf-cron-timeout-type"
	tl := "integration-wf-cron-timeout-tasklist"
	identity := "worker1"
	cronSchedule := "@every 3s"

	memo := &commonproto.Memo{
		Fields: map[string][]byte{
			"memoKey": []byte("memoVal"),
		},
	}
	searchAttr := &commonproto.SearchAttributes{
		IndexedFields: map[string][]byte{
			"CustomKeywordField": []byte("1"),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 1, // set workflow timeout to 1s
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		CronSchedule:                        cronSchedule, //minimum interval by standard spec is 1m (* * * * *), use non-standard descriptor for short interval for test
		Memo:                                memo,
		SearchAttributes:                    searchAttr,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonproto.WorkflowExecution
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, h *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		executions = append(executions, execution)
		return nil, []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,

				Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
					TimerId:                   "timer-id",
					StartToFireTimeoutSeconds: 5,
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	time.Sleep(1 * time.Second) // wait for workflow timeout

	// check when workflow timeout, continueAsNew event contains expected fields
	events := s.getHistory(s.domainName, executions[0])
	lastEvent := events[len(events)-1]
	s.Equal(enums.EventTypeWorkflowExecutionContinuedAsNew, lastEvent.GetEventType())
	attributes := lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enums.ContinueAsNewInitiatorCronSchedule, attributes.GetInitiator())
	s.Equal("cadenceInternal:Timeout TimeoutTypeStartToClose", attributes.GetFailureReason())
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	// check new run contains expected fields
	events = s.getHistory(s.domainName, executions[1])
	firstEvent := events[0]
	s.Equal(enums.EventTypeWorkflowExecutionStarted, firstEvent.GetEventType())
	startAttributes := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startAttributes.Memo)
	s.Equal(searchAttr, startAttributes.SearchAttributes)

	// terminate cron
	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(terminateErr)
}

func (s *integrationSuite) TestSequential_UserTimers() {
	id := "integration-sequential-user-timers-test"
	wt := "integration-sequential-user-timers-test-type"
	tl := "integration-sequential-user-timers-test-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	timerCount := int32(4)
	timerCounter := int32(0)
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if timerCounter < timerCount {
			timerCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
			return []byte(strconv.Itoa(int(timerCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeStartTimer,
				Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
					TimerId:                   fmt.Sprintf("timer-id-%d", timerCounter),
					StartToFireTimeoutSeconds: 1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(timerCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	for i := 0; i < 4; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask: completed")
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestRateLimitBufferedEvents() {
	id := "integration-rate-limit-buffered-events-test"
	wt := "integration-rate-limit-buffered-events-test-type"
	tl := "integration-rate-limit-buffered-events-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	workflowComplete := false
	signalsSent := false
	signalCount := 0
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, h *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}

		if !signalsSent {
			signalsSent = true
			// Buffered Signals
			for i := 0; i < 100; i++ {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, i)
				s.Nil(s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity))
			}

			buf := new(bytes.Buffer)
			binary.Write(buf, binary.LittleEndian, 101)
			signalErr := s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity)
			s.Nil(signalErr)

			// this decision will be ignored as he decision task is already failed
			return nil, []*commonproto.Decision{}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// first decision to send 101 signals, the last signal will force fail decision and flush buffered events.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NotNil(err)
	st := status.Convert(err)
	s.Equal(codes.NotFound, st.Code())
	s.Equal("Decision task not found.", st.Message())

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(101, signalCount) // check that all 101 signals are received.
}

func (s *integrationSuite) TestBufferedEvents() {
	id := "integration-buffered-events-test"
	wt := "integration-buffered-events-test-type"
	tl := "integration-buffered-events-test-tasklist"
	identity := "worker1"
	signalName := "buffered-signal"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	workflowComplete := false
	signalSent := false
	var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !signalSent {
			signalSent = true

			// this will create new event when there is in-flight decision task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Domain: s.domainName,
					WorkflowExecution: &commonproto.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal",
					Input:      []byte("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "1",
					ActivityType:                  &commonproto.ActivityType{Name: "test-activity-type"},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("test-input"),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 && signalEvent == nil {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
				}
			}
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// first decision, which sends signal and the signal event should be buffered to append after first decision closed
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// check history, the signal event should be after the complete decision task
	histResp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain: s.domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	s.NotNil(histResp.History.Events)
	s.True(len(histResp.History.Events) >= 6)
	s.Equal(histResp.History.Events[3].GetEventType(), enums.EventTypeDecisionTaskCompleted)
	s.Equal(histResp.History.Events[4].GetEventType(), enums.EventTypeActivityTaskScheduled)
	s.Equal(histResp.History.Events[5].GetEventType(), enums.EventTypeWorkflowExecutionSignaled)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestDescribeWorkflowExecution() {
	id := "integration-describe-wfe-test"
	wt := "integration-describe-wfe-test-type"
	tl := "integration-describe-wfe-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}
	dweResponse, err := describeWorkflowExecution()
	s.NoError(err)
	s.Equal(int64(0), dweResponse.WorkflowExecutionInfo.CloseTime)
	s.Equal(int64(2), dweResponse.WorkflowExecutionInfo.HistoryLength) // WorkflowStarted, DecisionScheduled
	s.Equal(dweResponse.WorkflowExecutionInfo.GetStartTime(), dweResponse.WorkflowExecutionInfo.GetExecutionTime())

	// decider logic
	workflowComplete := false
	signalSent := false
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "1",
					ActivityType:                  &commonproto.ActivityType{Name: "test-activity-type"},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("test-input"),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// first decision to schedule new activity
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enums.WorkflowExecutionCloseStatusRunning, dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(5), dweResponse.WorkflowExecutionInfo.HistoryLength) // DecisionStarted, DecisionCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.PendingActivities))
	s.Equal("test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
	s.Equal(int64(0), dweResponse.PendingActivities[0].GetLastHeartbeatTimestamp())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enums.WorkflowExecutionCloseStatusRunning, dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(8), dweResponse.WorkflowExecutionInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, DecisionTaskScheduled
	s.Equal(0, len(dweResponse.PendingActivities))

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enums.WorkflowExecutionCloseStatusCompleted, dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(11), dweResponse.WorkflowExecutionInfo.HistoryLength) // DecisionStarted, DecisionCompleted, WorkflowCompleted
}

func (s *integrationSuite) TestVisibility() {
	startTime := time.Now().UnixNano()

	// Start 2 workflow executions
	id1 := "integration-visibility-test1"
	id2 := "integration-visibility-test2"
	wt := "integration-visibility-test-type"
	tl := "integration-visibility-test-tasklist"
	identity := "worker1"

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id1,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	startResponse, err0 := s.engine.StartWorkflowExecution(NewContext(), startRequest)
	s.NoError(err0)

	// Now complete one of the executions
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		return []byte{}, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	_, err1 := poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err1)

	// wait until the start workflow is done
	var nextToken []byte
	historyEventFilterType := enums.HistoryEventFilterTypeCloseEvent
	for {
		historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: startRequest.Domain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: startRequest.WorkflowId,
				RunId:      startResponse.RunId,
			},
			WaitForNewEvent:        true,
			HistoryEventFilterType: historyEventFilterType,
			NextPageToken:          nextToken,
		})
		s.Nil(historyErr)
		if len(historyResponse.NextPageToken) == 0 {
			break
		}

		nextToken = historyResponse.NextPageToken
	}

	startRequest = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id2,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	_, err2 := s.engine.StartWorkflowExecution(NewContext(), startRequest)
	s.NoError(err2)

	startFilter := &commonproto.StartTimeFilter{}
	startFilter.EarliestTime = startTime
	startFilter.LatestTime = time.Now().UnixNano()

	closedCount := 0
	openCount := 0

	var historyLength int64
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
		})
		s.NoError(err3)
		closedCount = len(resp.Executions)
		if closedCount == 1 {
			historyLength = resp.Executions[0].HistoryLength
			break
		}
		s.Logger.Info("Closed WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.Equal(1, closedCount)
	s.Equal(int64(5), historyLength)

	for i := 0; i < 10; i++ {
		resp, err4 := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
		})
		s.NoError(err4)
		openCount = len(resp.Executions)
		if openCount == 1 {
			break
		}
		s.Logger.Info("Open WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.Equal(1, openCount)
}

func (s *integrationSuite) TestChildWorkflowExecution() {
	parentID := "integration-child-workflow-test-parent"
	childID := "integration-child-workflow-test-child"
	wtParent := "integration-child-workflow-test-parent-type"
	wtChild := "integration-child-workflow-test-child-type"
	tlParent := "integration-child-workflow-test-parent-tasklist"
	tlChild := "integration-child-workflow-test-child-tasklist"
	identity := "worker1"

	parentWorkflowType := &commonproto.WorkflowType{}
	parentWorkflowType.Name = wtParent

	childWorkflowType := &commonproto.WorkflowType{}
	childWorkflowType.Name = wtChild

	taskListParent := &commonproto.TaskList{}
	taskListParent.Name = tlParent
	taskListChild := &commonproto.TaskList{}
	taskListChild.Name = tlChild

	header := &commonproto.Header{
		Fields: map[string][]byte{"tracing": []byte("sample payload")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          parentID,
		WorkflowType:                        parentWorkflowType,
		TaskList:                            taskListParent,
		Input:                               nil,
		Header:                              header,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	childComplete := false
	childExecutionStarted := false
	var startedEvent *commonproto.HistoryEvent
	var completedEvent *commonproto.HistoryEvent

	memoInfo, _ := json.Marshal("memo")
	memo := &commonproto.Memo{
		Fields: map[string][]byte{
			"Info": memoInfo,
		},
	}
	attrValBytes, _ := json.Marshal("attrVal")
	searchAttr := &commonproto.SearchAttributes{
		IndexedFields: map[string][]byte{
			"CustomKeywordField": attrValBytes,
		},
	}

	// Parent Decider Logic
	dtHandlerParent := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		s.Logger.Info("Processing decision task for ", tag.WorkflowID(execution.WorkflowId))

		if execution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.Logger.Info("Starting child execution")
				childExecutionStarted = true

				return nil, []*commonproto.Decision{{
					DecisionType: enums.DecisionTypeStartChildWorkflowExecution,
					Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
						WorkflowId:                          childID,
						WorkflowType:                        childWorkflowType,
						TaskList:                            taskListChild,
						Input:                               []byte("child-workflow-input"),
						Header:                              header,
						ExecutionStartToCloseTimeoutSeconds: 200,
						TaskStartToCloseTimeoutSeconds:      2,
						Control:                             nil,
						Memo:                                memo,
						SearchAttributes:                    searchAttr,
					}},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if event.GetEventType() == enums.EventTypeChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*commonproto.Decision{}, nil
					}

					if event.GetEventType() == enums.EventTypeChildWorkflowExecutionCompleted {
						completedEvent = event
						return nil, []*commonproto.Decision{{
							DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
							Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
								Result: []byte("Done"),
							}},
						}}, nil
					}
				}
			}
		}

		return nil, nil, nil
	}

	var childStartedEvent *commonproto.HistoryEvent
	// Child Decider Logic
	dtHandlerChild := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if previousStartedEventID <= 0 {
			childStartedEvent = history.Events[0]
		}

		s.Logger.Info("Processing decision task for Child ", tag.WorkflowID(execution.WorkflowId))
		childComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Child Done"),
			}},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListParent,
		Identity:        identity,
		DecisionHandler: dtHandlerParent,
		Logger:          s.Logger,
		T:               s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListChild,
		Identity:        identity,
		DecisionHandler: dtHandlerChild,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to start child execution
	_, err := pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event and Process Child Execution and complete it
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	_, err = pollerChild.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)
	s.True(childComplete)
	s.NotNil(childStartedEvent)
	s.Equal(enums.EventTypeWorkflowExecutionStarted, childStartedEvent.GetEventType())
	s.Equal(s.domainName, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowDomain())
	s.Equal(parentID, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetRunId())
	s.Equal(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
		childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentInitiatedEventId())
	s.Equal(header, startedEvent.GetChildWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(header, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(memo, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetMemo())
	s.Equal(searchAttr, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes())

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Empty(completedAttributes.Domain)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal([]byte("Child Done"), completedAttributes.Result)
}

func (s *integrationSuite) TestCronChildWorkflowExecution() {
	parentID := "integration-cron-child-workflow-test-parent"
	childID := "integration-cron-child-workflow-test-child"
	wtParent := "integration-cron-child-workflow-test-parent-type"
	wtChild := "integration-cron-child-workflow-test-child-type"
	tlParent := "integration-cron-child-workflow-test-parent-tasklist"
	tlChild := "integration-cron-child-workflow-test-child-tasklist"
	identity := "worker1"

	cronSchedule := "@every 3s"
	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Second

	parentWorkflowType := &commonproto.WorkflowType{Name: wtParent}
	childWorkflowType := &commonproto.WorkflowType{Name: wtChild}

	taskListParent := &commonproto.TaskList{Name: tlParent}
	taskListChild := &commonproto.TaskList{Name: tlChild}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          parentID,
		WorkflowType:                        parentWorkflowType,
		TaskList:                            taskListParent,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	startParentWorkflowTS := time.Now()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	childExecutionStarted := false
	var terminatedEvent *commonproto.HistoryEvent
	var startChildWorkflowTS time.Time
	// Parent Decider Logic
	dtHandlerParent := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		s.Logger.Info("Processing decision task for ", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true
			startChildWorkflowTS = time.Now()
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeStartChildWorkflowExecution,
				Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId:                          childID,
					WorkflowType:                        childWorkflowType,
					TaskList:                            taskListChild,
					Input:                               nil,
					ExecutionStartToCloseTimeoutSeconds: 200,
					TaskStartToCloseTimeoutSeconds:      2,
					Control:                             nil,
					CronSchedule:                        cronSchedule,
				}},
			}}, nil
		}
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enums.EventTypeChildWorkflowExecutionTerminated {
				terminatedEvent = event
				return nil, []*commonproto.Decision{{
					DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
					Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
						Result: []byte("Done"),
					}},
				}}, nil
			}
		}
		return nil, nil, nil
	}

	// Child Decider Logic
	dtHandlerChild := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		s.Logger.Info("Processing decision task for Child ", tag.WorkflowID(execution.WorkflowId))
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes:   &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{}}}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListParent,
		Identity:        identity,
		DecisionHandler: dtHandlerParent,
		Logger:          s.Logger,
		T:               s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListChild,
		Identity:        identity,
		DecisionHandler: dtHandlerChild,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to start child execution
	_, err := pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	startFilter := &commonproto.StartTimeFilter{}
	startFilter.EarliestTime = startChildWorkflowTS.UnixNano()
	for i := 0; i < 2; i++ {
		// Sleep some time before checking the open executions.
		// This will not cost extra time as the polling for first decision task will be blocked for 3 seconds.
		time.Sleep(2 * time.Second)
		startFilter.LatestTime = time.Now().UnixNano()
		resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
				WorkflowId: childID,
			}},
		})
		s.NoError(err)
		s.Equal(1, len(resp.GetExecutions()))

		_, err = pollerChild.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)

		backoffDuration := time.Now().Sub(startChildWorkflowTS)
		s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)
		startChildWorkflowTS = time.Now()
	}

	// terminate the childworkflow
	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: childID,
		},
	})
	s.Nil(terminateErr)

	// Process ChildExecution terminated event and complete parent execution
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(terminatedEvent)
	terminatedAttributes := terminatedEvent.GetChildWorkflowExecutionTerminatedEventAttributes()
	s.Empty(terminatedAttributes.Domain)
	s.Equal(childID, terminatedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, terminatedAttributes.WorkflowType.Name)

	startFilter.EarliestTime = startParentWorkflowTS.UnixNano()
	startFilter.LatestTime = time.Now().UnixNano()
	var closedExecutions []*commonproto.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == 4 {
			closedExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.NotNil(closedExecutions)
	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime() < closedExecutions[j].GetStartTime()
	})
	//The first parent is not the cron workflow, only verify child workflow with cron schedule
	lastExecution := closedExecutions[1]
	for i := 2; i != 4; i++ {
		executionInfo := closedExecutions[i]
		// Round up the time precision to seconds
		expectedBackoff := executionInfo.GetExecutionTime()/1000000000 - lastExecution.GetExecutionTime()/1000000000
		// The execution time calculate based on last execution close time
		// However, the current execution time is based on the current start time
		// This code is to remove the diff between current start time and last execution close time
		// TODO: Remove this line once we unify the time source.
		executionTimeDiff := executionInfo.GetStartTime()/1000000000 - lastExecution.GetCloseTime()/1000000000
		// The backoff between any two executions should be multiplier of the target backoff duration which is 3 in this test
		s.Equal(int64(0), int64(expectedBackoff-executionTimeDiff)/1000000000%(targetBackoffDuration.Nanoseconds()/1000000000))
		lastExecution = executionInfo
	}
}

func (s *integrationSuite) TestWorkflowTimeout() {
	startTime := time.Now().UnixNano()

	id := "integration-workflow-timeout"
	wt := "integration-workflow-timeout-type"
	tl := "integration-workflow-timeout-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false

GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.GetEventType() != enums.EventTypeWorkflowExecutionTimedOut {
			s.Logger.Warn("Execution not timedout yet")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		s.Equal(enums.TimeoutTypeStartToClose, timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)

	startFilter := &commonproto.StartTimeFilter{
		EarliestTime: startTime,
		LatestTime:   time.Now().UnixNano(),
	}

	closedCount := 0
ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
		})
		s.NoError(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.Logger.Info("Closed WorkflowExecution is not yet visibile")
			time.Sleep(1000 * time.Millisecond)
			continue ListClosedLoop
		}
		break ListClosedLoop
	}
	s.Equal(1, closedCount)
}

func (s *integrationSuite) TestDecisionTaskFailed() {
	id := "integration-decisiontask-failed-test"
	wt := "integration-decisiontask-failed-test-type"
	tl := "integration-decisiontask-failed-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	failureCount := 10
	signalCount := 0
	sendSignal := false
	lastDecisionTimestamp := int64(0)
	//var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}
		// Some signals received on this decision
		if signalCount == 1 {
			return nil, []*commonproto.Decision{}, nil
		}

		// Send signals during decision
		if sendSignal {
			s.sendSignal(s.domainName, workflowExecution, "signalC", nil, identity)
			s.sendSignal(s.domainName, workflowExecution, "signalD", nil, identity)
			s.sendSignal(s.domainName, workflowExecution, "signalE", nil, identity)
			sendSignal = false
		}

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if failureCount > 0 {
			// Otherwise decrement failureCount and keep failing decisions
			failureCount--
			return nil, nil, errors.New("Decider Panic")
		}

		workflowComplete = true
		time.Sleep(time.Second)
		s.Logger.Warn(fmt.Sprintf("PrevStarted: %v, StartedEventID: %v, Size: %v", previousStartedEventID, startedEventID,
			len(history.Events)))
		lastDecisionEvent := history.Events[startedEventID-1]
		s.Equal(enums.EventTypeDecisionTaskStarted, lastDecisionEvent.GetEventType())
		lastDecisionTimestamp = lastDecisionEvent.GetTimestamp()
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// process activity
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// fail decision 5 times
	for i := 0; i < 5; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(i))
		s.NoError(err)
	}

	err = s.sendSignal(s.domainName, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// process signal
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, signalCount)

	// send another signal to trigger decision
	err = s.sendSignal(s.domainName, workflowExecution, "signalB", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// fail decision 2 more times
	for i := 0; i < 2; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(i))
		s.NoError(err)
	}
	s.Equal(3, signalCount)

	// now send a signal during failed decision
	sendSignal = true
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(2))
	s.NoError(err)
	s.Equal(4, signalCount)

	// fail decision 1 more times
	for i := 0; i < 2; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(i))
		s.NoError(err)
	}
	s.Equal(12, signalCount)

	// Make complete workflow decision
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(2))
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(16, signalCount)

	events := s.getHistory(s.domainName, workflowExecution)
	var lastEvent *commonproto.HistoryEvent
	var lastDecisionStartedEvent *commonproto.HistoryEvent
	lastIdx := 0
	for i, e := range events {
		if e.GetEventType() == enums.EventTypeDecisionTaskStarted {
			lastDecisionStartedEvent = e
			lastIdx = i
		}
		lastEvent = e
	}
	s.Equal(enums.EventTypeWorkflowExecutionCompleted, lastEvent.GetEventType())
	s.Logger.Info(fmt.Sprintf("Last Decision Time: %v, Last Decision History Timestamp: %v, Complete Timestamp: %v",
		time.Unix(0, lastDecisionTimestamp), time.Unix(0, lastDecisionStartedEvent.GetTimestamp()),
		time.Unix(0, lastEvent.GetTimestamp())))
	s.Equal(lastDecisionTimestamp, lastDecisionStartedEvent.GetTimestamp())
	s.True(time.Duration(lastEvent.GetTimestamp()-lastDecisionTimestamp) >= time.Second)

	s.Equal(2, len(events)-lastIdx-1)
	decisionCompletedEvent := events[lastIdx+1]
	workflowCompletedEvent := events[lastIdx+2]
	s.Equal(enums.EventTypeDecisionTaskCompleted, decisionCompletedEvent.GetEventType())
	s.Equal(enums.EventTypeWorkflowExecutionCompleted, workflowCompletedEvent.GetEventType())
}

func (s *integrationSuite) TestDescribeTaskList() {
	workflowID := "integration-get-poller-history"
	wt := "integration-get-poller-history-type"
	tl := "integration-get-poller-history-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 25,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       25,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// this function poll events from history side
	testDescribeTaskList := func(domain string, tasklist *commonproto.TaskList, tasklistType enums.TaskListType) []*commonproto.PollerInfo {
		responseInner, errInner := s.engine.DescribeTaskList(NewContext(), &workflowservice.DescribeTaskListRequest{
			Domain:       domain,
			TaskList:     tasklist,
			TaskListType: tasklistType,
		})

		s.NoError(errInner)
		return responseInner.Pollers
	}

	before := time.Now()

	// when no one polling on the tasklist (activity or decision), there shall be no poller information
	pollerInfos := testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeActivity)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeDecision)
	s.Empty(pollerInfos)

	_, errDecision := poller.PollAndProcessDecisionTask(false, false)
	s.NoError(errDecision)
	pollerInfos = testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeActivity)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeDecision)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())

	errActivity := poller.PollAndProcessActivityTask(false)
	s.NoError(errActivity)
	pollerInfos = testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeActivity)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
	pollerInfos = testDescribeTaskList(s.domainName, &commonproto.TaskList{Name: tl}, enums.TaskListTypeDecision)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
}

func (s *integrationSuite) TestTransientDecisionTimeout() {
	id := "integration-transient-decision-timeout-test"
	wt := "integration-transient-decision-timeout-test-type"
	tl := "integration-transient-decision-timeout-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      2,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	workflowComplete := false
	failDecision := true
	signalCount := 0
	//var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if failDecision {
			failDecision = false
			return nil, nil, errors.New("Decider Panic")
		}

		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// First decision immediately fails and schedules a transient decision
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Now send a signal when transient decision is scheduled
	err = s.sendSignal(s.domainName, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// Drop decision task to cause a Decision Timeout
	_, err = poller.PollAndProcessDecisionTask(true, true)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Now process signal and complete workflow execution
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.Equal(1, signalCount)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestNoTransientDecisionAfterFlushBufferedEvents() {
	id := "integration-no-transient-decision-after-flush-buffered-events-test"
	wt := "integration-no-transient-decision-after-flush-buffered-events-test-type"
	tl := "integration-no-transient-decision-after-flush-buffered-events-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      20,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	workflowComplete := false
	continueAsNewAndSignal := false
	dtHandler := func(execution *commonproto.WorkflowExecution, workflowType *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !continueAsNewAndSignal {
			continueAsNewAndSignal = true
			// this will create new event when there is in-flight decision task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Domain: s.domainName,
					WorkflowExecution: &commonproto.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal-1",
					Input:      []byte("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeContinueAsNewWorkflowExecution,
				Attributes: &commonproto.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            &commonproto.TaskList{Name: tl},
					Input:                               nil,
					ExecutionStartToCloseTimeoutSeconds: 1000,
					TaskStartToCloseTimeoutSeconds:      100,
				}},
			}}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// fist decision, this try to do a continue as new but there is a buffered event,
	// so it will fail and create a new decision
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// second decision, which will complete the workflow
	// this expect the decision to have attempt == 0
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, 0)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}

func (s *integrationSuite) TestRelayDecisionTimeout() {
	id := "integration-relay-decision-timeout-test"
	wt := "integration-relay-decision-timeout-test-type"
	tl := "integration-relay-decision-timeout-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      2,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	workflowComplete, isFirst := false, true
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if isFirst {
			isFirst = false
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "test-marker",
				}},
			}}, nil
		}
		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes:   &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{}}}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// First decision task complete with a marker decision, and request to relay decision (immediately return a new decision task)
	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		false,
		false,
		false,
		false,
		0,
		3,
		true,
		nil)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)

	time.Sleep(time.Second * 2) // wait 2s for relay decision to timeout
	decisionTaskTimeout := false
	for i := 0; i < 3; i++ {
		events := s.getHistory(s.domainName, workflowExecution)
		if len(events) >= 8 {
			s.Equal(enums.EventTypeDecisionTaskTimedOut, events[7].GetEventType())
			s.Equal(enums.TimeoutTypeStartToClose, events[7].GetDecisionTaskTimedOutEventAttributes().GetTimeoutType())
			decisionTaskTimeout = true
			break
		}
		time.Sleep(time.Second)
	}
	// verify relay decision task timeout
	s.True(decisionTaskTimeout)

	// Now complete workflow
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}

func (s *integrationSuite) TestTaskProcessingProtectionForRateLimitError() {
	id := "integration-task-processing-protection-for-rate-limit-error-test"
	wt := "integration-task-processing-protection-for-rate-limit-error-test-type"
	tl := "integration-task-processing-protection-for-rate-limit-error-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 601,
		TaskStartToCloseTimeoutSeconds:      600,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	workflowComplete := false
	signalCount := 0
	createUserTimer := false
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, h *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !createUserTimer {
			createUserTimer = true

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeStartTimer,
				Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
					TimerId:                   "timer-id-1",
					StartToFireTimeoutSeconds: 5,
				}},
			}}, nil
		}

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Process first decision to create user timer
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Send one signal to create a new decision
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, 0)
	s.Nil(s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity))

	// Drop decision to cause all events to be buffered from now on
	_, err = poller.PollAndProcessDecisionTask(false, true)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Buffered 100 Signals
	for i := 1; i < 101; i++ {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		s.Nil(s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity))
	}

	// 101 signal, which will fail the decision
	buf = new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, 101)
	signalErr := s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity)
	s.Nil(signalErr)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, 0)
	s.Logger.Info("pollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(102, signalCount)
}

func (s *integrationSuite) TestStickyTimeout_NonTransientDecision() {
	id := "integration-sticky-timeout-non-transient-decision"
	wt := "integration-sticky-timeout-non-transient-decision-type"
	tl := "integration-sticky-timeout-non-transient-decision-tasklist"
	stl := "integration-sticky-timeout-non-transient-decision-tasklist-sticky"
	identity := "worker1"

	stickyTaskList := &commonproto.TaskList{}
	stickyTaskList.Name = stl
	stickyScheduleToStartTimeoutSeconds := 2

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	localActivityDone := false
	failureCount := 5
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !localActivityDone {
			localActivityDone = true

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "local activity marker",
					Details:    []byte("local activity data"),
				}},
			}}, nil
		}

		if failureCount > 0 {
			// send a signal on third failure to be buffered, forcing a non-transient decision when buffer is flushed
			/*if failureCount == 3 {
				err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
					Domain:            s.domainName,
					WorkflowExecution: workflowExecution,
					SignalName:        "signalB",
					Input:             []byte("signal input"),
					Identity:          identity,
					RequestId:         uuid.New(),
				})
				s.NoError(err)
			}*/
			failureCount--
			return nil, nil, errors.New("non deterministic error")
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                              s.engine,
		Domain:                              s.domainName,
		TaskList:                            &commonproto.TaskList{Name: tl},
		Identity:                            identity,
		DecisionHandler:                     dtHandler,
		Logger:                              s.Logger,
		T:                                   s.T(),
		StickyTaskList:                      stickyTaskList,
		StickyScheduleToStartTimeoutSeconds: int32(stickyScheduleToStartTimeoutSeconds),
	}

	_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, true, int64(0))
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             []byte("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})

	// Wait for decision timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.domainName, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enums.EventTypeDecisionTaskTimedOut {
				s.Equal(enums.TimeoutTypeScheduleToStart, event.GetDecisionTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Decision not timed out")

	for i := 0; i < 3; i++ {
		_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(i))
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             []byte("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 0; i < 2; i++ {
		_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(i))
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)
	}

	decisionTaskFailed := false
	events := s.getHistory(s.domainName, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enums.EventTypeDecisionTaskFailed {
			decisionTaskFailed = true
			break
		}
	}
	s.True(decisionTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(2))

	// Assert for single decision task failed and workflow completion
	failedDecisions := 0
	workflowComplete := false
	events = s.getHistory(s.domainName, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enums.EventTypeDecisionTaskFailed:
			failedDecisions++
		case enums.EventTypeWorkflowExecutionCompleted:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedDecisions, "Mismatched failed decision count")
}

func (s *integrationSuite) TestStickyTasklistResetThenTimeout() {
	id := "integration-reset-sticky-fire-schedule-to-start-timeout"
	wt := "integration-reset-sticky-fire-schedule-to-start-timeout-type"
	tl := "integration-reset-sticky-fire-schedule-to-start-timeout-tasklist"
	stl := "integration-reset-sticky-fire-schedule-to-start-timeout-tasklist-sticky"
	identity := "worker1"

	stickyTaskList := &commonproto.TaskList{}
	stickyTaskList.Name = stl
	stickyScheduleToStartTimeoutSeconds := 2

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	localActivityDone := false
	failureCount := 5
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !localActivityDone {
			localActivityDone = true

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "local activity marker",
					Details:    []byte("local activity data"),
				}},
			}}, nil
		}

		if failureCount > 0 {
			failureCount--
			return nil, nil, errors.New("non deterministic error")
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                              s.engine,
		Domain:                              s.domainName,
		TaskList:                            &commonproto.TaskList{Name: tl},
		Identity:                            identity,
		DecisionHandler:                     dtHandler,
		Logger:                              s.Logger,
		T:                                   s.T(),
		StickyTaskList:                      stickyTaskList,
		StickyScheduleToStartTimeoutSeconds: int32(stickyScheduleToStartTimeoutSeconds),
	}

	_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, true, int64(0))
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             []byte("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})

	//Reset sticky tasklist before sticky decision task starts
	s.engine.ResetStickyTaskList(NewContext(), &workflowservice.ResetStickyTaskListRequest{
		Domain:    s.domainName,
		Execution: workflowExecution,
	})

	// Wait for decision timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.domainName, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enums.EventTypeDecisionTaskTimedOut {
				s.Equal(enums.TimeoutTypeScheduleToStart, event.GetDecisionTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Decision not timed out")

	for i := 0; i < 3; i++ {
		_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(i))
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             []byte("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 0; i < 2; i++ {
		_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(i))
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)
	}

	decisionTaskFailed := false
	events := s.getHistory(s.domainName, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enums.EventTypeDecisionTaskFailed {
			decisionTaskFailed = true
			break
		}
	}
	s.True(decisionTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, true, int64(2))

	// Assert for single decision task failed and workflow completion
	failedDecisions := 0
	workflowComplete := false
	events = s.getHistory(s.domainName, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enums.EventTypeDecisionTaskFailed:
			failedDecisions++
		case enums.EventTypeWorkflowExecutionCompleted:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedDecisions, "Mismatched failed decision count")
}

func (s *integrationSuite) TestBufferedEventsOutOfOrder() {
	id := "integration-buffered-events-out-of-order-test"
	wt := "integration-buffered-events-out-of-order-test-type"
	tl := "integration-buffered-events-out-of-order-test-tasklist"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      20,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// decider logic
	workflowComplete := false
	firstDecision := false
	secondDecision := false
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		s.Logger.Info(fmt.Sprintf("Decider called: first: %v, second: %v, complete: %v\n", firstDecision, secondDecision, workflowComplete))

		if !firstDecision {
			firstDecision = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "some random marker name",
					Details:    []byte("some random marker details"),
				}},
			}, {
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    "Activity-1",
					ActivityType:                  &commonproto.ActivityType{Name: "ActivityType"},
					Domain:                        s.domainName,
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         []byte("some random activity input"),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 100,
					StartToCloseTimeoutSeconds:    100,
					HeartbeatTimeoutSeconds:       100,
				}},
			}}, nil
		}

		if !secondDecision {
			secondDecision = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "some random marker name",
					Details:    []byte("some random marker details"),
				}},
			}}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}
	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// first decision, which will schedule an activity and add marker
	_, task, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(
		true,
		false,
		false,
		false,
		int64(0),
		1,
		true,
		nil)
	s.Logger.Info("pollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// This will cause activity start and complete to be buffered
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("pollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// second decision, completes another local activity and forces flush of buffered activity events
	newDecisionTask := task.GetDecisionTask()
	s.NotNil(newDecisionTask)
	task, err = poller.HandlePartialDecision(newDecisionTask)
	s.Logger.Info("pollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(task)

	// third decision, which will close workflow
	newDecisionTask = task.GetDecisionTask()
	s.NotNil(newDecisionTask)
	task, err = poller.HandlePartialDecision(newDecisionTask)
	s.Logger.Info("pollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.Nil(task.DecisionTask)

	events := s.getHistory(s.domainName, workflowExecution)
	var scheduleEvent, startedEvent, completedEvent *commonproto.HistoryEvent
	for _, event := range events {
		switch event.GetEventType() {
		case enums.EventTypeActivityTaskScheduled:
			scheduleEvent = event
		case enums.EventTypeActivityTaskStarted:
			startedEvent = event
		case enums.EventTypeActivityTaskCompleted:
			completedEvent = event
		}
	}

	s.NotNil(scheduleEvent)
	s.NotNil(startedEvent)
	s.NotNil(completedEvent)
	s.True(startedEvent.GetEventId() < completedEvent.GetEventId())
	s.Equal(scheduleEvent.GetEventId(), startedEvent.GetActivityTaskStartedEventAttributes().GetScheduledEventId())
	s.Equal(scheduleEvent.GetEventId(), completedEvent.GetActivityTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(startedEvent.GetEventId(), completedEvent.GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	s.True(workflowComplete)
}

type RunIdGetter interface {
	GetRunId() string
}
type startFunc func() (RunIdGetter, error)

func (s *integrationSuite) TestStartWithMemo() {
	id := "integration-start-with-memo-test"
	wt := "integration-start-with-memo-test-type"
	tl := "integration-start-with-memo-test-tasklist"
	identity := "worker1"

	memoInfo, _ := json.Marshal(id)
	memo := &commonproto.Memo{
		Fields: map[string][]byte{
			"Info": memoInfo,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
		Memo:                                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.StartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &commonproto.TaskList{Name: tl}, memo)
}

func (s *integrationSuite) TestSignalWithStartWithMemo() {
	id := "integration-signal-with-start-with-memo-test"
	wt := "integration-signal-with-start-with-memo-test-type"
	tl := "integration-signal-with-start-with-memo-test-tasklist"
	identity := "worker1"

	memoInfo, _ := json.Marshal(id)
	memo := &commonproto.Memo{
		Fields: map[string][]byte{
			"Info": memoInfo,
		},
	}

	signalName := "my signal"
	signalInput := []byte("my signal input")
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		SignalName:                          signalName,
		SignalInput:                         signalInput,
		Identity:                            identity,
		Memo:                                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.SignalWithStartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &commonproto.TaskList{Name: tl}, memo)
}

func (s *integrationSuite) TestCancelTimer() {
	id := "integration-cancel-timer-test"
	wt := "integration-cancel-timer-test-type"
	tl := "integration-cancel-timer-test-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1000,
		Identity:                            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := int64(2000)
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !timerScheduled {
			timerScheduled = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeStartTimer,
				Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
					TimerId:                   fmt.Sprintf("%v", timerID),
					StartToFireTimeoutSeconds: timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain:          s.domainName,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enums.EventTypeWorkflowExecutionSignaled:
				signalDelivered = true
			case enums.EventTypeTimerCanceled:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCancelTimer,
				Attributes: &commonproto.Decision_CancelTimerDecisionAttributes{CancelTimerDecisionAttributes: &commonproto.CancelTimerDecisionAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
				}},
			}}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.domainName, workflowExecution, "random signal name", []byte("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.domainName, workflowExecution, "random signal name", []byte("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:          s.domainName,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enums.EventTypeWorkflowExecutionSignaled:
			signalDelivered = true
		case enums.EventTypeTimerCanceled:
			timerCancelled = true
		case enums.EventTypeTimerFired:
			s.Fail("timer got fired")
		}
	}
}

func (s *integrationSuite) TestCancelTimer_CancelFiredAndBuffered() {
	id := "integration-cancel-timer-fired-and-buffered-test"
	wt := "integration-cancel-timer-fired-and-buffered-test-type"
	tl := "integration-cancel-timer-fired-and-buffered-test-tasklist"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        &commonproto.WorkflowType{Name: wt},
		TaskList:                            &commonproto.TaskList{Name: tl},
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1000,
		Identity:                            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := int64(4)
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !timerScheduled {
			timerScheduled = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeStartTimer,
				Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
					TimerId:                   fmt.Sprintf("%v", timerID),
					StartToFireTimeoutSeconds: timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain:          s.domainName,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enums.EventTypeWorkflowExecutionSignaled:
				signalDelivered = true
			case enums.EventTypeTimerCanceled:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			time.Sleep(time.Duration(2*timer) * time.Second)
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCancelTimer,
				Attributes: &commonproto.Decision_CancelTimerDecisionAttributes{CancelTimerDecisionAttributes: &commonproto.CancelTimerDecisionAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
				}},
			}}, nil
		}

		workflowComplete = true
		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        &commonproto.TaskList{Name: tl},
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.domainName, workflowExecution, "random signal name", []byte("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.domainName, workflowExecution, "random signal name", []byte("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:          s.domainName,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enums.EventTypeWorkflowExecutionSignaled:
			signalDelivered = true
		case enums.EventTypeTimerCanceled:
			timerCancelled = true
		case enums.EventTypeTimerFired:
			s.Fail("timer got fired")
		}
	}
}

// helper function for TestStartWithMemo and TestSignalWithStartWithMemo to reduce duplicate code
func (s *integrationSuite) startWithMemoHelper(startFn startFunc, id string, taskList *commonproto.TaskList, memo *commonproto.Memo) {
	identity := "worker1"

	we, err0 := startFn()
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	dtHandler := func(execution *commonproto.WorkflowExecution, workflowType *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		return []byte(strconv.Itoa(1)), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// verify open visibility
	var openExecutionInfo *commonproto.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: &commonproto.StartTimeFilter{
				EarliestTime: 0,
				LatestTime:   time.Now().UnixNano(),
			},
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err1)
		if len(resp.Executions) == 1 {
			openExecutionInfo = resp.Executions[0]
			break
		}
		s.Logger.Info("Open WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.NotNil(openExecutionInfo)
	s.Equal(memo, openExecutionInfo.Memo)

	// make progress of workflow
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// verify history
	execution := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}
	historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:    s.domainName,
		Execution: execution,
	})
	s.Nil(historyErr)
	history := historyResponse.History
	firstEvent := history.Events[0]
	s.Equal(enums.EventTypeWorkflowExecutionStarted, firstEvent.GetEventType())
	startdEventAttributes := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startdEventAttributes.Memo)

	// verify DescribeWorkflowExecution result
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Domain:    s.domainName,
		Execution: execution,
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	s.Equal(memo, descResp.WorkflowExecutionInfo.Memo)

	// verify closed visibility
	var closdExecutionInfo *commonproto.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Domain:          s.domainName,
			MaximumPageSize: 100,
			StartTimeFilter: &commonproto.StartTimeFilter{
				EarliestTime: 0,
				LatestTime:   time.Now().UnixNano(),
			},
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err1)
		if len(resp.Executions) == 1 {
			closdExecutionInfo = resp.Executions[0]
			break
		}
		s.Logger.Info("Closed WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.NotNil(closdExecutionInfo)
	s.Equal(memo, closdExecutionInfo.Memo)
}

func (s *integrationSuite) sendSignal(domainName string, execution *commonproto.WorkflowExecution, signalName string,
	input []byte, identity string) error {
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            domainName,
		WorkflowExecution: execution,
		SignalName:        signalName,
		Input:             input,
		Identity:          identity,
	})

	return err
}
