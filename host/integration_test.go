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
	"flag"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/matching"
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
	tl := "integration-start-workflow-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tl},
		Input:              nil,
		WorkflowRunTimeout: timestamp.DurationPtr(100 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}
	we2, err2 := s.engine.StartWorkflowExecution(NewContext(), newRequest)
	s.NotNil(err2)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err2)
	log.Errorf("Unable to start workflow execution: %v", err2)
	s.Nil(we2)
}

func (s *integrationSuite) TestTerminateWorkflow() {
	id := "integration-terminate-workflow-test"
	wt := "integration-terminate-workflow-test-type"
	tl := "integration-terminate-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
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

	_, err := poller.PollAndProcessWorkflowTask(false, false)
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
			RequestId:           uuid.New(),
			Namespace:           s.namespace,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: wt},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
			Input:               nil,
			WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
			WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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

func (s *integrationSuite) TestSequentialWorkflow() {
	id := "integration-sequential-workflow-test"
	wt := "integration-sequential-workflow-test-type"
	tl := "integration-sequential-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
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
		var b []byte
		err := payloads.Decode(input, &b)
		s.NoError(err)
		buf := bytes.NewReader(b)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++

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

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessWorkflowTask(false, false)
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
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestCompleteWorkflowTaskAndCreateNewOne() {
	id := "integration-complete-workflow-task-create-new-test"
	wt := "integration-complete-workflow-task-create-new-test-type"
	tl := "integration-complete-workflow-task-create-new-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		StickyTaskQueue:     &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, newTask, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		1,
		true,
		nil)
	s.NoError(err)
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

func (s *integrationSuite) TestWorkflowTaskAndActivityTaskTimeoutsWorkflow() {
	id := "integration-timeouts-workflow-test"
	wt := "integration-timeouts-workflow-test-type"
	tl := "integration-timeouts-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(1 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(1 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(1 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
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
			_, err = poller.PollAndProcessWorkflowTask(true, true)
		} else {
			_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 2)
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
			common.PrettyPrintHistory(history, s.Logger)
		}
		s.True(err == nil || err == matching.ErrNoTasks, "%v", err)
		if !dropWorkflowTask {
			s.Logger.Info("Calling PollAndProcessActivityTask", tag.Counter(i))
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || err == matching.ErrNoTasks)
		}
	}

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestWorkflowRetry() {
	id := "integration-wf-retry-test"
	wt := "integration-wf-retry-type"
	tl := "integration-wf-retry-taskqueue"
	identity := "worker1"

	initialInterval := 1 * time.Second
	backoffCoefficient := 1.5
	maximumAttempts := 5
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			// Intentionally test server-initialization of Initial Interval value (which should be 1 second)
			MaximumAttempts:        int32(maximumAttempts),
			MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
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
		_, err := poller.PollAndProcessWorkflowTask(false, false)
		s.True(err == nil, err)
		events := s.getHistory(s.namespace, executions[i-1])
		if i == maximumAttempts {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[len(events)-1].GetEventType())
		} else {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[len(events)-1].GetEventType())
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
		expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().Add(backoff)
		s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))
	}
}

func (s *integrationSuite) TestWorkflowRetryFailures() {
	id := "integration-wf-retry-failures-test"
	wt := "integration-wf-retry-failures-type"
	tl := "integration-wf-retry-failures-taskqueue"
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)
	events := s.getHistory(s.namespace, executions[0])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.namespace, executions[1])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[len(events)-1].GetEventType())
	s.Equal(int32(2), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.namespace, executions[2])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(3), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// Fail error reason
	request = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)
	events = s.getHistory(s.namespace, executions[0])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())
}

func (s *integrationSuite) TestCronWorkflow() {
	s.T().Skip(`
	    integration_test.go:1034:
	        	Error Trace:	integration_test.go:1034
	        	Error:      	Not equal:
	        	            	expected: 0
	        	            	actual  : 2
	        	Test:       	TestIntegrationSuite/TestCronWorkflow
	        	Messages:   	exected backof 2-0 should be multiplier of target backoff 3
	`)

	id := "integration-wf-cron-test"
	wt := "integration-wf-cron-type"
	tl := "integration-wf-cron-taskqueue"
	identity := "worker1"
	cronSchedule := "@every 3s"

	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Millisecond * 500

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{"memoKey": payload.EncodeString("memoVal")},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": payload.EncodeString(`"1"`),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		CronSchedule:        cronSchedule, // minimum interval by standard spec is 1m (* * * * *, use non-standard descriptor for short interval for test
		Memo:                memo,
		SearchAttributes:    searchAttr,
	}

	startWorkflowTS := time.Now().UTC()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	attemptCount := 1

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		executions = append(executions, execution)
		attemptCount++
		if attemptCount == 3 {
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("cron-test-result"),
					}},
				}}, nil
		}
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("cron-test-error", false),
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startWorkflowTS
	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())

	// Sleep some time before checking the open executions.
	// This will not cost extra time as the polling for first workflow task will be blocked for 3 seconds.
	time.Sleep(2 * time.Second)
	resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: startFilter,
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	})
	s.NoError(err)
	s.Equal(1, len(resp.GetExecutions()))
	executionInfo := resp.GetExecutions()[0]
	s.Equal(targetBackoffDuration, executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(executionInfo.GetStartTime())))

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)

	// Make sure the cron workflow start running at a proper time, in this case 3 seconds after the
	// startWorkflowExecution request
	backoffDuration := time.Now().UTC().Sub(startWorkflowTS)
	s.True(backoffDuration > targetBackoffDuration)
	s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)

	s.Equal(4, attemptCount)

	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(terminateErr)
	events := s.getHistory(s.namespace, executions[0])
	lastEvent := events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, lastEvent.GetEventType())
	attributes := lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attributes.GetInitiator())
	s.Equal("cron-test-error", attributes.GetFailure().GetMessage())
	s.Nil(attributes.GetLastCompletionResult())
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	events = s.getHistory(s.namespace, executions[1])
	lastEvent = events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, lastEvent.GetEventType())
	attributes = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attributes.GetInitiator())
	s.Equal("", attributes.GetFailure().GetMessage())

	var r string
	err = payloads.Decode(attributes.GetLastCompletionResult(), &r)
	s.NoError(err)
	s.Equal("cron-test-result", r)
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	events = s.getHistory(s.namespace, executions[2])
	lastEvent = events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, lastEvent.GetEventType())
	attributes = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attributes.GetInitiator())
	s.Equal("cron-test-error", attributes.GetFailure().GetMessage())

	err = payloads.Decode(attributes.GetLastCompletionResult(), &r)
	s.NoError(err)
	s.Equal("cron-test-result", r)
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
	var closedExecutions []*workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
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
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().Add(3 * time.Second)
	s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))

	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime().Before(timestamp.TimeValue(closedExecutions[j].GetStartTime()))
	})
	lastExecution := closedExecutions[0]
	for i := 1; i != 4; i++ {
		executionInfo := closedExecutions[i]
		expectedBackoff := executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(lastExecution.GetExecutionTime()))
		// The execution time calculate based on last execution close time
		// However, the current execution time is based on the current start time
		// This code is to remove the diff between current start time and last execution close time
		// TODO: Remove this line once we unify the time source
		executionTimeDiff := executionInfo.GetStartTime().Sub(timestamp.TimeValue(lastExecution.GetCloseTime()))
		// The backoff between any two executions should be multiplier of the target backoff duration which is 3 in this test
		s.Equal(
			0,
			int((expectedBackoff-executionTimeDiff).Round(time.Second).Seconds())%int(targetBackoffDuration.Seconds()),
			"exected backoff %v-%v=%v should be multiplier of target backoff %v",
			expectedBackoff.Seconds(),
			executionTimeDiff.Seconds(),
			(expectedBackoff - executionTimeDiff).Round(time.Second).Seconds(),
			targetBackoffDuration.Seconds())
		lastExecution = executionInfo
	}
}

func (s *integrationSuite) TestCronWorkflowTimeout() {
	id := "integration-wf-cron-timeout-test"
	wt := "integration-wf-cron-timeout-type"
	tl := "integration-wf-cron-timeout-taskqueue"
	identity := "worker1"
	cronSchedule := "@every 3s"

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"memoKey": payload.EncodeString("memoVal"),
		},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": payload.EncodeString(`"1"`),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(1 * time.Second), // set workflow timeout to 1s
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		CronSchedule:        cronSchedule, // minimum interval by standard spec is 1m (* * * * *), use non-standard descriptor for short interval for test
		Memo:                memo,
		SearchAttributes:    searchAttr,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, h *historypb.History) ([]*commandpb.Command, error) {

		executions = append(executions, execution)
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,

				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-id",
					StartToFireTimeout: timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)

	time.Sleep(1 * time.Second) // wait for workflow timeout

	// check when workflow timeout, continueAsNew event contains expected fields
	events := s.getHistory(s.namespace, executions[0])
	lastEvent := events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, lastEvent.GetEventType())
	attributes := lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attributes.GetInitiator())
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, attributes.GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
	s.Equal(memo, attributes.Memo)
	s.Equal(searchAttr, attributes.SearchAttributes)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.True(err == nil, err)

	// check new run contains expected fields
	events = s.getHistory(s.namespace, executions[1])
	firstEvent := events[0]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, firstEvent.GetEventType())
	startAttributes := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startAttributes.Memo)
	s.Equal(searchAttr, startAttributes.SearchAttributes)

	// terminate cron
	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(terminateErr)
}

func (s *integrationSuite) TestSequential_UserTimers() {
	id := "integration-sequential-user-timers-test"
	wt := "integration-sequential-user-timers-test-type"
	tl := "integration-sequential-user-timers-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	timerCount := int32(4)
	timerCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if timerCounter < timerCount {
			timerCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("timer-id-%d", timerCounter),
					StartToFireTimeout: timestamp.DurationPtr(1 * time.Second),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 4; i++ {
		_, err := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask: completed")
		s.NoError(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestRateLimitBufferedEvents() {
	id := "integration-rate-limit-buffered-events-test"
	wt := "integration-rate-limit-buffered-events-test-type"
	tl := "integration-rate-limit-buffered-events-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	signalsSent := false
	signalCount := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, h *historypb.History) ([]*commandpb.Command, error) {

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}

		if !signalsSent {
			signalsSent = true
			// Buffered Signals
			for i := 0; i < 100; i++ {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, i)
				s.Nil(s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity))
			}

			buf := new(bytes.Buffer)
			binary.Write(buf, binary.LittleEndian, 101)
			signalErr := s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity)
			s.Nil(signalErr)

			// this command will be ignored as he workflow task is already failed
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task to send 101 signals, the last signal will force fail workflow task and flush buffered events.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Equal("Workflow task not found.", err.Error())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(101, signalCount) // check that all 101 signals are received.
}

func (s *integrationSuite) TestBufferedEvents() {
	id := "integration-buffered-events-test"
	wt := "integration-buffered-events-test-type"
	tl := "integration-buffered-events-test-taskqueue"
	identity := "worker1"
	signalName := "buffered-signal"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	signalSent := false
	var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 && signalEvent == nil {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task, which sends signal and the signal event should be buffered to append after first workflow task closed
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// check history, the signal event should be after the complete workflow task
	histResp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	s.NotNil(histResp.History.Events)
	s.True(len(histResp.History.Events) >= 6)
	s.Equal(histResp.History.Events[3].GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
	s.Equal(histResp.History.Events[4].GetEventType(), enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	s.Equal(histResp.History.Events[5].GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestDescribeWorkflowExecution() {
	id := "integration-describe-wfe-test"
	wt := "integration-describe-wfe-test-type"
	tl := "integration-describe-wfe-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	s.Nil(dweResponse.WorkflowExecutionInfo.CloseTime)
	s.Equal(int64(2), dweResponse.WorkflowExecutionInfo.HistoryLength) // WorkflowStarted, WorkflowTaskScheduled
	s.Equal(dweResponse.WorkflowExecutionInfo.GetStartTime(), dweResponse.WorkflowExecutionInfo.GetExecutionTime())

	// workflow logic
	workflowComplete := false
	signalSent := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
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

	// first workflow task to schedule new activity
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, dweResponse.WorkflowExecutionInfo.GetStatus())
	s.Equal(int64(5), dweResponse.WorkflowExecutionInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.PendingActivities))
	s.Equal("test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
	s.True(timestamp.TimeValue(dweResponse.PendingActivities[0].GetLastHeartbeatTime()).IsZero())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, dweResponse.WorkflowExecutionInfo.GetStatus())
	s.Equal(int64(8), dweResponse.WorkflowExecutionInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, WorkflowTaskScheduled
	s.Equal(0, len(dweResponse.PendingActivities))

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, dweResponse.WorkflowExecutionInfo.GetStatus())
	s.Equal(int64(11), dweResponse.WorkflowExecutionInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowCompleted
}

func (s *integrationSuite) TestVisibility() {
	startTime := time.Now().UTC()

	// Start 2 workflow executions
	id1 := "integration-visibility-test1"
	id2 := "integration-visibility-test2"
	wt := "integration-visibility-test-type"
	tl := "integration-visibility-test-taskqueue"
	identity := "worker1"

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id1,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
		Identity:            identity,
	}

	startResponse, err0 := s.engine.StartWorkflowExecution(NewContext(), startRequest)
	s.NoError(err0)

	// Now complete one of the executions
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err1 := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err1)

	// wait until the start workflow is done
	var nextToken []byte
	historyEventFilterType := enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	for {
		historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: startRequest.Namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: startRequest.WorkflowId,
				RunId:      startResponse.RunId,
			},
			WaitNewEvent:           true,
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
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id2,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
		Identity:            identity,
	}

	_, err2 := s.engine.StartWorkflowExecution(NewContext(), startRequest)
	s.NoError(err2)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startTime
	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())

	closedCount := 0
	openCount := 0

	var historyLength int64
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
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
			Namespace:       s.namespace,
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
	tlParent := "integration-child-workflow-test-parent-taskqueue"
	tlChild := "integration-child-workflow-test-child-taskqueue"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{}
	parentWorkflowType.Name = wtParent

	childWorkflowType := &commonpb.WorkflowType{}
	childWorkflowType.Name = wtChild

	taskQueueParent := &taskqueuepb.TaskQueue{}
	taskQueueParent.Name = tlParent
	taskQueueChild := &taskqueuepb.TaskQueue{}
	taskQueueChild.Name = tlChild

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample payload")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childComplete := false
	childExecutionStarted := false
	var startedEvent *historypb.HistoryEvent
	var completedEvent *historypb.HistoryEvent

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString("memo"),
		},
	}
	attrValPayload := payload.EncodeString("attrVal")
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": attrValPayload,
		},
	}

	// Parent workflow logic
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if execution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.Logger.Info("Starting child execution")
				childExecutionStarted = true

				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childID,
						WorkflowType:        childWorkflowType,
						TaskQueue:           taskQueueChild,
						Input:               payloads.EncodeString("child-workflow-input"),
						Header:              header,
						WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
						WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
						Control:             "",
						Memo:                memo,
						SearchAttributes:    searchAttr,
					}},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
						startedEvent = event
						return []*commandpb.Command{}, nil
					}

					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
						completedEvent = event
						return []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
							Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("Done"),
							}},
						}}, nil
					}
				}
			}
		}

		return nil, nil
	}

	var childStartedEvent *historypb.HistoryEvent
	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if previousStartedEventID <= 0 {
			childStartedEvent = history.Events[0]
		}

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId))
		childComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Child Done"),
			}},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event and Process Child Execution and complete it
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)
	s.True(childComplete)
	s.NotNil(childStartedEvent)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	s.Equal(s.namespace, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetRunId())
	s.Equal(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
		childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentInitiatedEventId())
	s.Equal(header, startedEvent.GetChildWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(header, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(memo, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetMemo())
	s.Equal(searchAttr, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes())
	s.Equal(315360000*time.Second, timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowExecutionTimeout()))
	s.Equal(200*time.Second, timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout()))

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Empty(completedAttributes.Namespace)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	var r string
	err = payloads.Decode(completedAttributes.GetResult(), &r)
	s.NoError(err)
	s.Equal("Child Done", r)
}

func (s *integrationSuite) TestCronChildWorkflowExecution() {
	s.T().Skip(`
	    integration_test.go:2046:
	        	Error Trace:	integration_test.go:2046
	        	Error:      	Expected value not to be nil.
	        	Test:       	TestIntegrationSuite/TestCronChildWorkflowExecution
	`)

	parentID := "integration-cron-child-workflow-test-parent"
	childID := "integration-cron-child-workflow-test-child"
	wtParent := "integration-cron-child-workflow-test-parent-type"
	wtChild := "integration-cron-child-workflow-test-child-type"
	tlParent := "integration-cron-child-workflow-test-parent-taskqueue"
	tlChild := "integration-cron-child-workflow-test-child-taskqueue"
	identity := "worker1"

	cronSchedule := "@every 3s"
	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Second

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}

	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	startParentWorkflowTS := time.Now().UTC()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childExecutionStarted := false
	var terminatedEvent *historypb.HistoryEvent
	var startChildWorkflowTS time.Time
	// Parent workflow logic
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true
			startChildWorkflowTS = time.Now().UTC()
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId:          childID,
					WorkflowType:        childWorkflowType,
					TaskQueue:           taskQueueChild,
					Input:               nil,
					WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
					Control:             "",
					CronSchedule:        cronSchedule,
				}},
			}}, nil
		}
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED {
				terminatedEvent = event
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Done"),
					}},
				}}, nil
			}
		}
		return nil, nil
	}

	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId))
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startChildWorkflowTS
	for i := 0; i < 2; i++ {
		// Sleep some time before checking the open executions.
		// This will not cost extra time as the polling for first workflow task will be blocked for 3 seconds.
		time.Sleep(2 * time.Second)
		startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
		resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: childID,
			}},
		})
		s.NoError(err)
		s.Equal(1, len(resp.GetExecutions()))

		_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)

		backoffDuration := time.Now().UTC().Sub(startChildWorkflowTS)
		s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)
		startChildWorkflowTS = time.Now().UTC()
	}

	// terminate the childworkflow
	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childID,
		},
	})
	s.Nil(terminateErr)

	// Process ChildExecution terminated event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(terminatedEvent)
	terminatedAttributes := terminatedEvent.GetChildWorkflowExecutionTerminatedEventAttributes()
	s.Empty(terminatedAttributes.Namespace)
	s.Equal(childID, terminatedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, terminatedAttributes.WorkflowType.Name)

	startFilter.EarliestTime = &startParentWorkflowTS
	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
	var closedExecutions []*workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
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
		return closedExecutions[i].GetStartTime().Before(timestamp.TimeValue(closedExecutions[j].GetStartTime()))
	})
	// The first parent is not the cron workflow, only verify child workflow with cron schedule
	lastExecution := closedExecutions[1]
	for i := 2; i != 4; i++ {
		executionInfo := closedExecutions[i]
		// Round up the time precision to seconds
		expectedBackoff := executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(lastExecution.GetExecutionTime()))
		// The execution time calculate based on last execution close time
		// However, the current execution time is based on the current start time
		// This code is to remove the diff between current start time and last execution close time
		// TODO: Remove this line once we unify the time source.
		executionTimeDiff := executionInfo.GetStartTime().Sub(timestamp.TimeValue(lastExecution.GetCloseTime()))
		// The backoff between any two executions should be multiplier of the target backoff duration which is 3 in this test
		s.Equal(0, int(expectedBackoff.Seconds()-executionTimeDiff.Seconds())%int(targetBackoffDuration.Seconds()))
		lastExecution = executionInfo
	}
}

func (s *integrationSuite) TestWorkflowTimeout() {
	startTime := time.Now().UTC()

	id := "integration-workflow-timeout"
	wt := "integration-workflow-timeout-type"
	tl := "integration-workflow-timeout-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(1 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false

	time.Sleep(time.Second)

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
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			s.Logger.Warn("Execution not timedout yet. Last event: " + lastEvent.GetEventType().String())
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)

	startFilter := &filterpb.StartTimeFilter{
		EarliestTime: &startTime,
		LatestTime:   timestamp.TimePtr(time.Now().UTC()),
	}

	closedCount := 0
ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
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

func (s *integrationSuite) TestWorkflowTaskFailed() {
	id := "integration-workflowtask-failed-test"
	wt := "integration-workflowtask-failed-test-type"
	tl := "integration-workflowtask-failed-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	failureCount := 10
	signalCount := 0
	sendSignal := false
	lastWorkflowTaskTime := time.Time{}
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}
		// Some signals received on this workflow task
		if signalCount == 1 {
			return []*commandpb.Command{}, nil
		}

		// Send signals during workflow task
		if sendSignal {
			s.sendSignal(s.namespace, workflowExecution, "signalC", nil, identity)
			s.sendSignal(s.namespace, workflowExecution, "signalD", nil, identity)
			s.sendSignal(s.namespace, workflowExecution, "signalE", nil, identity)
			sendSignal = false
		}

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if failureCount > 0 {
			// Otherwise decrement failureCount and keep failing workflow tasks
			failureCount--
			return nil, errors.New("Workflow panic")
		}

		workflowComplete = true
		time.Sleep(time.Second)
		s.Logger.Warn(fmt.Sprintf("PrevStarted: %v, StartedEventID: %v, Size: %v", previousStartedEventID, startedEventID,
			len(history.Events)))
		lastWorkflowTaskEvent := history.Events[startedEventID-1]
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastWorkflowTaskEvent.GetEventType())
		lastWorkflowTaskTime = timestamp.TimeValue(lastWorkflowTaskEvent.GetEventTime())
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
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

	// Make first workflow task to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// process activity
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// fail workflow task 5 times
	for i := 1; i <= 5; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}

	err = s.sendSignal(s.namespace, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// process signal
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, signalCount)

	// send another signal to trigger workflow task
	err = s.sendSignal(s.namespace, workflowExecution, "signalB", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// fail workflow task 2 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}
	s.Equal(3, signalCount)

	// now send a signal during failed workflow task
	sendSignal = true
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, 3)
	s.NoError(err)
	s.Equal(4, signalCount)

	// fail workflow task 1 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, int32(i))
		s.NoError(err)
	}
	s.Equal(12, signalCount)

	// Make complete workflow workflow task
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 3)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(16, signalCount)

	events := s.getHistory(s.namespace, workflowExecution)
	var lastEvent *historypb.HistoryEvent
	var lastWorkflowTaskStartedEvent *historypb.HistoryEvent
	lastIdx := 0
	for i, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
			lastWorkflowTaskStartedEvent = e
			lastIdx = i
		}
		lastEvent = e
	}
	s.NotNil(lastEvent)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
	s.Logger.Info(fmt.Sprintf("Last workflow task time: %v, Last Workflow task history timestamp: %v, Complete timestamp: %v",
		lastWorkflowTaskTime, lastWorkflowTaskStartedEvent.GetEventTime(), lastEvent.GetEventTime()))
	s.Equal(lastWorkflowTaskTime, timestamp.TimeValue(lastWorkflowTaskStartedEvent.GetEventTime()))
	s.True(timestamp.TimeValue(lastEvent.GetEventTime()).Sub(lastWorkflowTaskTime) >= time.Second)

	s.Equal(2, len(events)-lastIdx-1)
	workflowTaskCompletedEvent := events[lastIdx+1]
	workflowCompletedEvent := events[lastIdx+2]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, workflowTaskCompletedEvent.GetEventType())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, workflowCompletedEvent.GetEventType())
}

func (s *integrationSuite) TestDescribeTaskQueue() {
	workflowID := "integration-get-poller-history"
	wt := "integration-get-poller-history-type"
	tl := "integration-get-poller-history-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(25 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(25 * time.Second),
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
	pollerInfos := testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Empty(pollerInfos)

	_, errWorkflowTask := poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(errWorkflowTask)
	pollerInfos = testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())

	errActivity := poller.PollAndProcessActivityTask(false)
	s.NoError(errActivity)
	pollerInfos = testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
	pollerInfos = testDescribeTaskQueue(s.namespace, &taskqueuepb.TaskQueue{Name: tl}, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
}

func (s *integrationSuite) TestTransientWorkflowTaskTimeout() {
	id := "integration-transient-workflow-task-timeout-test"
	wt := "integration-transient-workflow-task-timeout-test-type"
	tl := "integration-transient-workflow-task-timeout-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	failWorkflowTask := true
	signalCount := 0
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("Workflow panic")
		}

		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First workflow task immediately fails and schedules a transient workflow task
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now send a signal when transient workflow task is scheduled
	err = s.sendSignal(s.namespace, workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// Drop workflow task to cause a workflow task timeout
	_, err = poller.PollAndProcessWorkflowTask(true, true)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now process signal and complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 2)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.Equal(1, signalCount)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestNoTransientWorkflowTaskAfterFlushBufferedEvents() {
	id := "integration-no-transient-workflow-task-after-flush-buffered-events-test"
	wt := "integration-no-transient-workflow-task-after-flush-buffered-events-test-type"
	tl := "integration-no-transient-workflow-task-after-flush-buffered-events-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(20 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	continueAsNewAndSignal := false
	wtHandler := func(execution *commonpb.WorkflowExecution, workflowType *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !continueAsNewAndSignal {
			continueAsNewAndSignal = true
			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.engine.SignalWorkflowExecution(NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal-1",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
					Input:               nil,
					WorkflowRunTimeout:  timestamp.DurationPtr(1000 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(100 * time.Second),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// fist workflow task, this try to do a continue as new but there is a buffered event,
	// so it will fail and create a new workflow task
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("UnhandledCommand", err.Error())

	// second workflow task, which will complete the workflow
	// this expect the workflow task to have attempt == 1
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}

func (s *integrationSuite) TestRelayWorkflowTaskTimeout() {
	id := "integration-relay-workflow-task-timeout-test"
	wt := "integration-relay-workflow-task-timeout-test-type"
	tl := "integration-relay-workflow-task-timeout-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	workflowComplete, isFirst := false, true
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if isFirst {
			isFirst = false
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "test-marker",
				}},
			}}, nil
		}
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First workflow task complete with a marker command, and request to relay workflow task (immediately return a new workflow task)
	_, newTask, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		false,
		false,
		0,
		3,
		true,
		nil)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(newTask)
	s.NotNil(newTask.WorkflowTask)

	time.Sleep(time.Second * 2) // wait 2s for relay workflow task to timeout
	workflowTaskTimeout := false
	for i := 0; i < 3; i++ {
		events := s.getHistory(s.namespace, workflowExecution)
		if len(events) >= 8 {
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, events[7].GetEventType())
			s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, events[7].GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType())
			workflowTaskTimeout = true
			break
		}
		time.Sleep(time.Second)
	}
	// verify relay workflow task timeout
	s.True(workflowTaskTimeout)

	// Now complete workflow
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 2)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}

func (s *integrationSuite) TestTaskProcessingProtectionForRateLimitError() {
	id := "integration-task-processing-protection-for-rate-limit-error-test"
	wt := "integration-task-processing-protection-for-rate-limit-error-test-type"
	tl := "integration-task-processing-protection-for-rate-limit-error-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(601 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(600 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	signalCount := 0
	createUserTimer := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, h *historypb.History) ([]*commandpb.Command, error) {

		if !createUserTimer {
			createUserTimer = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-id-1",
					StartToFireTimeout: timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Process first workflow task to create user timer
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send one signal to create a new workflow task
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, 0)
	s.Nil(s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity))

	// Drop workflow task to cause all events to be buffered from now on
	_, err = poller.PollAndProcessWorkflowTask(false, true)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Buffered 100 Signals
	for i := 1; i < 101; i++ {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		s.Nil(s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity))
	}

	// 101 signal, which will fail the workflow task
	buf = new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, 101)
	signalErr := s.sendSignal(s.namespace, workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity)
	s.Nil(signalErr)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, false, 1)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(102, signalCount)
}

func (s *integrationSuite) TestStickyTimeout_NonTransientWorkflowTask() {
	id := "integration-sticky-timeout-non-transient-workflow-task"
	wt := "integration-sticky-timeout-non-transient-command-type"
	tl := "integration-sticky-timeout-non-transient-workflow-taskqueue"
	stl := "integration-sticky-timeout-non-transient-workflow-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{}
	stickyTaskQueue.Name = stl
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			// send a signal on third failure to be buffered, forcing a non-transient workflow task when buffer is flushed
			/*if failureCount == 3 {
				err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:            s.namespace,
					WorkflowExecution: workflowExecution,
					SignalName:        "signalB",
					Input:             codec.EncodeString("signal input"),
					Identity:          identity,
					RequestId:         uuid.New(),
				})
				s.NoError(err)
			}*/
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.namespace, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	workflowTaskFailed := false
	events := s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			workflowTaskFailed = true
			break
		}
	}
	s.True(workflowTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, 3)

	// Assert for single workflow task failed and workflow completion
	failedWorkflowTasks := 0
	workflowComplete := false
	events = s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			failedWorkflowTasks++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedWorkflowTasks, "Mismatched failed workflow tasks count")
}

func (s *integrationSuite) TestStickyTaskqueueResetThenTimeout() {
	id := "integration-reset-sticky-fire-schedule-to-start-timeout"
	wt := "integration-reset-sticky-fire-schedule-to-start-timeout-type"
	tl := "integration-reset-sticky-fire-schedule-to-start-timeout-taskqueue"
	stl := "integration-reset-sticky-fire-schedule-to-start-timeout-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{}
	stickyTaskQueue.Name = stl
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})

	// Reset sticky taskqueue before sticky workflow task starts
	s.engine.ResetStickyTaskQueue(NewContext(), &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: s.namespace,
		Execution: workflowExecution,
	})

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
		events := s.getHistory(s.namespace, workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType())
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second)
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.New(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, int32(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	workflowTaskFailed := false
	events := s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			workflowTaskFailed = true
			break
		}
	}
	s.True(workflowTaskFailed)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTaskWithAttempt(true, false, false, true, 3)

	// Assert for single workflow task failed and workflow completion
	failedWorkflowTasks := 0
	workflowComplete := false
	events = s.getHistory(s.namespace, workflowExecution)
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			failedWorkflowTasks++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			workflowComplete = true
		}
	}
	s.True(workflowComplete, "Workflow not complete")
	s.Equal(2, failedWorkflowTasks, "Mismatched failed workflow tasks count")
}

func (s *integrationSuite) TestBufferedEventsOutOfOrder() {
	id := "integration-buffered-events-out-of-order-test"
	wt := "integration-buffered-events-out-of-order-test-type"
	tl := "integration-buffered-events-out-of-order-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(20 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	firstWorkflowTask := false
	secondWorkflowTask := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info(fmt.Sprintf("Workflow called: first: %v, second: %v, complete: %v\n", firstWorkflowTask, secondWorkflowTask, workflowComplete))

		if !firstWorkflowTask {
			firstWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
			}, {
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "Activity-1",
					ActivityType:           &commonpb.ActivityType{Name: "ActivityType"},
					Namespace:              s.namespace,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeString("some random activity input"),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(100 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(100 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(100 * time.Second),
				}},
			}}, nil
		}

		if !secondWorkflowTask {
			secondWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
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
	// activity handler
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

	// first workflow task, which will schedule an activity and add marker
	_, task, err := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		true,
		false,
		false,
		false,
		0,
		1,
		true,
		nil)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// This will cause activity start and complete to be buffered
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("pollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// second workflow task, completes another local activity and forces flush of buffered activity events
	newWorkflowTask := task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(task)

	// third workflow task, which will close workflow
	newWorkflowTask = task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Nil(task.WorkflowTask)

	events := s.getHistory(s.namespace, workflowExecution)
	var scheduleEvent, startedEvent, completedEvent *historypb.HistoryEvent
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			scheduleEvent = event
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			startedEvent = event
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
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
	tl := "integration-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString(id),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		Memo:                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.StartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &taskqueuepb.TaskQueue{Name: tl}, memo)
}

func (s *integrationSuite) TestSignalWithStartWithMemo() {
	id := "integration-signal-with-start-with-memo-test"
	wt := "integration-signal-with-start-with-memo-test-type"
	tl := "integration-signal-with-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString(id),
		},
	}

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		Memo:                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.SignalWithStartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &taskqueuepb.TaskQueue{Name: tl}, memo)
}

func (s *integrationSuite) TestCancelTimer() {
	id := "integration-cancel-timer-test"
	wt := "integration-cancel-timer-test-type"
	tl := "integration-cancel-timer-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := 2000 * time.Second
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: &timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.namespace,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       s.namespace,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalDelivered = true
		case enumspb.EVENT_TYPE_TIMER_CANCELED:
			timerCancelled = true
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			s.Fail("timer got fired")
		}
	}
}

func (s *integrationSuite) TestCancelTimer_CancelFiredAndBuffered() {
	id := "integration-cancel-timer-fired-and-buffered-test"
	wt := "integration-cancel-timer-fired-and-buffered-test-type"
	tl := "integration-cancel-timer-fired-and-buffered-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	workflowComplete := false
	timer := 4 * time.Second
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: &timer,
				}},
			}}, nil
		}

		resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       s.namespace,
			Execution:       workflowExecution,
			MaximumPageSize: 200,
		})
		s.NoError(err)
		for _, event := range resp.History.Events {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			time.Sleep(2 * timer)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.sendSignal(s.namespace, workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.True(workflowComplete)

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       s.namespace,
		Execution:       workflowExecution,
		MaximumPageSize: 200,
	})
	s.NoError(err)
	for _, event := range resp.History.Events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalDelivered = true
		case enumspb.EVENT_TYPE_TIMER_CANCELED:
			timerCancelled = true
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			s.Fail("timer got fired")
		}
	}
}

func (s *integrationSuite) TestRespondWorkflowTaskCompleted_ReturnsErrorIfInvalidArgument() {
	id := "integration-respond-workflow-task-completed-test"
	wt := "integration-respond-workflow-task-completed-test-type"
	tq := "integration-respond-workflow-task-completed-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tq},
		Input:              nil,
		WorkflowRunTimeout: timestamp.DurationPtr(100 * time.Second),
		Identity:           identity,
	}

	we0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.NotNil(we0)

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "", // Marker name is missing.
					Details:    nil,
					Header:     nil,
					Failure:    nil,
				}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadRecordMarkerAttributes: MarkerName is not set on command.", err.Error())

	resp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.GetRunId(),
		},
	})

	s.NoError(err)
	s.NotNil(resp)

	// Last event is WORKFLOW_TASK_FAILED.
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, resp.History.Events[len(resp.History.Events)-1].GetEventType())
}

// helper function for TestStartWithMemo and TestSignalWithStartWithMemo to reduce duplicate code
func (s *integrationSuite) startWithMemoHelper(startFn startFunc, id string, taskQueue *taskqueuepb.TaskQueue, memo *commonpb.Memo) {
	identity := "worker1"

	we, err0 := startFn()
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	wtHandler := func(execution *commonpb.WorkflowExecution, workflowType *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
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

	// verify open visibility
	var openExecutionInfo *workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamp.TimePtr(time.Time{}),
				LatestTime:   timestamp.TimePtr(time.Now().UTC()),
			},
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
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
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// verify history
	execution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}
	historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: execution,
	})
	s.Nil(historyErr)
	history := historyResponse.History
	firstEvent := history.Events[0]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, firstEvent.GetEventType())
	startdEventAttributes := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startdEventAttributes.Memo)

	// verify DescribeWorkflowExecution result
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: execution,
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	s.Equal(memo, descResp.WorkflowExecutionInfo.Memo)

	// verify closed visibility
	var closdExecutionInfo *workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamp.TimePtr(time.Time{}),
				LatestTime:   timestamp.TimePtr(time.Now().UTC()),
			},
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
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

func (s *integrationSuite) sendSignal(namespace string, execution *commonpb.WorkflowExecution, signalName string,
	input *commonpb.Payloads, identity string) error {
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         namespace,
		WorkflowExecution: execution,
		SignalName:        signalName,
		Input:             input,
		Identity:          identity,
	})

	return err
}
