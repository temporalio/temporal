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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
)

func (s *IntegrationBase) setupShards() {
	// shard 0 is always created, we create additional shards if needed
	for shardID := 1; shardID < testNumberOfHistoryShards; shardID++ {
		err := s.CreateShard(shardID, "", 0)
		if err != nil {
			s.logger.WithField("error", err).Fatal("Failed to create shard")
		}
	}
}

func TestIntegrationSuite(t *testing.T) {
	flag.Parse()
	if *integration {
		s := new(integrationSuite)
		suite.Run(t, s)
	} else {
		t.Skip()
	}
}

func (s *integrationSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	logger.Formatter = formatter
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)
	s.setupSuite(false, false)
}

func (s *integrationSuite) TearDownSuite() {
	s.host.Stop()
	s.host = nil
	s.TearDownWorkflowStore()
}

func (s *integrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *integrationSuite) TearDownTest() {

}

func (s *integrationSuite) setupSuite(enableGlobalDomain bool, isMasterCluster bool) {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	options := persistence.TestBaseOptions{}
	options.ClusterHost = "127.0.0.1"
	options.DropKeySpace = true
	options.SchemaDir = ".."
	options.EnableGlobalDomain = enableGlobalDomain
	options.IsMasterCluster = isMasterCluster
	s.SetupWorkflowStoreWithOptions(options, nil)

	s.setupShards()

	// TODO: Use mock messaging client until we support kafka setup onebox to write end-to-end integration test
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)

	s.host = NewCadence(s.ClusterMetadata, s.mockMessagingClient, s.MetadataProxy, s.MetadataManagerV2, s.ShardMgr, s.HistoryMgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger, 0, false)
	s.host.Start()

	s.engine = s.host.GetFrontendClient()
	s.domainName = "integration-test-domain"
	s.domainID = uuid.New()

	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          s.domainID,
			Name:        s.domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test domain for integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:  1,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})
	s.foreignDomainName = "integration-foreign-test-domain"
	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        s.foreignDomainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test foreign domain for integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:  1,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})
}

func (s *integrationSuite) TestStartWorkflowExecution() {
	id := "integration-start-workflow-test"
	wt := "integration-start-workflow-test-type"
	tl := "integration-start-workflow-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we0, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	we1, err1 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err1)
	s.Equal(we0.RunId, we1.RunId)

	newRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err2 := s.engine.StartWorkflowExecution(createContext(), newRequest)
	s.NotNil(err2)
	s.IsType(&workflow.WorkflowExecutionAlreadyStartedError{}, err2)
	log.Infof("Unable to start workflow execution: %v", err2.Error())
	s.Nil(we2)
}

func (s *integrationSuite) TestTerminateWorkflow() {
	id := "integration-terminate-workflow-test"
	wt := "integration-terminate-workflow-test-type"
	tl := "integration-terminate-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	terminateReason := "terminate reason."
	terminateDetails := []byte("terminate details.")
	err = s.engine.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
		Reason:   common.StringPtr(terminateReason),
		Details:  terminateDetails,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	executionTerminated := false
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionTerminated {
			s.logger.Warnf("Execution not terminated yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.WorkflowExecutionTerminatedEventAttributes
		s.Equal(terminateReason, *terminateEventAttributes.Reason)
		s.Equal(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, *terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}

	s.True(executionTerminated)

	newExecutionStarted := false
StartNewExecutionLoop:
	for i := 0; i < 10; i++ {
		request := &workflow.StartWorkflowExecutionRequest{
			RequestId:    common.StringPtr(uuid.New()),
			Domain:       common.StringPtr(s.domainName),
			WorkflowId:   common.StringPtr(id),
			WorkflowType: workflowType,
			TaskList:     taskList,
			Input:        nil,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			Identity:                            common.StringPtr(identity),
		}

		newExecution, err := s.engine.StartWorkflowExecution(createContext(), request)
		if err != nil {
			s.logger.Warnf("Start New Execution failed. Error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue StartNewExecutionLoop
		}

		s.logger.Infof("New Execution Started with the same ID.  WorkflowID: %v, RunID: %v", id,
			*newExecution.RunId)
		newExecutionStarted = true
		break StartNewExecutionLoop
	}

	s.True(newExecutionStarted)
}

func (s *integrationSuite) TestSequentialWorkflow() {
	id := "interation-sequential-workflow-test"
	wt := "interation-sequential-workflow-test-type"
	tl := "interation-sequential-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	activityCount := int32(10)
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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
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

	expectedActivity := int32(1)
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err)
		if i%2 == 0 {
			err = poller.PollAndProcessActivityTask(false)
		} else { // just for testing respondActivityTaskCompleteByID
			err = poller.PollAndProcessActivityTaskWithID(false)
		}
		s.logger.Infof("PollAndProcessActivityTask: %v", err)
		s.Nil(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestCompleteDecisionTaskAndCreateNewOne() {
	id := "interation-complete-decision-create-new-test"
	wt := "interation-complete-decision-create-new-test-type"
	tl := "interation-complete-decision-create-new-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	decisionCount := 0
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if decisionCount < 2 {
			decisionCount++
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRecordMarker),
				RecordMarkerDecisionAttributes: &workflow.RecordMarkerDecisionAttributes{
					MarkerName: common.StringPtr("test-marker"),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		StickyTaskList:  taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(false, false, true, true, int64(0), 1, true)
	s.Nil(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)

	s.Equal(int64(3), newTask.DecisionTask.GetPreviousStartedEventId())
	s.Equal(int64(7), newTask.DecisionTask.GetStartedEventId())
	s.Equal(4, len(newTask.DecisionTask.History.Events))
	s.Equal(workflow.EventTypeDecisionTaskCompleted, newTask.DecisionTask.History.Events[0].GetEventType())
	s.Equal(workflow.EventTypeMarkerRecorded, newTask.DecisionTask.History.Events[1].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, newTask.DecisionTask.History.Events[2].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskStarted, newTask.DecisionTask.History.Events[3].GetEventType())
}

func (s *integrationSuite) TestDecisionAndActivityTimeoutsWorkflow() {
	id := "interation-timeouts-workflow-test"
	wt := "interation-timeouts-workflow-test-type"
	tl := "interation-timeouts-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	activityCount := int32(4)
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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
				},
			}}, nil
		}

		s.logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		s.logger.Infof("Activity ID: %v", activityID)
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	for i := 0; i < 8; i++ {
		dropDecisionTask := (i%2 == 0)
		s.logger.Infof("Calling Decision Task: %d", i)
		var err error
		if dropDecisionTask {
			_, err = poller.PollAndProcessDecisionTask(true, true)
		} else {
			_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
		}
		if err != nil {
			historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
				Domain: common.StringPtr(s.domainName),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(id),
					RunId:      common.StringPtr(*we.RunId),
				},
			})
			s.Nil(err)
			history := historyResponse.History
			common.PrettyPrintHistory(history, s.logger)
		}
		s.True(err == nil || err == matching.ErrNoTasks, "Error: %v", err)
		if !dropDecisionTask {
			s.logger.Infof("Calling Activity Task: %d", i)
			err = poller.PollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || err == matching.ErrNoTasks)
		}
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
}

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

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(15),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
				},
			}}, nil
		}

		s.logger.Info("Completing Workflow.")

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
			s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
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
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)

	s.False(workflowComplete)
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
	s.True(activityExecutedCount == 1)
}

func (s *integrationSuite) TestActivityRetry() {
	id := "integration-activity-retry-test"
	wt := "integration-activity-retry-type"
	tl := "integration-activity-retry-tasklist"
	identity := "worker1"
	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
						ActivityId:   common.StringPtr("A"),
						ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
						TaskList:     &workflow.TaskList{Name: &tl},
						Input:        []byte("1"),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(4),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(4),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(4),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
						RetryPolicy: &workflow.RetryPolicy{
							InitialIntervalInSeconds: common.Int32Ptr(1),
							MaximumAttempts:          common.Int32Ptr(3),
							MaximumIntervalInSeconds: common.Int32Ptr(1),
							NonRetriableErrorReasons: []string{"bad-bug"},
							BackoffCoefficient:       common.Float64Ptr(1),
						},
					},
				},
				{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
					ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:   common.StringPtr("B"),
						ActivityType: &workflow.ActivityType{Name: common.StringPtr(timeoutActivityName)},
						TaskList:     &workflow.TaskList{Name: common.StringPtr("no_worker_tasklist")},
						Input:        []byte("2"),
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
			s.logger.Info("Completing Workflow.")
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
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks, err)

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)
	for i := 0; i < 3; i++ {
		s.False(workflowComplete)

		s.logger.Infof("Processing decision task: %v", i)
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

	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(we.GetRunId()),
	})
	s.True(workflowComplete)
	s.True(activityExecutedCount == 2)
}

func (s *integrationSuite) TestWorkflowRetry() {
	id := "integration-wf-retry-test"
	wt := "integration-wf-retry-type"
	tl := "integration-wf-retry-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
		RetryPolicy: &workflow.RetryPolicy{
			InitialIntervalInSeconds: common.Int32Ptr(1),
			MaximumAttempts:          common.Int32Ptr(3),
			MaximumIntervalInSeconds: common.Int32Ptr(1),
			NonRetriableErrorReasons: []string{"bad-bug"},
			BackoffCoefficient:       common.Float64Ptr(1),
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	var executions []*workflow.WorkflowExecution

	attemptCount := 0

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		executions = append(executions, execution)
		attemptCount++
		if attemptCount == 3 {
			return nil, []*workflow.Decision{
				{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
					CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
						Result: []byte("succeed-after-retry"),
					},
				}}, nil
		}
		return nil, []*workflow.Decision{
			{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
				FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
					Reason:  common.StringPtr("retryable-error"),
					Details: nil,
				},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil, err)

	s.Equal(3, attemptCount)

	events := s.getHistory(s.domainName, executions[0])
	s.Equal(workflow.EventTypeWorkflowExecutionContinuedAsNew, events[len(events)-1].GetEventType())

	events = s.getHistory(s.domainName, executions[1])
	s.Equal(workflow.EventTypeWorkflowExecutionContinuedAsNew, events[len(events)-1].GetEventType())

	events = s.getHistory(s.domainName, executions[2])
	s.Equal(workflow.EventTypeWorkflowExecutionCompleted, events[len(events)-1].GetEventType())
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
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		s.logger.Infof("Calling DecisionTask Handler: %d, %d.", activityCounter, activityCount)

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
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
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.PollAndProcessActivityTask(false)

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)

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
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(300),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
					ActivityId:   common.StringPtr("A"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: common.StringPtr("NoWorker")},
					Input:        []byte("ScheduleToStart"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(35),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(3), // ActivityID A is expected to timeout using ScheduleToStart
					StartToCloseTimeoutSeconds:    common.Int32Ptr(30),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr("B"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        []byte("ScheduleClose"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(7), // ActivityID B is expected to timeout using ScheduleClose
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(10),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr("C"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        []byte("StartToClose"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(15),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(5), // ActivityID C is expected to timeout using StartToClose
					HeartbeatTimeoutSeconds:       common.Int32Ptr(0),
				},
			}, {
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr("D"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        []byte("Heartbeat"),
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
			s.logger.Errorf("Failing workflow.")
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
				FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
					Reason: common.StringPtr(failReason),
				},
			}}, nil
		}

		if activityATimedout && activityBTimedout && activityCTimedout && activityDTimedout {
			s.logger.Info("Completing Workflow.")
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
			s.logger.Infof("Sleeping activityB for 6 seconds.")
			time.Sleep(7 * time.Second)
		case "StartToClose":
			s.logger.Infof("Sleeping activityC for 6 seconds.")
			time.Sleep(8 * time.Second)
		case "Heartbeat":
			s.logger.Info("Starting hearbeat activity.")
			go func() {
				for i := 0; i < 6; i++ {
					s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
					_, err := s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
						TaskToken: taskToken, Details: []byte(string(i))})
					s.Nil(err)
					time.Sleep(1 * time.Second)
				}
				s.logger.Info("End Heartbeating.")
			}()
			s.logger.Info("Sleeping hearbeat activity.")
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
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	for i := 0; i < 3; i++ {
		go func() {
			err = poller.PollAndProcessActivityTask(false)
			s.logger.Infof("Activity Processing Completed.  Error: %v", err)
		}()
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)
	for i := 0; i < 10; i++ {
		s.logger.Infof("Processing decision task: %v", i)
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Nil(err, "Poll for decision task failed.")

		if workflowComplete {
			break
		}
	}

	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(we.GetRunId()),
	})
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
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(70),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
						ActivityId:   common.StringPtr(aID),
						ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
						TaskList:     &workflow.TaskList{Name: &tl},
						Input:        []byte("Heartbeat"),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(60),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(60),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(3),
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
			s.logger.Errorf("Failing workflow. Reason: %v", failReason)
			workflowComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
				FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
					Reason: common.StringPtr(failReason),
				},
			}}, nil
		}

		if activitiesTimedout == activityCount {
			s.logger.Info("Completing Workflow.")
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
		s.logger.Infof("Starting heartbeat activity. ID: %v", activityID)
		for i := 0; i < 10; i++ {
			if !workflowComplete {
				s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
				_, err := s.engine.RecordActivityTaskHeartbeat(createContext(), &workflow.RecordActivityTaskHeartbeatRequest{
					TaskToken: taskToken, Details: []byte(strconv.Itoa(i))})
				if err != nil {
					s.logger.Errorf("Activity heartbeat failed.  ID: %v, Progress: %v, Error: %v", activityID, i, err)
				}

				secondsToSleep := rand.Intn(3)
				s.logger.Infof("Activity ID '%v' sleeping for: %v seconds", activityID, secondsToSleep)
				time.Sleep(time.Duration(secondsToSleep) * time.Second)
			}
		}
		s.logger.Infof("End Heartbeating. ID: %v", activityID)

		s.logger.Infof("Sleeping activity before completion. ID: %v", activityID)
		time.Sleep(5 * time.Second)

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	for i := 0; i < activityCount; i++ {
		go func() {
			err := poller.PollAndProcessActivityTask(false)
			s.logger.Infof("Activity Processing Completed.  Error: %v", err)
		}()
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)
	for i := 0; i < 10; i++ {
		s.logger.Infof("Processing decision task: %v", i)
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Nil(err, "Poll for decision task failed.")

		if workflowComplete {
			break
		}
	}

	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(we.GetRunId()),
	})
	s.True(workflowComplete)
	s.False(failWorkflow, failReason)
	s.Equal(activityCount, activitiesTimedout)
	s.Equal(activityCount, len(lastHeartbeatMap))
	for aID, lastHeartbeat := range lastHeartbeatMap {
		s.logger.Infof("Last heartbeat for activity with scheduleID '%v': %v", aID, lastHeartbeat)
		s.Equal(9, lastHeartbeat)
	}
}

func (s *integrationSuite) TestSequential_UserTimers() {
	id := "interation-sequential-user-timers-test"
	wt := "interation-sequential-user-timers-test-type"
	tl := "interation-sequential-user-timers-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	timerCount := int32(4)
	timerCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if timerCounter < timerCount {
			timerCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
			return []byte(strconv.Itoa(int(timerCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartTimer),
				StartTimerDecisionAttributes: &workflow.StartTimerDecisionAttributes{
					TimerId:                   common.StringPtr(fmt.Sprintf("timer-id-%d", timerCounter)),
					StartToFireTimeoutSeconds: common.Int64Ptr(1),
				},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(timerCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	for i := 0; i < 4; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Info("PollAndProcessDecisionTask: completed")
		s.Nil(err)
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
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
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
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

		s.logger.Info("Completing Workflow.")

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
			s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
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
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	cancelCh := make(chan struct{})

	go func() {
		s.logger.Info("Trying to cancel the task in a different thread.")
		scheduleActivity = false
		requestCancellation = true
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.True(err == nil || err == matching.ErrNoTasks)
		cancelCh <- struct{}{}
	}()

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	<-cancelCh
	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)
}

func (s *integrationSuite) TestSignalWorkflow() {
	id := "integration-signal-workflow-test"
	wt := "integration-signal-workflow-test-type"
	tl := "integration-signal-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Send a signal to non-exist workflow
	err0 := s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(uuid.New()),
		},
		SignalName: common.StringPtr("failed signal."),
		Input:      nil,
		Identity:   common.StringPtr(identity),
	})
	s.NotNil(err0)
	s.IsType(&workflow.EntityNotExistsError{}, err0)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Send first signal using RunID
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

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = []byte("another signal input.")
	err = s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		SignalName: common.StringPtr(signalName),
		Input:      signalInput,
		Identity:   common.StringPtr(identity),
	})
	s.Nil(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)

	// Terminate workflow execution
	err = s.engine.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr("test signal"),
		Details:  nil,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	// Send signal to terminated workflow
	err = s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
		SignalName: common.StringPtr("failed signal 1."),
		Input:      nil,
		Identity:   common.StringPtr(identity),
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *integrationSuite) TestRateLimitBufferedEvents() {
	id := "integration-rate-limit-buffered-events-test"
	wt := "integration-rate-limit-buffered-events-test-type"
	tl := "integration-rate-limit-buffered-events-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(*we.RunId),
	}

	// decider logic
	workflowComplete := false
	signalsSent := false
	signalCount := 0
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, h *workflow.History) ([]byte, []*workflow.Decision, error) {

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
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

			// Rate limitted signals
			for i := 0; i < 10; i++ {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, i)
				signalErr := s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity)
				s.NotNil(signalErr)
				s.Equal(history.ErrBufferedEventsLimitExceeded, signalErr)
			}

			// First decision is empty
			return nil, []*workflow.Decision{}, nil
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.printWorkflowHistory(s.domainName, workflowExecution)

	s.True(workflowComplete)
	s.Equal(100, signalCount)
}

func (s *integrationSuite) TestSignalWorkflow_DuplicateRequest() {
	id := "interation-signal-workflow-test-duplicate"
	wt := "interation-signal-workflow-test-duplicate-type"
	tl := "interation-signal-workflow-test-duplicate-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *workflow.HistoryEvent
	numOfSignaledEvent := 0
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			numOfSignaledEvent = 0
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
			return nil, []*workflow.Decision{}, nil
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Send first signal
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	requestID := uuid.New()
	signalReqest := &workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
		SignalName: common.StringPtr(signalName),
		Input:      signalInput,
		Identity:   common.StringPtr(identity),
		RequestId:  common.StringPtr(requestID),
	}
	err = s.engine.SignalWorkflowExecution(createContext(), signalReqest)
	s.Nil(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
	s.Equal(1, numOfSignaledEvent)

	// Send another signal with same request id
	err = s.engine.SignalWorkflowExecution(createContext(), signalReqest)
	s.Nil(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(0, numOfSignaledEvent)
}

func (s *integrationSuite) TestBufferedEvents() {
	id := "interation-buffered-events-test"
	wt := "interation-buffered-events-test-type"
	tl := "interation-buffered-events-test-tasklist"
	identity := "worker1"
	signalName := "buffered-signal"

	workflowType := &workflow.WorkflowType{Name: &wt}
	taskList := &workflow.TaskList{Name: &tl}

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	workflowComplete := false
	signalSent := false
	var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !signalSent {
			signalSent = true

			// this will create new event when there is in-flight decision task, and the new event will be buffered
			err := s.engine.SignalWorkflowExecution(createContext(),
				&workflow.SignalWorkflowExecutionRequest{
					Domain: common.StringPtr(s.domainName),
					WorkflowExecution: &workflow.WorkflowExecution{
						WorkflowId: common.StringPtr(id),
					},
					SignalName: common.StringPtr("buffered-signal"),
					Input:      []byte("buffered-signal-input"),
					Identity:   common.StringPtr(identity),
				})
			s.NoError(err)
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr("1"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr("test-activity-type")},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        []byte("test-input"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 && signalEvent == nil {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// first decision, which sends signal and the signal event should be buffered to append after first decision closed
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// check history, the signal event should be after the complete decision task
	histResp, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(s.domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	s.NotNil(histResp.History.Events)
	s.True(len(histResp.History.Events) >= 6)
	s.Equal(histResp.History.Events[3].GetEventType(), workflow.EventTypeDecisionTaskCompleted)
	s.Equal(histResp.History.Events[4].GetEventType(), workflow.EventTypeActivityTaskScheduled)
	s.Equal(histResp.History.Events[5].GetEventType(), workflow.EventTypeWorkflowExecutionSignaled)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(signalEvent)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestQueryWorkflow_Sticky() {
	id := "interation-query-workflow-test-sticky"
	wt := "interation-query-workflow-test-sticky-type"
	tl := "interation-query-workflow-test-sticky-tasklist"
	stl := "interation-query-workflow-test-sticky-tasklist-sticky"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	stickyTaskList := &workflow.TaskList{}
	stickyTaskList.Name = common.StringPtr(stl)
	stickyScheduleToStartTimeoutSeconds := common.Int32Ptr(10)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	queryHandler := func(task *workflow.PollForDecisionTaskResponse) ([]byte, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if *task.Query.QueryType == queryType {
			return []byte("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:                              s.engine,
		Domain:                              s.domainName,
		TaskList:                            taskList,
		Identity:                            identity,
		DecisionHandler:                     dtHandler,
		ActivityHandler:                     atHandler,
		QueryHandler:                        queryHandler,
		Logger:                              s.logger,
		T:                                   s.T(),
		StickyTaskList:                      stickyTaskList,
		StickyScheduleToStartTimeoutSeconds: stickyScheduleToStartTimeoutSeconds,
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, true, int64(0))
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	type QueryResult struct {
		Resp *workflow.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string) {
		queryResp, err := s.engine.QueryWorkflow(createContext(), &workflow.QueryWorkflowRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
			Query: &workflow.WorkflowQuery{
				QueryType: common.StringPtr(queryType),
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessDecisionTaskWithSticky(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	queryResultString := string(queryResult.Resp.QueryResult)
	s.Equal("query-result", queryResultString)

	go queryWorkflowFn("invalid-query-type")
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessDecisionTaskWithSticky(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(errInner)
		if isQueryTask {
			break
		}
	}
	queryResult = <-queryResultCh
	s.NotNil(queryResult.Err)
	queryFailError, ok := queryResult.Err.(*workflow.QueryFailedError)
	s.True(ok)
	s.Equal("unknown-query-type", queryFailError.Message)
}

func (s *integrationSuite) TestQueryWorkflow_StickyTimeout() {
	id := "interation-query-workflow-test-sticky-timeout"
	wt := "interation-query-workflow-test-sticky-timeout-type"
	tl := "interation-query-workflow-test-sticky-timeout-tasklist"
	stl := "interation-query-workflow-test-sticky-timeout-tasklist-sticky"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	stickyTaskList := &workflow.TaskList{}
	stickyTaskList.Name = common.StringPtr(stl)
	stickyScheduleToStartTimeoutSeconds := common.Int32Ptr(10)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	queryHandler := func(task *workflow.PollForDecisionTaskResponse) ([]byte, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if *task.Query.QueryType == queryType {
			return []byte("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:                              s.engine,
		Domain:                              s.domainName,
		TaskList:                            taskList,
		Identity:                            identity,
		DecisionHandler:                     dtHandler,
		ActivityHandler:                     atHandler,
		QueryHandler:                        queryHandler,
		Logger:                              s.logger,
		T:                                   s.T(),
		StickyTaskList:                      stickyTaskList,
		StickyScheduleToStartTimeoutSeconds: stickyScheduleToStartTimeoutSeconds,
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, true, int64(0))
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	type QueryResult struct {
		Resp *workflow.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string) {
		queryResp, err := s.engine.QueryWorkflow(createContext(), &workflow.QueryWorkflowRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
			Query: &workflow.WorkflowQuery{
				QueryType: common.StringPtr(queryType),
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		// here we poll on normal tasklist, to simulate a worker crash and restart
		// on the server side, server will first try the sticky tasklist and then the normal tasklist
		isQueryTask, errInner := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	queryResultString := string(queryResult.Resp.QueryResult)
	s.Equal("query-result", queryResultString)
}

func (s *integrationSuite) TestQueryWorkflow_NonSticky() {
	id := "interation-query-workflow-test-non-sticky"
	wt := "interation-query-workflow-test-non-sticky-type"
	tl := "interation-query-workflow-test-non-sticky-tasklist"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	queryHandler := func(task *workflow.PollForDecisionTaskResponse) ([]byte, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if *task.Query.QueryType == queryType {
			return []byte("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		QueryHandler:    queryHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	type QueryResult struct {
		Resp *workflow.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string) {
		queryResp, err := s.engine.QueryWorkflow(createContext(), &workflow.QueryWorkflowRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
			Query: &workflow.WorkflowQuery{
				QueryType: common.StringPtr(queryType),
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(errInner)
		if isQueryTask {
			break
		}
	} // wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	queryResultString := string(queryResult.Resp.QueryResult)
	s.Equal("query-result", queryResultString)

	go queryWorkflowFn("invalid-query-type")
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(errInner)
		if isQueryTask {
			break
		}
	}
	queryResult = <-queryResultCh
	s.NotNil(queryResult.Err)
	queryFailError, ok := queryResult.Err.(*workflow.QueryFailedError)
	s.True(ok)
	s.Equal("unknown-query-type", queryFailError.Message)
}

func (s *integrationSuite) TestDescribeWorkflowExecution() {
	id := "interation-describe-wfe-test"
	wt := "interation-describe-wfe-test-type"
	tl := "interation-describe-wfe-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{Name: &wt}
	taskList := &workflow.TaskList{Name: &tl}

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	describeWorkflowExecution := func() (*workflow.DescribeWorkflowExecutionResponse, error) {
		return s.engine.DescribeWorkflowExecution(createContext(), &workflow.DescribeWorkflowExecutionRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      we.RunId,
			},
		})
	}
	dweResponse, err := describeWorkflowExecution()
	s.Nil(err)
	s.True(nil == dweResponse.WorkflowExecutionInfo.CloseTime)
	s.Equal(int64(2), *dweResponse.WorkflowExecutionInfo.HistoryLength) // WorkflowStarted, DecisionScheduled

	// decider logic
	workflowComplete := false
	signalSent := false
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr("1"),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr("test-activity-type")},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        []byte("test-input"),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// first decision to schedule new activity
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	dweResponse, err = describeWorkflowExecution()
	s.Nil(err)
	s.True(nil == dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(5), *dweResponse.WorkflowExecutionInfo.HistoryLength) // DecisionStarted, DecisionCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.PendingActivities))
	s.Equal("test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
	s.Equal(int64(0), dweResponse.PendingActivities[0].GetLastHeartbeatTimestamp())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.Nil(err)
	s.True(nil == dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(8), *dweResponse.WorkflowExecutionInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, DecisionTaskScheduled
	s.Equal(0, len(dweResponse.PendingActivities))

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.Nil(err)
	s.Equal(workflow.WorkflowExecutionCloseStatusCompleted, *dweResponse.WorkflowExecutionInfo.CloseStatus)
	s.Equal(int64(11), *dweResponse.WorkflowExecutionInfo.HistoryLength) // DecisionStarted, DecisionCompleted, WorkflowCompleted
}

func (s *integrationSuite) TestContinueAsNewWorkflow() {
	id := "interation-continue-as-new-workflow-test"
	wt := "interation-continue-as-new-workflow-test-type"
	tl := "interation-continue-as-new-workflow-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeContinueAsNewWorkflowExecution),
				ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType: workflowType,
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
				},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.WorkflowExecutionStartedEventAttributes.GetContinuedExecutionRunId())
}

func (s *integrationSuite) TestContinueAsNewWorkflow_Timeout() {
	id := "interation-continue-as-new-workflow-timeout-test"
	wt := "interation-continue-as-new-workflow-timeout-test-type"
	tl := "interation-continue-as-new-workflow-timeout-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	continueAsNewCount := int32(1)
	continueAsNewCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeContinueAsNewWorkflowExecution),
				ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType: workflowType,
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1), // set timeout to 1
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
				},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// process the decision and continue as new
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)

	time.Sleep(1 * time.Second) // wait 1 second for timeout

GetHistoryLoop:
	for i := 0; i < 20; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionTimedOut {
			s.logger.Warnf("Execution not timedout yet.")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.WorkflowExecutionTimedOutEventAttributes
		s.Equal(workflow.TimeoutTypeStartToClose, *timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)
}

func (s *integrationSuite) TestVisibility() {
	startTime := time.Now().UnixNano()

	// Start 2 workflow executions
	id1 := "integration-visibility-test1"
	id2 := "integration-visibility-test2"
	wt := "integration-visibility-test-type"
	tl := "integration-visibility-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	startRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id1),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	startResponse, err0 := s.engine.StartWorkflowExecution(createContext(), startRequest)
	s.Nil(err0)

	// Now complete one of the executions
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		return []byte{}, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err1 := poller.PollAndProcessDecisionTask(false, false)
	s.Nil(err1)

	// wait until the start workflow is done
	var nexttoken []byte
	historyEventFilterType := workflow.HistoryEventFilterTypeCloseEvent
	for {
		historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: startRequest.Domain,
			Execution: &workflow.WorkflowExecution{
				WorkflowId: startRequest.WorkflowId,
				RunId:      startResponse.RunId,
			},
			WaitForNewEvent:        common.BoolPtr(true),
			HistoryEventFilterType: &historyEventFilterType,
			NextPageToken:          nexttoken,
		})
		s.Nil(historyErr)
		if len(historyResponse.NextPageToken) == 0 {
			break
		}

		nexttoken = historyResponse.NextPageToken
	}

	startRequest = &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id2),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	_, err2 := s.engine.StartWorkflowExecution(createContext(), startRequest)
	s.Nil(err2)

	startFilter := &workflow.StartTimeFilter{}
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

	closedCount := 0
	openCount := 0

	resp, err3 := s.engine.ListClosedWorkflowExecutions(createContext(), &workflow.ListClosedWorkflowExecutionsRequest{
		Domain:          common.StringPtr(s.domainName),
		MaximumPageSize: common.Int32Ptr(100),
		StartTimeFilter: startFilter,
	})
	s.Nil(err3)
	closedCount = len(resp.Executions)
	s.Equal(1, closedCount)

	for i := 0; i < 10; i++ {
		resp, err4 := s.engine.ListOpenWorkflowExecutions(createContext(), &workflow.ListOpenWorkflowExecutionsRequest{
			Domain:          common.StringPtr(s.domainName),
			MaximumPageSize: common.Int32Ptr(100),
			StartTimeFilter: startFilter,
		})
		s.Nil(err4)
		openCount = len(resp.Executions)
		if openCount == 1 {
			break
		}
		s.logger.Info("Open WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.Equal(1, openCount)
}

func (s *integrationSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "integration-request-cancel-workflow-test"
	wt := "integration-request-cancel-workflow-test-type"
	tl := "integration-request-cancel-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelWorkflowExecution),
			CancelWorkflowExecutionDecisionAttributes: &workflow.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = poller.PollAndProcessActivityTask(false)
	s.logger.Infof("PollAndProcessActivityTask: %v", err)
	s.Nil(err)

	err = s.engine.RequestCancelWorkflowExecution(createContext(), &workflow.RequestCancelWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
	})
	s.Nil(err)

	err = s.engine.RequestCancelWorkflowExecution(createContext(), &workflow.RequestCancelWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(*we.RunId),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.CancellationAlreadyRequestedError{}, err)

	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 3; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionCanceled {
			s.logger.Warnf("Execution not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.WorkflowExecutionCanceledEventAttributes
		s.Equal("Cancelled", string(cancelledEventAttributes.Details))
		executionCancelled = true
		break GetHistoryLoop
	}
	s.True(executionCancelled)
}

func (s *integrationSuite) TestRequestCancelWorkflowDecisionExecution() {
	id := "integration-cancel-workflow-decision-test"
	wt := "integration-cancel-workflow-decision-test-type"
	tl := "integration-cancel-workflow-decision-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	foreignRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.foreignDomainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err0 := s.engine.StartWorkflowExecution(createContext(), foreignRequest)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution on foreign Domain: %v,  response: %v \n", s.foreignDomainName, *we2.RunId)

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelExternalWorkflowExecution),
			RequestCancelExternalWorkflowExecutionDecisionAttributes: &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     common.StringPtr(s.foreignDomainName),
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we2.RunId),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	foreignDtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(foreignActivityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelWorkflowExecution),
			CancelWorkflowExecutionDecisionAttributes: &workflow.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
			},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.foreignDomainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: foreignDtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.logger.Infof("foreign PollAndProcessActivityTask: %v", err)
	s.Nil(err)

	// Cancel the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	cancellationSent := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-2]
		if *lastEvent.EventType != workflow.EventTypeExternalWorkflowExecutionCancelRequested {
			s.logger.Info("Cancellation still not sent.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		externalWorkflowExecutionCancelRequestedEvent := lastEvent.ExternalWorkflowExecutionCancelRequestedEventAttributes
		s.Equal(int64(intiatedEventID), *externalWorkflowExecutionCancelRequestedEvent.InitiatedEventId)
		s.Equal(id, *externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.WorkflowId)
		s.Equal(we2.RunId, externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.RunId)

		cancellationSent = true
		break
	}

	s.True(cancellationSent)

	// Accept cancellation.
	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.foreignDomainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we2.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionCanceled {
			s.logger.Warnf("Execution not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.WorkflowExecutionCanceledEventAttributes
		s.Equal("Cancelled", string(cancelledEventAttributes.Details))
		executionCancelled = true

		// Find cancel requested event and verify it.
		var cancelRequestEvent *workflow.HistoryEvent
		for _, x := range history.Events {
			if *x.EventType == workflow.EventTypeWorkflowExecutionCancelRequested {
				cancelRequestEvent = x
			}
		}

		s.NotNil(cancelRequestEvent)
		cancelRequestEventAttributes := cancelRequestEvent.WorkflowExecutionCancelRequestedEventAttributes
		s.Equal(int64(intiatedEventID), *cancelRequestEventAttributes.ExternalInitiatedEventId)
		s.Equal(id, *cancelRequestEventAttributes.ExternalWorkflowExecution.WorkflowId)
		s.Equal(we.RunId, cancelRequestEventAttributes.ExternalWorkflowExecution.RunId)

		break GetHistoryLoop
	}
	s.True(executionCancelled)
}

func (s *integrationSuite) TestRequestCancelWorkflowDecisionExecution_UnKnownTarget() {
	id := "integration-cancel-unknown-workflow-decision-test"
	wt := "integration-cancel-unknown-workflow-decision-test-type"
	tl := "integration-cancel-unknown-workflow-decision-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelExternalWorkflowExecution),
			RequestCancelExternalWorkflowExecutionDecisionAttributes: &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     common.StringPtr(s.foreignDomainName),
				WorkflowId: common.StringPtr("workflow_not_exist"),
				RunId:      common.StringPtr(*we.RunId),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Cancel the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	cancellationSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-2]
		if *lastEvent.EventType != workflow.EventTypeRequestCancelExternalWorkflowExecutionFailed {
			s.logger.Info("Cancellaton not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		requestCancelExternalWorkflowExecutionFailedEvetn := lastEvent.RequestCancelExternalWorkflowExecutionFailedEventAttributes
		s.Equal(int64(intiatedEventID), *requestCancelExternalWorkflowExecutionFailedEvetn.InitiatedEventId)
		s.Equal("workflow_not_exist", *requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.WorkflowId)
		s.Equal(we.RunId, requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.RunId)

		cancellationSentFailed = true
		break
	}

	s.True(cancellationSentFailed)
}

func (s *integrationSuite) TestHistoryVersionCompatibilityCheck() {
	id := "integration-history-version-workflow-test"
	wt := "integration-history-version-workflow-test-type"
	tl := "integration-history-version-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_simple"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	// override the default / max versions to force history
	// version incompatibilities
	prevMaxVersion := persistence.GetMaxSupportedHistoryVersion()
	prevDefaultVersion := persistence.GetDefaultHistoryVersion()
	defer func() {
		persistence.SetMaxSupportedHistoryVersion(prevMaxVersion)
		persistence.SetDefaultHistoryVersion(prevDefaultVersion)
	}()

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false
	activityCount := int32(4)
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
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(10),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(10),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(10),
				},
			}}, nil
		}

		s.logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		s.logger.Infof("Activity ID: %v", activityID)
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	upgradeRollbackStep := 2
	testDecisionPollFailStep := 3
	decisionFailed := false

	for i := 0; i < int(activityCount)+1; i++ {

		if i == upgradeRollbackStep {
			// force history to be persisted with a higher version
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion + 1)
			persistence.SetDefaultHistoryVersion(prevMaxVersion + 1)
		}

		s.logger.Infof("Calling Decision Task: %d", i)
		var err error
		if decisionFailed {
			_, err = poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(1))
		} else {
			_, err = poller.PollAndProcessDecisionTask(false, false)
		}

		if i == testDecisionPollFailStep {
			// make sure we get an error due to history version
			// incompatibility i.e. old code, new history version
			s.NotNil(err)
			// reset the supported versions, this should make subsequent
			// polls / activities to succeed
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion + 1)
			persistence.SetDefaultHistoryVersion(prevMaxVersion + 1)
			decisionFailed = true
			continue
		}

		s.True(err == nil || err == matching.ErrNoTasks)
		s.logger.Infof("Calling Activity Task: %d", i)
		err = poller.PollAndProcessActivityTask(false)
		s.True(err == nil || err == matching.ErrNoTasks)

		if i == upgradeRollbackStep {
			// now revert the versions back to the original
			// this simulates a rollback of code deployment
			// and a persisted history with future version
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion)
			persistence.SetDefaultHistoryVersion(prevMaxVersion)
		}
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", *we.RunId)

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Nil(err)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestChildWorkflowExecution() {
	parentID := "integration-child-workflow-test-parent"
	childID := "integration-child-workflow-test-child"
	wtParent := "integration-child-workflow-test-parent-type"
	wtChild := "integration-child-workflow-test-child-type"
	tlParent := "integration-child-workflow-test-parent-tasklist"
	tlChild := "integration-child-workflow-test-child-tasklist"
	identity := "worker1"

	parentWorkflowType := &workflow.WorkflowType{}
	parentWorkflowType.Name = common.StringPtr(wtParent)

	childWorkflowType := &workflow.WorkflowType{}
	childWorkflowType.Name = common.StringPtr(wtChild)

	taskListParent := &workflow.TaskList{}
	taskListParent.Name = common.StringPtr(tlParent)
	taskListChild := &workflow.TaskList{}
	taskListChild.Name = common.StringPtr(tlChild)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(parentID),
		WorkflowType: parentWorkflowType,
		TaskList:     taskListParent,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		ChildPolicy:                         common.ChildPolicyPtr(workflow.ChildPolicyRequestCancel),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	childComplete := false
	childExecutionStarted := false
	var parentStartedEvent *workflow.HistoryEvent
	var startedEvent *workflow.HistoryEvent
	var completedEvent *workflow.HistoryEvent

	// Parent Decider Logic
	dtHandlerParent := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		s.logger.Infof("Processing decision task for WorkflowID: %v", *execution.WorkflowId)

		if *execution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.logger.Info("Starting child execution.")
				childExecutionStarted = true
				parentStartedEvent = history.Events[0]

				return nil, []*workflow.Decision{{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
					StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
						WorkflowId:   common.StringPtr(childID),
						WorkflowType: childWorkflowType,
						TaskList:     taskListChild,
						Input:        []byte("child-workflow-input"),
						ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(200),
						TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
						ChildPolicy:                         common.ChildPolicyPtr(workflow.ChildPolicyRequestCancel),
						Control:                             nil,
					},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if *event.EventType == workflow.EventTypeChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*workflow.Decision{}, nil
					}

					if *event.EventType == workflow.EventTypeChildWorkflowExecutionCompleted {
						completedEvent = event
						return nil, []*workflow.Decision{{
							DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
							CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
								Result: []byte("Done."),
							},
						}}, nil
					}
				}
			}
		}

		return nil, nil, nil
	}

	var childStartedEvent *workflow.HistoryEvent
	// Child Decider Logic
	dtHandlerChild := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if previousStartedEventID <= 0 {
			childStartedEvent = history.Events[0]
		}

		s.logger.Infof("Processing decision task for Child WorkflowID: %v", *execution.WorkflowId)
		childComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Child Done."),
			},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListParent,
		Identity:        identity,
		DecisionHandler: dtHandlerParent,
		Logger:          s.logger,
		T:               s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskListChild,
		Identity:        identity,
		DecisionHandler: dtHandlerChild,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to start child execution
	_, err := pollerParent.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childExecutionStarted)
	s.Equal(workflow.ChildPolicyRequestCancel,
		parentStartedEvent.WorkflowExecutionStartedEventAttributes.GetChildPolicy())

	// Process ChildExecution Started event and Process Child Execution and complete it
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	_, err = pollerChild.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(startedEvent)
	s.True(childComplete)
	s.NotNil(childStartedEvent)
	s.Equal(workflow.EventTypeWorkflowExecutionStarted, childStartedEvent.GetEventType())
	s.Equal(s.domainName, childStartedEvent.WorkflowExecutionStartedEventAttributes.GetParentWorkflowDomain())
	s.Equal(parentID, childStartedEvent.WorkflowExecutionStartedEventAttributes.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEvent.WorkflowExecutionStartedEventAttributes.ParentWorkflowExecution.GetRunId())
	s.Equal(startedEvent.ChildWorkflowExecutionStartedEventAttributes.GetInitiatedEventId(),
		childStartedEvent.WorkflowExecutionStartedEventAttributes.GetParentInitiatedEventId())
	s.Equal(workflow.ChildPolicyRequestCancel, childStartedEvent.WorkflowExecutionStartedEventAttributes.GetChildPolicy())

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.ChildWorkflowExecutionCompletedEventAttributes
	s.Nil(completedAttributes.Domain)
	s.Equal(childID, *completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, *completedAttributes.WorkflowType.Name)
	s.Equal([]byte("Child Done."), completedAttributes.Result)

	s.logger.Info("Parent Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(parentID),
		RunId:      common.StringPtr(*we.RunId),
	})

	s.logger.Info("Child Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName,
		startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowExecution)
}

func (s *integrationSuite) TestChildWorkflowWithContinueAsNew() {
	parentID := "integration-child-workflow-with-continue-as-new-test-parent"
	childID := "integration-child-workflow-with-continue-as-new-test-child"
	wtParent := "integration-child-workflow-with-continue-as-new-test-parent-type"
	wtChild := "integration-child-workflow-with-continue-as-new-test-child-type"
	tl := "integration-child-workflow-with-continue-as-new-test-tasklist"
	identity := "worker1"

	parentWorkflowType := &workflow.WorkflowType{}
	parentWorkflowType.Name = common.StringPtr(wtParent)

	childWorkflowType := &workflow.WorkflowType{}
	childWorkflowType.Name = common.StringPtr(wtChild)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(parentID),
		WorkflowType: parentWorkflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	childComplete := false
	childExecutionStarted := false
	childData := int32(1)
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var startedEvent *workflow.HistoryEvent
	var completedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		s.logger.Infof("Processing decision task for WorkflowID: %v", *execution.WorkflowId)

		// Child Decider Logic
		if *execution.WorkflowId == childID {
			if continueAsNewCounter < continueAsNewCount {
				continueAsNewCounter++
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

				return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeContinueAsNewWorkflowExecution),
					ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
						Input: buf.Bytes(),
					},
				}}, nil
			}

			childComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Child Done."),
				},
			}}, nil
		}

		// Parent Decider Logic
		if *execution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.logger.Info("Starting child execution.")
				childExecutionStarted = true
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, childData))

				return nil, []*workflow.Decision{{
					DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
					StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
						Domain:       common.StringPtr(s.domainName),
						WorkflowId:   common.StringPtr(childID),
						WorkflowType: childWorkflowType,
						Input:        buf.Bytes(),
						ChildPolicy:  common.ChildPolicyPtr(workflow.ChildPolicyTerminate),
					},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if *event.EventType == workflow.EventTypeChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*workflow.Decision{}, nil
					}

					if *event.EventType == workflow.EventTypeChildWorkflowExecutionCompleted {
						completedEvent = event
						return nil, []*workflow.Decision{{
							DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
							CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
								Result: []byte("Done."),
							},
						}}, nil
					}
				}
			}
		}

		return nil, nil, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to start child execution
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := 0; i < 11; i++ {
		s.logger.Warnf("decision: %v", i)
		_, err = poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", err)
		s.Nil(err)
	}

	s.False(childComplete)
	s.NotNil(startedEvent)

	// Process Child Execution final decision to complete it
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childComplete)

	// Process ChildExecution completed event and complete parent execution
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.ChildWorkflowExecutionCompletedEventAttributes
	s.Equal(s.domainName, *completedAttributes.Domain)
	s.Equal(childID, *completedAttributes.WorkflowExecution.WorkflowId)
	s.NotEqual(startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowExecution.RunId,
		completedAttributes.WorkflowExecution.RunId)
	s.Equal(wtChild, *completedAttributes.WorkflowType.Name)
	s.Equal([]byte("Child Done."), completedAttributes.Result)

	s.logger.Info("Parent Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(parentID),
		RunId:      common.StringPtr(*we.RunId),
	})

	s.logger.Info("Child Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName,
		startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowExecution)
}

func (s *integrationSuite) TestWorkflowTimeout() {
	startTime := time.Now().UnixNano()

	id := "integration-workflow-timeout"
	wt := "integration-workflow-timeout-type"
	tl := "integration-workflow-timeout-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowComplete := false

GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionTimedOut {
			s.logger.Warnf("Execution not timedout yet.")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.WorkflowExecutionTimedOutEventAttributes
		s.Equal(workflow.TimeoutTypeStartToClose, *timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)

	startFilter := &workflow.StartTimeFilter{}
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

	closedCount := 0
ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(createContext(), &workflow.ListClosedWorkflowExecutionsRequest{
			Domain:          common.StringPtr(s.domainName),
			MaximumPageSize: common.Int32Ptr(100),
			StartTimeFilter: startFilter,
		})
		s.Nil(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.logger.Info("Closed WorkflowExecution is not yet visibile")
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

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(*we.RunId),
	}

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	failureCount := 10
	signalCount := 0
	sendSignal := false
	lastDecisionTimestamp := int64(0)
	//var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}
		// Some signals received on this decision
		if signalCount == 1 {
			return nil, []*workflow.Decision{}, nil
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

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if failureCount > 0 {
			// Otherwise decrement failureCount and keep failing decisions
			failureCount--
			return nil, nil, errors.New("Decider Panic")
		}

		workflowComplete = true
		time.Sleep(time.Second)
		s.logger.Warnf("PrevStarted: %v, StartedEventID: %v, Size: %v", previousStartedEventID, startedEventID,
			len(history.Events))
		lastDecisionEvent := history.Events[startedEventID-1]
		s.Equal(workflow.EventTypeDecisionTaskStarted, lastDecisionEvent.GetEventType())
		lastDecisionTimestamp = lastDecisionEvent.GetTimestamp()
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// process activity
	err = poller.PollAndProcessActivityTask(false)
	s.logger.Infof("PollAndProcessActivityTask: %v", err)
	s.Nil(err)

	// fail decision 5 times
	for i := 0; i < 5; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(i))
		s.Nil(err)
	}

	err = s.sendSignal(s.domainName, workflowExecution, "signalA", nil, identity)
	s.Nil(err, "failed to send signal to execution")

	// process signal
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.Equal(1, signalCount)

	// send another signal to trigger decision
	err = s.sendSignal(s.domainName, workflowExecution, "signalB", nil, identity)
	s.Nil(err, "failed to send signal to execution")

	// fail decision 2 more times
	for i := 0; i < 2; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(i))
		s.Nil(err)
	}
	s.Equal(3, signalCount)

	// now send a signal during failed decision
	sendSignal = true
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, int64(2))
	s.Nil(err)
	s.Equal(4, signalCount)

	// fail decision 1 more times
	for i := 0; i < 2; i++ {
		_, err := poller.PollAndProcessDecisionTaskWithAttempt(false, false, false, false, 3+int64(i))
		s.Nil(err)
	}
	s.Equal(12, signalCount)

	// Make complete workflow decision
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(5))
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(workflowComplete)
	s.Equal(16, signalCount)

	s.printWorkflowHistory(s.domainName, workflowExecution)

	events := s.getHistory(s.domainName, workflowExecution)
	var lastEvent *workflow.HistoryEvent
	var lastDecisionStartedEvent *workflow.HistoryEvent
	for _, e := range events {
		if e.GetEventType() == workflow.EventTypeDecisionTaskStarted {
			lastDecisionStartedEvent = e
		}
		lastEvent = e
	}
	s.Equal(workflow.EventTypeWorkflowExecutionCompleted, lastEvent.GetEventType())
	s.logger.Infof("Last Decision Time: %v, Last Decision History Timestamp: %v, Complete Timestamp: %v",
		time.Unix(0, lastDecisionTimestamp), time.Unix(0, lastDecisionStartedEvent.GetTimestamp()),
		time.Unix(0, lastEvent.GetTimestamp()))
	s.Equal(lastDecisionTimestamp, lastDecisionStartedEvent.GetTimestamp())
	s.True(time.Duration(lastEvent.GetTimestamp()-lastDecisionTimestamp) >= time.Second)

	partialHistory, err := s.getHistoryFrom(s.domainID, *workflowExecution, lastDecisionStartedEvent.GetEventId()+1,
		lastEvent.GetEventId()+1)
	s.Nil(err)
	s.Equal(2, len(partialHistory))
	decisionCompletedEvent := partialHistory[0]
	s.Equal(workflow.EventTypeDecisionTaskCompleted, decisionCompletedEvent.GetEventType())
	s.Equal(lastDecisionStartedEvent.GetEventId()+1, decisionCompletedEvent.GetEventId())
}

func (s *integrationSuite) TestGetWorkflowExecutionHistory_All() {
	workflowID := "interation-get-workflow-history-events-long-poll-test-all"
	workflowTypeName := "interation-get-workflow-history-events-long-poll-test-all-type"
	tasklistName := "interation-get-workflow-history-events-long-poll-test-all-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(workflowTypeName)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tasklistName)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(workflowID),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     taskList,
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(25),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(25),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// this function poll events from history side
	getHistory := func(domain string, workflowID string, token []byte, isLongPoll bool) ([]*workflow.HistoryEvent, []byte) {
		responseInner, _ := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(domain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: common.Int32Ptr(100),
			WaitForNewEvent: common.BoolPtr(isLongPoll),
			NextPageToken:   token,
		})
		return responseInner.History.Events, responseInner.NextPageToken
	}

	var allEvents []*workflow.HistoryEvent
	var events []*workflow.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// make first decision to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errDecision1 := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errDecision1)
	})
	start = time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errActivity)
	})
	time.AfterFunc(time.Second*8, func() {
		_, errDecision2 := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errDecision2)
	})
	for token != nil {
		events, token = getHistory(s.domainName, workflowID, token, true)
		allEvents = append(allEvents, events...)
	}

	// there are total 11 events
	//  1. WorkflowExecutionStarted
	//  2. DecisionTaskScheduled
	//  3. DecisionTaskStarted
	//  4. DecisionTaskCompleted
	//  5. ActivityTaskScheduled
	//  6. ActivityTaskStarted
	//  7. ActivityTaskCompleted
	//  8. DecisionTaskScheduled
	//  9. DecisionTaskStarted
	// 10. DecisionTaskCompleted
	// 11. WorkflowExecutionCompleted
	s.Equal(11, len(allEvents))

	// test non long poll
	allEvents = nil
	token = nil
	for {
		events, token = getHistory(s.domainName, workflowID, token, false)
		allEvents = append(allEvents, events...)
		if token == nil {
			break
		}
	}
	s.Equal(11, len(allEvents))
}

func (s *integrationSuite) TestGetWorkflowExecutionHistory_Close() {
	workflowID := "interation-get-workflow-history-events-long-poll-test-close"
	workflowTypeName := "interation-get-workflow-history-events-long-poll-test-close-type"
	tasklistName := "interation-get-workflow-history-events-long-poll-test-close-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(workflowTypeName)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tasklistName)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(workflowID),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     taskList,
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(25),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(25),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// this function poll events from history side
	getHistory := func(domain string, workflowID string, token []byte, isLongPoll bool) ([]*workflow.HistoryEvent, []byte) {
		closeEventOnly := workflow.HistoryEventFilterTypeCloseEvent
		responseInner, _ := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(domain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize:        common.Int32Ptr(100),
			WaitForNewEvent:        common.BoolPtr(isLongPoll),
			NextPageToken:          token,
			HistoryEventFilterType: &closeEventOnly,
		})

		return responseInner.History.Events, responseInner.NextPageToken
	}

	var events []*workflow.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	s.True(time.Now().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// make first decision to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errDecision1 := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errDecision1)
	})
	start = time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	s.True(time.Now().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errActivity)
	})
	time.AfterFunc(time.Second*8, func() {
		_, errDecision2 := poller.PollAndProcessDecisionTask(false, false)
		s.logger.Infof("PollAndProcessDecisionTask: %v", errDecision2)
	})
	for token != nil {
		events, token = getHistory(s.domainName, workflowID, token, true)

		// since we are only interested in close event
		if token == nil {
			s.Equal(1, len(events))
			s.Equal(workflow.EventTypeWorkflowExecutionCompleted, *events[0].EventType)
		} else {
			s.Empty(events)
		}
	}

	// test non long poll for only closed events
	token = nil
	for {
		events, token = getHistory(s.domainName, workflowID, token, false)
		if token == nil {
			break
		}
	}
	s.Equal(1, len(events))
}

func (s *integrationSuite) TestDescribeTaskList() {
	workflowID := "interation-get-poller-history"
	workflowTypeName := "interation-get-poller-history-type"
	tasklistName := "interation-get-poller-history-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(workflowTypeName)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tasklistName)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(workflowID),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     taskList,
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(25),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(25),
				},
			}}, nil
		}

		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// this function poll events from history side
	testDescribeTaskList := func(domain string, tasklist *workflow.TaskList, tasklistType workflow.TaskListType) []*workflow.PollerInfo {
		responseInner, errInner := s.engine.DescribeTaskList(createContext(), &workflow.DescribeTaskListRequest{
			Domain:       common.StringPtr(domain),
			TaskList:     taskList,
			TaskListType: &tasklistType,
		})

		s.Nil(errInner)
		return responseInner.Pollers
	}

	before := time.Now()

	// when no one polling on the tasklist (activity or decition), there shall be no poller information
	pollerInfos := testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeActivity)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeDecision)
	s.Empty(pollerInfos)

	_, errDecision := poller.PollAndProcessDecisionTask(false, false)
	s.Nil(errDecision)
	pollerInfos = testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeActivity)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeDecision)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())

	errActivity := poller.PollAndProcessActivityTask(false)
	s.Nil(errActivity)
	pollerInfos = testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeActivity)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
	pollerInfos = testDescribeTaskList(s.domainName, taskList, workflow.TaskListTypeDecision)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(time.Unix(0, pollerInfos[0].GetLastAccessTime()).After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision() {
	id := "integration-signal-external-workflow-test"
	wt := "integration-signal-external-workflow-test-type"
	tl := "integration-signal-external-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	foreignRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.foreignDomainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err0 := s.engine.StartWorkflowExecution(createContext(), foreignRequest)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution on foreign Domain: %v,  response: %v \n", s.foreignDomainName, *we2.RunId)

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
			SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: common.StringPtr(s.foreignDomainName),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(id),
					RunId:      common.StringPtr(we2.GetRunId()),
				},
				SignalName: common.StringPtr(signalName),
				Input:      signalInput,
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *workflow.HistoryEvent
	foreignDtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(foreignActivityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.foreignDomainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: foreignDtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.logger.Infof("foreign PollAndProcessActivityTask: %v", err)
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if *signalRequestedEvent.EventType != workflow.EventTypeExternalWorkflowExecutionSignaled {
			s.logger.Info("Signal still not sent.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}

		ewfeAttributes := signalRequestedEvent.ExternalWorkflowExecutionSignaledEventAttributes
		s.NotNil(ewfeAttributes)
		s.Equal(int64(intiatedEventID), ewfeAttributes.GetInitiatedEventId())
		s.Equal(id, ewfeAttributes.WorkflowExecution.GetWorkflowId())
		s.Equal(we2.RunId, ewfeAttributes.WorkflowExecution.RunId)

		signalSent = true
		break
	}

	s.True(signalSent)

	// process signal in decider for foreign workflow
	_, err = foreignPoller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal("history-service", *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_WithoutRunID() {
	id := "integration-signal-external-workflow-test-without-run-id"
	wt := "integration-signal-external-workflow-test-without-run-id-type"
	tl := "integration-signal-external-workflow-test-without-run-id-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	foreignRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.foreignDomainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err0 := s.engine.StartWorkflowExecution(createContext(), foreignRequest)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution on foreign Domain: %v,  response: %v \n", s.foreignDomainName, *we2.RunId)

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
			SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: common.StringPtr(s.foreignDomainName),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(id),
					// No RunID in decision
				},
				SignalName: common.StringPtr(signalName),
				Input:      signalInput,
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *workflow.HistoryEvent
	foreignDtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(foreignActivityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.foreignDomainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: foreignDtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.logger.Infof("foreign PollAndProcessActivityTask: %v", err)
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if *signalRequestedEvent.EventType != workflow.EventTypeExternalWorkflowExecutionSignaled {
			s.logger.Info("Signal still not sent.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}

		ewfeAttributes := signalRequestedEvent.ExternalWorkflowExecutionSignaledEventAttributes
		s.NotNil(ewfeAttributes)
		s.Equal(int64(intiatedEventID), ewfeAttributes.GetInitiatedEventId())
		s.Equal(id, ewfeAttributes.WorkflowExecution.GetWorkflowId())
		s.Equal("", ewfeAttributes.WorkflowExecution.GetRunId())

		signalSent = true
		break
	}

	s.True(signalSent)

	// process signal in decider for foreign workflow
	_, err = foreignPoller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal("history-service", *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_UnKnownTarget() {
	id := "integration-signal-unknown-workflow-decision-test"
	wt := "integration-signal-unknown-workflow-decision-test-type"
	tl := "integration-signal-unknown-workflow-decision-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
			SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: common.StringPtr(s.foreignDomainName),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr("workflow_not_exist"),
					RunId:      common.StringPtr(we.GetRunId()),
				},
				SignalName: common.StringPtr(signalName),
				Input:      signalInput,
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	signalSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		signalFailedEvent := history.Events[len(history.Events)-2]
		if *signalFailedEvent.EventType != workflow.EventTypeSignalExternalWorkflowExecutionFailed {
			s.logger.Info("Cancellaton not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		signalExternalWorkflowExecutionFailedEventAttributes := signalFailedEvent.SignalExternalWorkflowExecutionFailedEventAttributes
		s.Equal(int64(intiatedEventID), *signalExternalWorkflowExecutionFailedEventAttributes.InitiatedEventId)
		s.Equal("workflow_not_exist", *signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.WorkflowId)
		s.Equal(we.RunId, signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.RunId)

		signalSentFailed = true
		break
	}

	s.True(signalSentFailed)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_SignalSelf() {
	id := "integration-signal-self-workflow-decision-test"
	wt := "integration-signal-self-workflow-decision-test-type"
	tl := "integration-signal-self-workflow-decision-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
			SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: common.StringPtr(s.domainName),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(id),
					RunId:      common.StringPtr(we.GetRunId()),
				},
				SignalName: common.StringPtr(signalName),
				Input:      signalInput,
			},
		}}, nil
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	signalSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(*we.RunId),
			},
		})
		s.Nil(err)
		history := historyResponse.History
		common.PrettyPrintHistory(history, s.logger)

		signalFailedEvent := history.Events[len(history.Events)-2]
		if *signalFailedEvent.EventType != workflow.EventTypeSignalExternalWorkflowExecutionFailed {
			s.logger.Info("Cancellaton not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		signalExternalWorkflowExecutionFailedEventAttributes := signalFailedEvent.SignalExternalWorkflowExecutionFailedEventAttributes
		s.Equal(int64(intiatedEventID), *signalExternalWorkflowExecutionFailedEventAttributes.InitiatedEventId)
		s.Equal(id, *signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.WorkflowId)
		s.Equal(we.RunId, signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.RunId)

		signalSentFailed = true
		break
	}

	s.True(signalSentFailed)

}

func (s *integrationSuite) TestSignalWithStartWorkflow() {
	id := "integration-signal-with-start-workflow-test"
	wt := "integration-signal-with-start-workflow-test-type"
	tl := "integration-signal-with-start-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start a workflow
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   common.StringPtr(strconv.Itoa(int(1))),
					ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			for _, event := range history.Events {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Send a signal
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	sRequest := &workflow.SignalWithStartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		SignalName:                          common.StringPtr(signalName),
		SignalInput:                         signalInput,
		Identity:                            common.StringPtr(identity),
	}
	resp, err := s.engine.SignalWithStartWorkflowExecution(createContext(), sRequest)
	s.Nil(err)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)

	// Terminate workflow execution
	err = s.engine.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr("test signal"),
		Details:  nil,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = []byte("signal to terminate input.")
	sRequest.SignalName = common.StringPtr(signalName)
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = common.StringPtr(id)

	resp, err = s.engine.SignalWithStartWorkflowExecution(createContext(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)

	// Send signal to not existed workflow
	id = "integration-signal-with-start-workflow-test-non-exist"
	signalName = "signal to non exist"
	signalInput = []byte("signal to non exist input.")
	sRequest.SignalName = common.StringPtr(signalName)
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = common.StringPtr(id)
	resp, err = s.engine.SignalWithStartWorkflowExecution(createContext(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)

	// Assert visibility is correct
	listOpenRequest := &workflow.ListOpenWorkflowExecutionsRequest{
		Domain:          common.StringPtr(s.domainName),
		MaximumPageSize: common.Int32Ptr(100),
		StartTimeFilter: &workflow.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &workflow.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr(id),
		},
	}
	listResp, err := s.engine.ListOpenWorkflowExecutions(createContext(), listOpenRequest)
	s.NoError(err)
	s.Equal(1, len(listResp.Executions))

	// Terminate workflow execution and assert visibility is correct
	err = s.engine.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr("kill workflow"),
		Details:  nil,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	for i := 0; i < 10; i++ { // retry
		listResp, err = s.engine.ListOpenWorkflowExecutions(createContext(), listOpenRequest)
		s.NoError(err)
		if len(listResp.Executions) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.Equal(0, len(listResp.Executions))

	listClosedRequest := &workflow.ListClosedWorkflowExecutionsRequest{
		Domain:          common.StringPtr(s.domainName),
		MaximumPageSize: common.Int32Ptr(100),
		StartTimeFilter: &workflow.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &workflow.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr(id),
		},
	}
	listClosedResp, err := s.engine.ListClosedWorkflowExecutions(createContext(), listClosedRequest)
	s.NoError(err)
	s.Equal(1, len(listClosedResp.Executions))
}

func (s *integrationSuite) TestTransientDecisionTimeout() {
	id := "integration-transient-decision-timeout-test"
	wt := "integration-transient-decision-timeout-test-type"
	tl := "integration-transient-decision-timeout-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(*we.RunId),
	}

	// decider logic
	workflowComplete := false
	failDecision := true
	signalCount := 0
	//var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if failDecision {
			failDecision = false
			return nil, nil, errors.New("Decider Panic")
		}

		// Count signals
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// First decision immediately fails and schedules a transient decision
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Now send a signal when transient decision is scheduled
	err = s.sendSignal(s.domainName, workflowExecution, "signalA", nil, identity)
	s.Nil(err, "failed to send signal to execution")

	// Drop decision task to cause a Decision Timeout
	_, err = poller.PollAndProcessDecisionTask(true, true)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Print history after dropping decision
	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(we.GetRunId()),
	})

	// Now process signal and complete workflow execution
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.Equal(1, signalCount)
	s.True(workflowComplete)
}

func (s *integrationSuite) TestRelayDecisionTimeout() {
	id := "integration-relay-decision-timeout-test"
	wt := "integration-relay-decision-timeout-test-type"
	tl := "integration-relay-decision-timeout-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)

	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(*we.RunId),
	}

	workflowComplete, isFirst := false, true
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		if isFirst {
			isFirst = false
			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRecordMarker),
				RecordMarkerDecisionAttributes: &workflow.RecordMarkerDecisionAttributes{
					MarkerName: common.StringPtr("test-marker"),
				},
			}}, nil
		}
		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType:                                common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// First decision task complete with a marker decision, and request to relay decision (immediately return a new decision task)
	_, newTask, err := poller.PollAndProcessDecisionTaskWithAttemptAndRetryAndForceNewDecision(false, false, false, false, 0, 3, true)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(newTask)
	s.NotNil(newTask.DecisionTask)

	time.Sleep(time.Second * 2) // wait 2s for relay decision to timeout
	decisionTaskTimeout := false
	for i := 0; i < 3; i++ {
		events := s.getHistory(s.domainName, workflowExecution)
		if len(events) >= 8 {
			s.Equal(workflow.EventTypeDecisionTaskTimedOut, events[7].GetEventType())
			s.Equal(workflow.TimeoutTypeStartToClose, events[7].DecisionTaskTimedOutEventAttributes.GetTimeoutType())
			decisionTaskTimeout = true
			break
		}
		time.Sleep(time.Second)
	}
	// verify relay decision task timeout
	s.True(decisionTaskTimeout)
	s.printWorkflowHistory(s.domainName, workflowExecution)

	// Now complete workflow
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, int64(1))
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.True(workflowComplete)
}

func (s *integrationSuite) TestTaskProcessingProtectionForRateLimitError() {
	id := "integration-task-processing-protection-for-rate-limit-error-test"
	wt := "integration-task-processing-protection-for-rate-limit-error-test-type"
	tl := "integration-task-processing-protection-for-rate-limit-error-test-tasklist"
	identity := "worker1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:    common.StringPtr(uuid.New()),
		Domain:       common.StringPtr(s.domainName),
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(601),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(600),
		Identity:                            common.StringPtr(identity),
	}

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", *we.RunId)
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(*we.RunId),
	}

	// decider logic
	workflowComplete := false
	signalCount := 0
	createUserTimer := false
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, h *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !createUserTimer {
			createUserTimer = true

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartTimer),
				StartTimerDecisionAttributes: &workflow.StartTimerDecisionAttributes{
					TimerId:                   common.StringPtr("timer-id-1"),
					StartToFireTimeoutSeconds: common.Int64Ptr(5),
				},
			}}, nil
		}

		// Count signals
		for _, event := range h.Events[previousStartedEventID:] {
			if event.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
				signalCount++
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Process first decision to create user timer
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Send one signal to create a new decision
	for i := 0; i < 1; i++ {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		s.Nil(s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity))
	}

	// Drop decision to cause all events to be buffered from now on
	_, err = poller.PollAndProcessDecisionTask(false, true)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Buffered Signals
	for i := 1; i < 101; i++ {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		s.Nil(s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity))
	}

	// Rate limitted signals
	for i := 0; i < 10; i++ {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		signalErr := s.sendSignal(s.domainName, workflowExecution, "SignalName", buf.Bytes(), identity)
		s.NotNil(signalErr)
		s.Equal(history.ErrBufferedEventsLimitExceeded, signalErr)
	}

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTaskWithAttempt(true, false, false, false, 1)
	s.logger.Infof("PollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.printWorkflowHistory(s.domainName, workflowExecution)

	s.True(workflowComplete)
	s.Equal(101, signalCount)
}

func (s *integrationSuite) getHistory(domain string, execution *workflow.WorkflowExecution) []*workflow.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
		Domain:          common.StringPtr(domain),
		Execution:       execution,
		MaximumPageSize: common.Int32Ptr(5), // Use small page size to force pagination code path
	})
	s.Nil(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain:        common.StringPtr(domain),
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.Nil(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *integrationSuite) getHistoryFrom(domainID string, execution workflow.WorkflowExecution,
	firstEventID, nextEventID int64) ([]*workflow.HistoryEvent, error) {

	getRequest := &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:     domainID,
		Execution:    execution,
		FirstEventID: firstEventID,
		NextEventID:  nextEventID,
		PageSize:     100,
	}
	resp, err := s.HistoryMgr.GetWorkflowExecutionHistory(getRequest)
	if err != nil {
		return nil, err
	}

	return resp.History.Events, nil
}

func (s *integrationSuite) sendSignal(domainName string, execution *workflow.WorkflowExecution, signalName string,
	input []byte, identity string) error {
	return s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
		Domain:            common.StringPtr(domainName),
		WorkflowExecution: execution,
		SignalName:        common.StringPtr(signalName),
		Input:             input,
		Identity:          common.StringPtr(identity),
	})
}

func (s *integrationSuite) printWorkflowHistory(domain string, execution *workflow.WorkflowExecution) {
	events := s.getHistory(domain, execution)
	history := &workflow.History{}
	history.Events = events
	common.PrettyPrintHistory(history, s.logger)
}
