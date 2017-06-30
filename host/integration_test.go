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
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	tchannel "github.com/uber/tchannel-go"

	"bytes"
	"encoding/binary"
	"strconv"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
)

var (
	integration = flag.Bool("integration", true, "run integration tests")
)

const (
	testNumberOfHistoryShards = 4
	testNumberOfHistoryHosts  = 1
)

type (
	integrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		domainName        string
		foreignDomainName string
		host              Cadence
		ch                *tchannel.Channel
		engine            frontend.Client
		logger            bark.Logger
		suite.Suite
		persistence.TestBase
	}

	decisionTaskHandler func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision)
	activityTaskHandler func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, takeToken []byte) ([]byte, bool, error)

	taskPoller struct {
		engine          frontend.Client
		domain          string
		taskList        *workflow.TaskList
		identity        string
		decisionHandler decisionTaskHandler
		activityHandler activityTaskHandler
		logger          bark.Logger
	}
)

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

	s.ch, _ = tchannel.NewChannel("cadence-integration-test", nil)
}

func (s *integrationSuite) TearDownSuite() {
}

func (s *integrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	options := persistence.TestBaseOptions{}
	options.ClusterHost = "127.0.0.1"
	options.DropKeySpace = true
	options.SchemaDir = ".."
	s.SetupWorkflowStoreWithOptions(options)

	s.setupShards()

	s.host = NewCadence(s.MetadataManager, s.ShardMgr, s.HistoryMgr, s.ExecutionMgrFactory, s.TaskMgr,
		s.VisibilityMgr, testNumberOfHistoryShards, testNumberOfHistoryHosts, s.logger)

	s.host.Start()
	s.engine, _ = frontend.NewClient(s.ch, s.host.FrontendAddress())
	s.domainName = "integration-test-domain"
	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Name:        s.domainName,
		Status:      persistence.DomainStatusRegistered,
		Description: "Test domain for integration test",
		Retention:   1,
		EmitMetric:  false,
	})
	s.foreignDomainName = "integration-foreign-test-domain"
	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Name:        s.foreignDomainName,
		Status:      persistence.DomainStatusRegistered,
		Description: "Test foreign domain for integration test",
		Retention:   1,
		EmitMetric:  false,
	})
}

func (s *integrationSuite) TearDownTest() {
	s.host.Stop()
	s.host = nil
	s.TearDownWorkflowStore()
}

func (s *integrationSuite) TestIntegrationStartWorkflowExecution() {
	id := "integration-start-workflow-test"
	wt := "integration-start-workflow-test-type"
	tl := "integration-start-workflow-test-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we0, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	we1, err1 := s.engine.StartWorkflowExecution(request)
	s.Nil(err1)
	s.Equal(we0.GetRunId(), we1.GetRunId())

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
	we2, err2 := s.engine.StartWorkflowExecution(newRequest)
	s.NotNil(err2)
	s.IsType(workflow.NewWorkflowExecutionAlreadyStartedError(), err2)
	log.Infof("Unable to start workflow execution: %v", err2.Error())
	s.Nil(we2)
}

func (s *integrationSuite) TestTerminateWorkflow() {
	id := "integration-terminate-workflow-test"
	wt := "integration-terminate-workflow-test-type"
	tl := "integration-terminate-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	terminateReason := "terminate reason."
	terminateDetails := []byte("terminate details.")
	err = s.engine.TerminateWorkflowExecution(&workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
		Reason:   common.StringPtr(terminateReason),
		Details:  terminateDetails,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	executionTerminated := false
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-1]
		if lastEvent.GetEventType() != workflow.EventType_WorkflowExecutionTerminated {
			s.logger.Warnf("Execution not terminated yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
		s.Equal(terminateReason, terminateEventAttributes.GetReason())
		s.Equal(terminateDetails, terminateEventAttributes.GetDetails())
		s.Equal(identity, terminateEventAttributes.GetIdentity())
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

		newExecution, err := s.engine.StartWorkflowExecution(request)
		if err != nil {
			s.logger.Warnf("Start New Execution failed. Error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue StartNewExecutionLoop
		}

		s.logger.Infof("New Execution Started with the same ID.  WorkflowID: %v, RunID: %v", id,
			newExecution.GetRunId())
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

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(10)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	expectedActivity := int32(1)
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++

		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	for i := 0; i < 10; i++ {
		err := poller.pollAndProcessDecisionTask(false, false)
		s.logger.Infof("pollAndProcessDecisionTask: %v", err)
		s.Nil(err)
		err = poller.pollAndProcessActivityTask(false)
		s.logger.Infof("pollAndProcessActivityTask: %v", err)
		s.Nil(err)
	}

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
}

func (p *taskPoller) pollAndProcessDecisionTask(dumpHistory bool, dropTask bool) error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.engine.PollForDecisionTask(&workflow.PollForDecisionTaskRequest{
			Domain:   common.StringPtr(p.domain),
			TaskList: p.taskList,
			Identity: common.StringPtr(p.identity),
		})

		if err1 == history.ErrDuplicate {
			p.logger.Info("Duplicate Decision task: Polling again.")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.logger.Info("Empty Decision task: Polling again.")
			continue retry
		}

		history := response.GetHistory()
		if history == nil {
			p.logger.Fatal("History is nil")
		}

		events := history.GetEvents()
		if events == nil || len(events) == 0 {
			p.logger.Fatalf("History Events are empty: %v", events)
		}

		nextPageToken := response.GetNextPageToken()
		for nextPageToken != nil {
			resp, err2 := p.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
				Domain:        common.StringPtr(p.domain),
				Execution:     response.GetWorkflowExecution(),
				NextPageToken: nextPageToken,
			})

			if err2 != nil {
				return err2
			}

			events = append(events, resp.GetHistory().GetEvents()...)
			nextPageToken = resp.GetNextPageToken()
		}

		if dropTask {
			p.logger.Info("Dropping Decision task: ")
			return nil
		}

		if dumpHistory {
			common.PrettyPrintHistory(response.GetHistory(), p.logger)
		}

		context, decisions := p.decisionHandler(response.GetWorkflowExecution(), response.GetWorkflowType(),
			response.GetPreviousStartedEventId(), response.GetStartedEventId(), response.GetHistory())

		return p.engine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        response.GetTaskToken(),
			Identity:         common.StringPtr(p.identity),
			ExecutionContext: context,
			Decisions:        decisions,
		})
	}

	return matching.ErrNoTasks
}

func (p *taskPoller) pollAndProcessActivityTask(dropTask bool) error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.engine.PollForActivityTask(&workflow.PollForActivityTaskRequest{
			Domain:   common.StringPtr(p.domain),
			TaskList: p.taskList,
			Identity: common.StringPtr(p.identity),
		})

		if err1 == history.ErrDuplicate {
			p.logger.Info("Duplicate Activity task: Polling again.")
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || len(response.TaskToken) == 0 {
			p.logger.Info("Empty Activity task: Polling again.")
			return nil
		}

		if dropTask {
			p.logger.Info("Dropping Activity task: ")
			return nil
		}
		p.logger.Debugf("Received Activity task: %v", response)

		result, cancel, err2 := p.activityHandler(response.GetWorkflowExecution(), response.GetActivityType(), response.GetActivityId(),
			response.GetStartedEventId(), response.GetInput(), response.GetTaskToken())
		if cancel {
			p.logger.Info("Executing RespondActivityTaskCanceled")
			return p.engine.RespondActivityTaskCanceled(&workflow.RespondActivityTaskCanceledRequest{
				TaskToken: response.GetTaskToken(),
				Details:   []byte("details"),
				Identity:  common.StringPtr(p.identity),
			})
		}

		if err2 != nil {
			return p.engine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
				TaskToken: response.GetTaskToken(),
				Reason:    common.StringPtr(err2.Error()),
				Identity:  common.StringPtr(p.identity),
			})
		}

		return p.engine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
			TaskToken: response.GetTaskToken(),
			Identity:  common.StringPtr(p.identity),
			Result_:   result,
		})
	}

	return matching.ErrNoTasks
}

func (s *integrationSuite) TestDecisionAndActivityTimeoutsWorkflow() {
	id := "interation-timeouts-workflow-test"
	wt := "interation-timeouts-workflow-test-type"
	tl := "interation-timeouts-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(4)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		s.logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		s.logger.Infof("Activity ID: %v", activityID)
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	for i := 0; i < 8; i++ {
		dropDecisionTask := (i%2 == 0)
		s.logger.Infof("Calling Decision Task: %d", i)
		err := poller.pollAndProcessDecisionTask(false, dropDecisionTask)
		s.True(err == nil || err == matching.ErrNoTasks)
		if !dropDecisionTask {
			s.logger.Infof("Calling Activity Task: %d", i)
			err = poller.pollAndProcessActivityTask(i%4 == 0)
			s.True(err == nil || err == matching.ErrNoTasks)
		}
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", we.GetRunId())

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "integration-heartbeat-test"
	wt := "integration-heartbeat-test-type"
	tl := "integration-heartbeat-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		s.logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
			_, err := s.engine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: []byte("details")})
			s.Nil(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	err := poller.pollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.pollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", we.GetRunId())

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
	s.True(activityExecutedCount == 1)
}

func (s *integrationSuite) TestActivityHeartBeatWorkflow_Timeout() {
	id := "integration-heartbeat-timeout-test"
	wt := "integration-heartbeat-timeout-test-type"
	tl := "integration-heartbeat-timeout-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {

		s.logger.Infof("Calling DecisionTask Handler: %d, %d.", activityCounter, activityCount)

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		// Timing out more than HB time.
		time.Sleep(2 * time.Second)
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	err := poller.pollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	err = poller.pollAndProcessActivityTask(false)

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", we.GetRunId())

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
}

func (s *integrationSuite) TestSequential_UserTimers() {
	id := "interation-sequential-user-timers-test"
	wt := "interation-sequential-user-timers-test-type"
	tl := "interation-sequential-user-timers-test-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	timerCount := int32(4)
	timerCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if timerCounter < timerCount {
			timerCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, timerCounter))
			return []byte(strconv.Itoa(int(timerCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_StartTimer),
				StartTimerDecisionAttributes: &workflow.StartTimerDecisionAttributes{
					TimerId:                   common.StringPtr(fmt.Sprintf("timer-id-%d", timerCounter)),
					StartToFireTimeoutSeconds: common.Int64Ptr(1),
				},
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(timerCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: nil,
		logger:          s.logger,
	}

	for i := 0; i < 4; i++ {
		err := poller.pollAndProcessDecisionTask(false, false)
		s.logger.Info("pollAndProcessDecisionTask: completed")
		s.Nil(err)
	}

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
}

func (s *integrationSuite) TestActivityCancelation() {
	id := "integration-activity-cancelation-test"
	wt := "integration-activity-cancelation-test-type"
	tl := "integration-activity-cancelation-test-tasklist"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		if requestCancellation {
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
				RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
					ActivityId: common.StringPtr(strconv.Itoa(int(activityCounter))),
				},
			}}
		}

		s.logger.Info("Completing Workflow.")

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	activityExecutedCount := 0
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		for i := 0; i < 10; i++ {
			s.logger.Infof("Heartbeating for activity: %s, count: %d", activityID, i)
			response, err := s.engine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
				TaskToken: taskToken, Details: []byte("details")})
			if response.GetCancelRequested() {
				return []byte("Activity Cancelled."), true, nil
			}
			s.Nil(err)
			time.Sleep(10 * time.Millisecond)
		}
		activityExecutedCount++
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	err := poller.pollAndProcessDecisionTask(false, false)
	s.True(err == nil || err == matching.ErrNoTasks)

	cancelCh := make(chan struct{})

	go func() {
		s.logger.Info("Trying to cancel the task in a different thread.")
		scheduleActivity = false
		requestCancellation = true
		err := poller.pollAndProcessDecisionTask(false, false)
		s.True(err == nil || err == matching.ErrNoTasks)
		cancelCh <- struct{}{}
	}()

	err = poller.pollAndProcessActivityTask(false)
	s.True(err == nil || err == matching.ErrNoTasks)

	<-cancelCh
	s.logger.Infof("Waiting for workflow to complete: RunId: %v", we.GetRunId())
}

func (s *integrationSuite) TestSignalWorkflow() {
	id := "interation-signal-workflow-test"
	wt := "interation-signal-workflow-test-type"
	tl := "interation-signal-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
	taskList.Name = common.StringPtr(tl)

	// Send a signal to non-existant workflow
	err0 := s.engine.SignalWorkflowExecution(&workflow.SignalWorkflowExecutionRequest{
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		} else if previousStartedEventID > 0 {
			for _, event := range history.GetEvents()[previousStartedEventID:] {
				if event.GetEventType() == workflow.EventType_WorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*workflow.Decision{}
				}
			}
		}

		workflowComplete = true
		return nil, []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	// activity handler
	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	// Make first decision to schedule activity
	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	err = s.engine.SignalWorkflowExecution(&workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
		SignalName: common.StringPtr(signalName),
		Input:      signalInput,
		Identity:   common.StringPtr(identity),
	})
	s.Nil(err)

	// Process signal in decider
	err = poller.pollAndProcessDecisionTask(true, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = []byte("another signal input.")
	err = s.engine.SignalWorkflowExecution(&workflow.SignalWorkflowExecutionRequest{
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
	err = poller.pollAndProcessDecisionTask(true, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	// Terminate workflow execution
	err = s.engine.TerminateWorkflowExecution(&workflow.TerminateWorkflowExecutionRequest{
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
	err = s.engine.SignalWorkflowExecution(&workflow.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
		SignalName: common.StringPtr("failed signal 1."),
		Input:      nil,
		Identity:   common.StringPtr(identity),
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *integrationSuite) TestContinueAsNewWorkflow() {
	id := "interation-continue-as-new-workflow-test"
	wt := "interation-continue-as-new-workflow-test-type"
	tl := "interation-continue-as-new-workflow-test-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ContinueAsNewWorkflowExecution),
				ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType: workflowType,
					TaskList:     &workflow.TaskList{Name: &tl},
					Input:        buf.Bytes(),
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
				},
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		logger:          s.logger,
	}

	for i := 0; i < 10; i++ {
		err := poller.pollAndProcessDecisionTask(false, false)
		s.logger.Infof("pollAndProcessDecisionTask: %v", err)
		s.Nil(err, i)
	}

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
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

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
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

	_, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	request = &workflow.StartWorkflowExecutionRequest{
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

	_, err1 := s.engine.StartWorkflowExecution(request)
	s.Nil(err1)

	// Now complete one of the executions
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		return []byte{}, []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: nil,
		logger:          s.logger,
	}

	err2 := poller.pollAndProcessDecisionTask(false, false)
	s.Nil(err2)

	startFilter := workflow.NewStartTimeFilter()
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

	closedCount := 0

ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(&workflow.ListClosedWorkflowExecutionsRequest{
			Domain:          common.StringPtr(s.domainName),
			MaximumPageSize: common.Int32Ptr(100),
			StartTimeFilter: startFilter,
		})
		s.Nil(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.logger.Info("Closed WorkflowExecution is not yet visibile")
			time.Sleep(100 * time.Millisecond)
			continue ListClosedLoop
		}
		break ListClosedLoop
	}
	s.Equal(1, closedCount)

	resp, err4 := s.engine.ListOpenWorkflowExecutions(&workflow.ListOpenWorkflowExecutionsRequest{
		Domain:          common.StringPtr(s.domainName),
		MaximumPageSize: common.Int32Ptr(100),
		StartTimeFilter: startFilter,
	})
	s.Nil(err4)
	s.Equal(1, len(resp.Executions))
}

func (s *integrationSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "integration-request-cancel-workflow-test"
	wt := "integration-request-cancel-workflow-test-type"
	tl := "integration-request-cancel-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelWorkflowExecution),
			CancelWorkflowExecutionDecisionAttributes: &workflow.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = poller.pollAndProcessActivityTask(false)
	s.logger.Infof("pollAndProcessActivityTask: %v", err)
	s.Nil(err)

	err = s.engine.RequestCancelWorkflowExecution(&workflow.RequestCancelWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
	})
	s.Nil(err)

	err = s.engine.RequestCancelWorkflowExecution(&workflow.RequestCancelWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
			RunId:      common.StringPtr(we.GetRunId()),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.CancellationAlreadyRequestedError{}, err)

	err = poller.pollAndProcessDecisionTask(true, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 3; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-1]
		if lastEvent.GetEventType() != workflow.EventType_WorkflowExecutionCanceled {
			s.logger.Warnf("Execution not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		s.Equal("Cancelled", string(cancelledEventAttributes.GetDetails()))
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

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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
	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

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
	we2, err0 := s.engine.StartWorkflowExecution(foreignRequest)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution on foreign domain: %v,  response: %v \n", s.foreignDomainName, we2.GetRunId())

	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelExternalWorkflowExecution),
			RequestCancelExternalWorkflowExecutionDecisionAttributes: &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     common.StringPtr(s.foreignDomainName),
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we2.GetRunId()),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	foreginWorkflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	foreignDtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		foreginWorkflowComplete = true
		return []byte(strconv.Itoa(int(foreignActivityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelWorkflowExecution),
			CancelWorkflowExecutionDecisionAttributes: &workflow.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
			},
		}}
	}

	foreignPoller := &taskPoller{
		engine:          s.engine,
		domain:          s.foreignDomainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: foreignDtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	// Start both current and foreign workflows to make some progress.
	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = foreignPoller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	err = foreignPoller.pollAndProcessActivityTask(false)
	s.logger.Infof("foreign pollAndProcessActivityTask: %v", err)
	s.Nil(err)

	// Cancel the foreign workflow with this decision request.
	err = poller.pollAndProcessDecisionTask(true, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	cancellationSent := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-2]
		if lastEvent.GetEventType() != workflow.EventType_ExternalWorkflowExecutionCancelRequested {
			s.logger.Info("Cancellaton still not sent.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		externalWorkflowExecutionCancelRequestedEvent := lastEvent.GetExternalWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), externalWorkflowExecutionCancelRequestedEvent.GetInitiatedEventId())
		s.Equal(id, externalWorkflowExecutionCancelRequestedEvent.GetWorkflowExecution().GetWorkflowId())
		s.Equal(we2.GetRunId(), externalWorkflowExecutionCancelRequestedEvent.GetWorkflowExecution().GetRunId())

		cancellationSent = true
		break
	}

	s.True(cancellationSent)

	// Accept cancellation.
	err = foreignPoller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("foreign pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.foreignDomainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we2.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-1]
		if lastEvent.GetEventType() != workflow.EventType_WorkflowExecutionCanceled {
			s.logger.Warnf("Execution not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		s.Equal("Cancelled", string(cancelledEventAttributes.GetDetails()))
		executionCancelled = true

		// Find cancel requested event and verify it.
		var cancelRequestEvent *workflow.HistoryEvent
		for _, x := range history.GetEvents() {
			if x.GetEventType() == workflow.EventType_WorkflowExecutionCancelRequested {
				cancelRequestEvent = x
			}
		}

		s.NotNil(cancelRequestEvent)
		cancelRequestEventAttributes := cancelRequestEvent.GetWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), cancelRequestEventAttributes.GetExternalInitiatedEventId())
		s.Equal(id, cancelRequestEventAttributes.GetExternalWorkflowExecution().GetWorkflowId())
		s.Equal(we.GetRunId(), cancelRequestEventAttributes.GetExternalWorkflowExecution().GetRunId())

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

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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
	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelExternalWorkflowExecution),
			RequestCancelExternalWorkflowExecutionDecisionAttributes: &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     common.StringPtr(s.foreignDomainName),
				WorkflowId: common.StringPtr("workflow_not_exist"),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	// Start both current and foreign workflows to make some progress.
	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	// Cancel the foreign workflow with this decision request.
	err = poller.pollAndProcessDecisionTask(true, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)

	cancellationSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-2]
		if lastEvent.GetEventType() != workflow.EventType_RequestCancelExternalWorkflowExecutionFailed {
			s.logger.Info("Cancellaton not cancelled yet.")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		requestCancelExternalWorkflowExecutionFailedEvetn := lastEvent.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()
		s.Equal(int64(intiatedEventID), requestCancelExternalWorkflowExecutionFailedEvetn.GetInitiatedEventId())
		s.Equal("workflow_not_exist", requestCancelExternalWorkflowExecutionFailedEvetn.GetWorkflowExecution().GetWorkflowId())
		s.Equal(we.GetRunId(), requestCancelExternalWorkflowExecutionFailedEvetn.GetWorkflowExecution().GetRunId())

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

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false
	activityCount := int32(4)
	activityCounter := int32(0)

	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
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
			}}
		}

		s.logger.Info("Completing Workflow.")

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result_: []byte("Done."),
			},
		}}
	}

	atHandler := func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte, taskToken []byte) ([]byte, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		s.logger.Infof("Activity ID: %v", activityID)
		return []byte("Activity Result."), false, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	upgradeRollbackStep := 2
	testDecisionPollFailStep := 3

	for i := 0; i < int(activityCount)+1; i++ {

		if i == upgradeRollbackStep {
			// force history to be persisted with a higher version
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion + 1)
			persistence.SetDefaultHistoryVersion(prevMaxVersion + 1)
		}

		s.logger.Infof("Calling Decision Task: %d", i)
		err := poller.pollAndProcessDecisionTask(false, false)

		if i == testDecisionPollFailStep {
			// make sure we get an error due to history version
			// incompatibility i.e. old code, new history version
			s.NotNil(err)
			// reset the supported versions, this should make subsequent
			// polls / activities to succeed
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion + 1)
			persistence.SetDefaultHistoryVersion(prevMaxVersion + 1)
			continue
		}

		s.True(err == nil || err == matching.ErrNoTasks)
		s.logger.Infof("Calling Activity Task: %d", i)
		err = poller.pollAndProcessActivityTask(false)
		s.True(err == nil || err == matching.ErrNoTasks)

		if i == upgradeRollbackStep {
			// now revert the versions back to the original
			// this simulates a rollback of code deployment
			// and a persisted history with future version
			persistence.SetMaxSupportedHistoryVersion(prevMaxVersion)
			persistence.SetDefaultHistoryVersion(prevMaxVersion)
		}
	}

	s.logger.Infof("Waiting for workflow to complete: RunId: %v", we.GetRunId())

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true, false))
	s.True(workflowComplete)
}

func (s *integrationSuite) TestChildWorkflowExecution() {
	parentID := "integration-child-workflow-parent-test"
	childID := "integration-child-workflow-child-test"
	wtParent := "integration-child-workflow-test-parent-type"
	wtChild := "integration-child-workflow-test-child-type"
	tl := "integration-child-workflow-test-tasklist"
	identity := "worker1"

	parentWorkflowType := workflow.NewWorkflowType()
	parentWorkflowType.Name = common.StringPtr(wtParent)

	childWorkflowType := workflow.NewWorkflowType()
	childWorkflowType.Name = common.StringPtr(wtChild)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	// decider logic
	workflowComplete := false
	childComplete := false
	childExecutionStarted := false
	childData := int32(1)
	var startedEvent *workflow.HistoryEvent
	var completedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		s.logger.Infof("Processing decision task for WorkflowID: %v", execution.GetWorkflowId())

		// Child Decider Logic
		if execution.GetWorkflowId() == childID {
			childComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result_: []byte("Child Done."),
				},
			}}
		}

		// Parent Decider Logic
		if execution.GetWorkflowId() == parentID {
			if !childExecutionStarted {
				s.logger.Info("Starting child execution.")
				childExecutionStarted = true
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, childData))

				return nil, []*workflow.Decision{{
					DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_StartChildWorkflowExecution),
					StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
						Domain:       common.StringPtr(s.domainName),
						WorkflowId:   common.StringPtr(childID),
						WorkflowType: childWorkflowType,
						TaskList:     taskList,
						Input:        buf.Bytes(),
						ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(200),
						TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
						ChildPolicy:                         workflow.ChildPolicyPtr(workflow.ChildPolicy_TERMINATE),
						Control:                             nil,
					},
				}}
			} else if previousStartedEventID > 0 {
				for _, event := range history.GetEvents()[previousStartedEventID:] {
					if event.GetEventType() == workflow.EventType_ChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*workflow.Decision{}
					}

					if event.GetEventType() == workflow.EventType_ChildWorkflowExecutionCompleted {
						completedEvent = event
						workflowComplete = true
						return nil, []*workflow.Decision{{
							DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
							CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
								Result_: []byte("Done."),
							},
						}}
					}
				}
			}
		}

		return nil, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		logger:          s.logger,
	}

	// Make first decision to start child execution
	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(startedEvent)

	// Process Child Execution and complete it
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childComplete)

	// Process ChildExecution completed event and complete parent execution
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.domainName, completedAttributes.GetDomain())
	s.Equal(childID, completedAttributes.GetWorkflowExecution().GetWorkflowId())
	s.Equal(wtChild, completedAttributes.GetWorkflowType().GetName())
	s.Equal([]byte("Child Done."), completedAttributes.GetResult_())

	s.logger.Info("Parent Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(parentID),
		RunId:      common.StringPtr(we.GetRunId()),
	})

	s.logger.Info("Child Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName,
		startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution())
}

func (s *integrationSuite) TestChildWorkflowWithContinueAsNew() {
	parentID := "integration-child-workflow-with-continue-as-new-parent-test"
	childID := "integration-child-workflow-with-continue-as-new-child-test"
	wtParent := "integration-child-workflow-with-continue-as-new-test-parent-type"
	wtChild := "integration-child-workflow-with-continue-as-new-test-child-type"
	tl := "integration-child-workflow-with-continue-as-new-test-tasklist"
	identity := "worker1"

	parentWorkflowType := workflow.NewWorkflowType()
	parentWorkflowType.Name = common.StringPtr(wtParent)

	childWorkflowType := workflow.NewWorkflowType()
	childWorkflowType.Name = common.StringPtr(wtChild)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)
	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	// decider logic
	workflowComplete := false
	childComplete := false
	childExecutionStarted := false
	childData := int32(1)
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var startedEvent *workflow.HistoryEvent
	var completedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision) {
		s.logger.Infof("Processing decision task for WorkflowID: %v", execution.GetWorkflowId())

		// Child Decider Logic
		if execution.GetWorkflowId() == childID {
			if continueAsNewCounter < continueAsNewCount {
				continueAsNewCounter++
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

				return []byte(strconv.Itoa(int(continueAsNewCounter))), []*workflow.Decision{{
					DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ContinueAsNewWorkflowExecution),
					ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
						WorkflowType: childWorkflowType,
						TaskList:     taskList,
						Input:        buf.Bytes(),
						ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
						TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
					},
				}}
			}

			childComplete = true
			return nil, []*workflow.Decision{{
				DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
					Result_: []byte("Child Done."),
				},
			}}
		}

		// Parent Decider Logic
		if execution.GetWorkflowId() == parentID {
			if !childExecutionStarted {
				s.logger.Info("Starting child execution.")
				childExecutionStarted = true
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, childData))

				return nil, []*workflow.Decision{{
					DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_StartChildWorkflowExecution),
					StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
						Domain:       common.StringPtr(s.domainName),
						WorkflowId:   common.StringPtr(childID),
						WorkflowType: childWorkflowType,
						TaskList:     taskList,
						Input:        buf.Bytes(),
						ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(200),
						TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
						ChildPolicy:                         workflow.ChildPolicyPtr(workflow.ChildPolicy_TERMINATE),
						Control:                             nil,
					},
				}}
			} else if previousStartedEventID > 0 {
				for _, event := range history.GetEvents()[previousStartedEventID:] {
					if event.GetEventType() == workflow.EventType_ChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*workflow.Decision{}
					}

					if event.GetEventType() == workflow.EventType_ChildWorkflowExecutionCompleted {
						completedEvent = event
						workflowComplete = true
						return nil, []*workflow.Decision{{
							DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
							CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
								Result_: []byte("Done."),
							},
						}}
					}
				}
			}
		}

		return nil, nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		domain:          s.domainName,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		logger:          s.logger,
	}

	// Make first decision to start child execution
	err := poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(startedEvent)

	// Process all generations of child executions
	for i := 0; i < 10; i++ {
		err = poller.pollAndProcessDecisionTask(false, false)
		s.logger.Infof("pollAndProcessDecisionTask: %v", err)
		s.Nil(err)
		s.False(childComplete)
	}

	// Process Child Execution final decision to complete it
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.True(childComplete)

	// Process ChildExecution completed event and complete parent execution
	err = poller.pollAndProcessDecisionTask(false, false)
	s.logger.Infof("pollAndProcessDecisionTask: %v", err)
	s.Nil(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.domainName, completedAttributes.GetDomain())
	s.Equal(childID, completedAttributes.GetWorkflowExecution().GetWorkflowId())
	s.NotEqual(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
		completedAttributes.GetWorkflowExecution().GetRunId())
	s.Equal(wtChild, completedAttributes.GetWorkflowType().GetName())
	s.Equal([]byte("Child Done."), completedAttributes.GetResult_())

	s.logger.Info("Parent Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName, &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(parentID),
		RunId:      common.StringPtr(we.GetRunId()),
	})

	s.logger.Info("Child Workflow Execution History: ")
	s.printWorkflowHistory(s.domainName,
		startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution())
}

func (s *integrationSuite) TestWorkflowTimeout() {
	startTime := time.Now().UnixNano()

	id := "integration-workflow-timeout-test"
	wt := "integration-workflow-timeout-type"
	tl := "integration-workflow-timeout-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
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

	we, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	s.logger.Infof("StartWorkflowExecution: response: %v \n", we.GetRunId())

	workflowComplete := false

GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain: common.StringPtr(s.domainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(id),
				RunId:      common.StringPtr(we.GetRunId()),
			},
		})
		s.Nil(err)
		history := historyResponse.GetHistory()
		common.PrettyPrintHistory(history, s.logger)

		lastEvent := history.GetEvents()[len(history.GetEvents())-1]
		if lastEvent.GetEventType() != workflow.EventType_WorkflowExecutionTimedOut {
			s.logger.Warnf("Execution not timedout yet.")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		s.Equal(workflow.TimeoutType_START_TO_CLOSE, timeoutEventAttributes.GetTimeoutType())
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)

	startFilter := workflow.NewStartTimeFilter()
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

	closedCount := 0
ListClosedLoop:
	for i := 0; i < 10; i++ {
		resp, err3 := s.engine.ListClosedWorkflowExecutions(&workflow.ListClosedWorkflowExecutionsRequest{
			Domain:          common.StringPtr(s.domainName),
			MaximumPageSize: common.Int32Ptr(100),
			StartTimeFilter: startFilter,
		})
		s.Nil(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.logger.Info("Closed WorkflowExecution is not yet visibile")
			time.Sleep(100 * time.Millisecond)
			continue ListClosedLoop
		}
		break ListClosedLoop
	}
	s.Equal(1, closedCount)
}

func (s *integrationSuite) setupShards() {
	// shard 0 is always created, we create additional shards if needed
	for shardID := 1; shardID < testNumberOfHistoryShards; shardID++ {
		err := s.CreateShard(shardID, "", 0)
		if err != nil {
			s.logger.WithField("error", err).Fatal("Failed to create shard")
		}
	}
}

func (s *integrationSuite) printWorkflowHistory(domain string, execution *workflow.WorkflowExecution) {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
		Domain:          common.StringPtr(domain),
		Execution:       execution,
		MaximumPageSize: common.Int32Ptr(10),
	})
	s.Nil(err)

	history := historyResponse.GetHistory()
	events := historyResponse.GetHistory().GetEvents()
	for historyResponse.GetNextPageToken() != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(&workflow.GetWorkflowExecutionHistoryRequest{
			Domain:        common.StringPtr(domain),
			Execution:     execution,
			NextPageToken: historyResponse.GetNextPageToken(),
		})
		s.Nil(err)
		events = append(events, historyResponse.GetHistory().GetEvents()...)
	}
	history.Events = events
	common.PrettyPrintHistory(history, s.logger)
}
