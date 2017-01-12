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
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	tchannel "github.com/uber/tchannel-go"

	"bytes"
	"encoding/binary"
	"strconv"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/frontend"
	"code.uber.internal/devexp/minions/common"
	wf "code.uber.internal/devexp/minions/workflow"
)

var (
	integration = flag.Bool("integration", false, "run integration tests")
)

type (
	integrationSuite struct {
		host   Cadence
		ch     *tchannel.Channel
		engine frontend.Client
		logger bark.Logger
		suite.Suite
		wf.TestBase
	}

	decisionTaskHandler func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision)
	activityTaskHandler func(execution *workflow.WorkflowExecution, activityType *workflow.ActivityType,
		activityID string, startedEventID int64, input []byte) ([]byte, error)

	taskPoller struct {
		engine          frontend.Client
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

	s.SetupWorkflowStore()
	logger := log.New()
	logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)

	s.ch, _ = tchannel.NewChannel("cadence-integration-test", nil)
}

func (s *integrationSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *integrationSuite) SetupTest() {
	s.ClearTransferQueue()
	s.host = NewCadence(s.WorkflowMgr, s.TaskMgr, s.logger)
	s.host.Start()
	s.engine, _ = frontend.NewClient(s.ch, s.host.FrontendAddress())
}

func (s *integrationSuite) TearDownTest() {
	s.host.Stop()
	s.host = nil
}

func (s *integrationSuite) TestStartWorkflowExecution() {
	id := "integration-start-workflow-test"
	wt := "integration-start-workflow-test-type"
	tl := "integration-start-workflow-test-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	_, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	we1, err1 := s.engine.StartWorkflowExecution(request)
	s.NotNil(err1)
	s.IsType(workflow.NewWorkflowExecutionAlreadyStartedError(), err1)
	log.Infof("Start workflow execution failed with error: %v", err1.Error())
	s.Nil(we1)
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
		activityID string, startedEventID int64, input []byte) ([]byte, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.GetName())
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivity), id)
		s.logger.Errorf("Input: %v", input)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivity, in)
		expectedActivity++

		return []byte("Activity Result."), nil
	}

	poller := &taskPoller{
		engine:          s.engine,
		taskList:        taskList,
		identity:        identity,
		decisionHandler: dtHandler,
		activityHandler: atHandler,
		logger:          s.logger,
	}

	for i := 0; i < 10; i++ {
		err := poller.pollAndProcessDecisionTask(false)
		s.logger.Infof("pollAndProcessDecisionTask: %v", err)
		s.Nil(err)
		err = poller.pollAndProcessActivityTask()
		s.logger.Infof("pollAndProcessActivityTask: %v", err)
		s.Nil(err)
	}

	s.False(workflowComplete)
	s.Nil(poller.pollAndProcessDecisionTask(true))
	s.True(workflowComplete)
}

func (p *taskPoller) pollAndProcessDecisionTask(dumpHistory bool) error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.engine.PollForDecisionTask(&workflow.PollForDecisionTaskRequest{
			TaskList: p.taskList,
			Identity: common.StringPtr(p.identity),
		})

		if err1 == wf.ErrDuplicate {
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || response == wf.EmptyPollForDecisionTaskResponse {
			continue retry
		}

		if dumpHistory {
			wf.PrintHistory(response.GetHistory(), p.logger)
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

	return wf.ErrNoTasks
}

func (p *taskPoller) pollAndProcessActivityTask() error {
retry:
	for attempt := 0; attempt < 5; attempt++ {
		response, err1 := p.engine.PollForActivityTask(&workflow.PollForActivityTaskRequest{
			TaskList: p.taskList,
			Identity: common.StringPtr(p.identity),
		})

		if err1 == wf.ErrDuplicate {
			continue retry
		}

		if err1 != nil {
			return err1
		}

		if response == nil || response == wf.EmptyPollForActivityTaskResponse {
			continue retry
		}

		result, err2 := p.activityHandler(response.GetWorkflowExecution(), response.GetActivityType(), response.GetActivityId(),
			response.GetStartedEventId(), response.GetInput())
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

	return wf.ErrNoTasks
}
