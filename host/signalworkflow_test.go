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
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/tag"
)

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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(1))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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

func (s *integrationSuite) TestSignalWorkflow_DuplicateRequest() {
	id := "integration-signal-workflow-test-duplicate"
	wt := "integration-signal-workflow-test-duplicate-type"
	tl := "integration-signal-workflow-test-duplicate-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(1))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(0, numOfSignaledEvent)
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

	foreignRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.foreignDomainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err0 := s.engine.StartWorkflowExecution(createContext(), foreignRequest)
	s.Nil(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Domain", tag.WorkflowDomainName(s.foreignDomainName), tag.WorkflowRunID(*we2.RunId))

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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(foreignActivityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("foreign PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
		//common.PrettyPrintHistory(history, s.Logger)

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if *signalRequestedEvent.EventType != workflow.EventTypeExternalWorkflowExecutionSignaled {
			s.Logger.Info("Signal still not sent.")
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal("history-service", *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
}

func (s *integrationSuite) TestSignalWorkflow_Cron_NoDecisionTaskCreated() {
	id := "integration-signal-workflow-test-cron"
	wt := "integration-signal-workflow-test-cron-type"
	tl := "integration-signal-workflow-test-cron-tasklist"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start workflow execution
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
		CronSchedule:                        &cronSpec,
	}
	now := time.Now()

	we, err0 := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	err := s.engine.SignalWorkflowExecution(createContext(), &workflow.SignalWorkflowExecutionRequest{
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

	// decider logic
	var decisionTaskDelay time.Duration
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {
		decisionTaskDelay = time.Now().Sub(now)

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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)
	s.True(decisionTaskDelay > time.Second*2)
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

	foreignRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.foreignDomainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we2, err0 := s.engine.StartWorkflowExecution(createContext(), foreignRequest)
	s.Nil(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Domain", tag.WorkflowDomainName(s.foreignDomainName), tag.WorkflowRunID(*we2.RunId))

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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(foreignActivityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("foreign PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if *signalRequestedEvent.EventType != workflow.EventTypeExternalWorkflowExecutionSignaled {
			s.Logger.Info("Signal still not sent.")
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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

		signalFailedEvent := history.Events[len(history.Events)-2]
		if *signalFailedEvent.EventType != workflow.EventTypeSignalExternalWorkflowExecutionFailed {
			s.Logger.Info("Cancellaton not cancelled yet.")
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
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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

		signalFailedEvent := history.Events[len(history.Events)-2]
		if *signalFailedEvent.EventType != workflow.EventTypeSignalExternalWorkflowExecutionFailed {
			s.Logger.Info("Cancellaton not cancelled yet.")
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

	header := &workflow.Header{
		Fields: map[string][]byte{"tracing": []byte("sample data")},
	}

	// Start a workflow
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

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent, startedEvent *workflow.HistoryEvent
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(1))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
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
			signalEvent = nil
			startedEvent = nil
			for _, event := range history.Events[previousStartedEventID:] {
				if *event.EventType == workflow.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
				}
				if *event.EventType == workflow.EventTypeWorkflowExecutionStarted {
					startedEvent = event
				}
			}
			if signalEvent != nil && startedEvent != nil {
				return nil, []*workflow.Decision{}, nil
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	// Send a signal
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	wfIDReusePolicy := workflow.WorkflowIdReusePolicyAllowDuplicate
	sRequest := &workflow.SignalWithStartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		Header:                              header,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		SignalName:                          common.StringPtr(signalName),
		SignalInput:                         signalInput,
		Identity:                            common.StringPtr(identity),
		WorkflowIdReusePolicy:               &wfIDReusePolicy,
	}
	resp, err := s.engine.SignalWithStartWorkflowExecution(createContext(), sRequest)
	s.Nil(err)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, *signalEvent.WorkflowExecutionSignaledEventAttributes.SignalName)
	s.Equal(signalInput, signalEvent.WorkflowExecutionSignaledEventAttributes.Input)
	s.Equal(identity, *signalEvent.WorkflowExecutionSignaledEventAttributes.Identity)
	s.True(startedEvent != nil)
	s.Equal(header, startedEvent.WorkflowExecutionStartedEventAttributes.Header)

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
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
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

func (s *integrationSuite) TestSignalWithStartWorkflow_IDReusePolicy() {
	id := "integration-signal-with-start-workflow-id-reuse-test"
	wt := "integration-signal-with-start-workflow-id-reuse-test-type"
	tl := "integration-signal-with-start-workflow-id-reuse-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &workflow.WorkflowType{}
	workflowType.Name = common.StringPtr(wt)

	taskList := &workflow.TaskList{}
	taskList.Name = common.StringPtr(tl)

	// Start a workflow
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
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)
	s.True(workflowComplete)

	// test policy WorkflowIdReusePolicyRejectDuplicate
	signalName := "my signal"
	signalInput := []byte("my signal input.")
	wfIDReusePolicy := workflow.WorkflowIdReusePolicyRejectDuplicate
	sRequest := &workflow.SignalWithStartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(s.domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		SignalName:                          common.StringPtr(signalName),
		SignalInput:                         signalInput,
		Identity:                            common.StringPtr(identity),
		WorkflowIdReusePolicy:               &wfIDReusePolicy,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	errMsg := err.(*workflow.WorkflowExecutionAlreadyStartedError).GetMessage()
	s.True(strings.Contains(errMsg, "reject duplicate workflow ID"))

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly
	wfIDReusePolicy = workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	errMsg = err.(*workflow.WorkflowExecutionAlreadyStartedError).GetMessage()
	s.True(strings.Contains(errMsg, "allow duplicate workflow ID if last run failed"))

	// test policy WorkflowIdReusePolicyAllowDuplicate
	wfIDReusePolicy = workflow.WorkflowIdReusePolicyAllowDuplicate
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(err)
	s.NotEmpty(resp.GetRunId())

	// Terminate workflow execution
	err = s.engine.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr("test WorkflowIdReusePolicyAllowDuplicateFailedOnly"),
		Details:  nil,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly success start
	wfIDReusePolicy = workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly
	resp, err = s.engine.SignalWithStartWorkflowExecution(createContext(), sRequest)
	s.Nil(err)
	s.NotEmpty(resp.GetRunId())
}
