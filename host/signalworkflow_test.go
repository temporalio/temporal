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
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/rpc"
)

func (s *integrationSuite) TestSignalWorkflow() {
	id := "integration-signal-workflow-test"
	wt := "integration-signal-workflow-test-type"
	tl := "integration-signal-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	// Send a signal to non-exist workflow
	_, err0 := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      uuid.New(),
		},
		SignalName: "failed signal",
		Input:      nil,
		Identity:   identity,
	})
	s.NotNil(err0)
	s.IsType(&serviceerror.NotFound{}, err0)

	// Start workflow execution
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

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*commonproto.Decision{}, nil
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

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

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

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Send first signal using RunID
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

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = []byte("another signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: "failed signal 1",
		Input:      nil,
		Identity:   identity,
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *integrationSuite) TestSignalWorkflow_DuplicateRequest() {
	id := "integration-signal-workflow-test-duplicate"
	wt := "integration-signal-workflow-test-duplicate-type"
	tl := "integration-signal-workflow-test-duplicate-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	// Start workflow execution
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

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *commonproto.HistoryEvent
	numOfSignaledEvent := 0
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			numOfSignaledEvent = 0
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
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

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

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

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Send first signal
	signalName := "my signal"
	signalInput := []byte("my signal input")
	requestID := uuid.New()
	signalReqest := &workflowservice.SignalWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		RequestId:  requestID,
	}
	_, err = s.engine.SignalWorkflowExecution(NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.Equal(1, numOfSignaledEvent)

	// Send another signal with same request id
	_, err = s.engine.SignalWorkflowExecution(NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

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

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.foreignDomainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Domain", tag.WorkflowDomainName(s.foreignDomainName), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input")
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
			DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
			Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: s.foreignDomainName,
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: id,
					RunId:      we2.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
	var signalEvent *commonproto.HistoryEvent
	foreignDtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*commonproto.Decision{}, nil
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
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("foreign PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History
		//common.PrettyPrintHistory(history, s.Logger)

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if signalRequestedEvent.GetEventType() != enums.EventTypeExternalWorkflowExecutionSignaled {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}

		ewfeAttributes := signalRequestedEvent.GetExternalWorkflowExecutionSignaledEventAttributes()
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
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *integrationSuite) TestSignalWorkflow_Cron_NoDecisionTaskCreated() {
	id := "integration-signal-workflow-test-cron"
	wt := "integration-signal-workflow-test-cron-type"
	tl := "integration-signal-workflow-test-cron-tasklist"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	// Start workflow execution
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
		CronSchedule:                        cronSpec,
	}
	now := time.Now()

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := []byte("my signal input")
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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

	// decider logic
	var decisionTaskDelay time.Duration
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		decisionTaskDelay = time.Now().Sub(now)

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
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(decisionTaskDelay > time.Second*2)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_WithoutRunID() {
	id := "integration-signal-external-workflow-test-without-run-id"
	wt := "integration-signal-external-workflow-test-without-run-id-type"
	tl := "integration-signal-external-workflow-test-without-run-id-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

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

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.foreignDomainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Domain", tag.WorkflowDomainName(s.foreignDomainName), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input")
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
			DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
			Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: s.foreignDomainName,
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: id,
					// No RunID in decision
				},
				SignalName: signalName,
				Input:      signalInput,
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
	var signalEvent *commonproto.HistoryEvent
	foreignDtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []byte(strconv.Itoa(int(foreignActivityCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*commonproto.Decision{}, nil
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
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("foreign PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if signalRequestedEvent.GetEventType() != enums.EventTypeExternalWorkflowExecutionSignaled {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForSignalSent
		}

		ewfeAttributes := signalRequestedEvent.GetExternalWorkflowExecutionSignaledEventAttributes()
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
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_UnKnownTarget() {
	id := "integration-signal-unknown-workflow-decision-test"
	wt := "integration-signal-unknown-workflow-decision-test-type"
	tl := "integration-signal-unknown-workflow-decision-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

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

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input")
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
			DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
			Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: s.foreignDomainName,
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: "workflow_not_exist",
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	signalSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		signalFailedEvent := history.Events[len(history.Events)-2]
		if signalFailedEvent.GetEventType() != enums.EventTypeSignalExternalWorkflowExecutionFailed {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		signalExternalWorkflowExecutionFailedEventAttributes := signalFailedEvent.GetSignalExternalWorkflowExecutionFailedEventAttributes()
		s.Equal(int64(intiatedEventID), signalExternalWorkflowExecutionFailedEventAttributes.InitiatedEventId)
		s.Equal("workflow_not_exist", signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.WorkflowId)
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

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := []byte("my signal input")
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
			DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
			Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
				Domain: s.domainName,
				Execution: &commonproto.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	signalSentFailed := false
	intiatedEventID := 10
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		signalFailedEvent := history.Events[len(history.Events)-2]
		if signalFailedEvent.GetEventType() != enums.EventTypeSignalExternalWorkflowExecutionFailed {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		signalExternalWorkflowExecutionFailedEventAttributes := signalFailedEvent.GetSignalExternalWorkflowExecutionFailedEventAttributes()
		s.Equal(int64(intiatedEventID), signalExternalWorkflowExecutionFailedEventAttributes.InitiatedEventId)
		s.Equal(id, signalExternalWorkflowExecutionFailedEventAttributes.WorkflowExecution.WorkflowId)
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

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	header := &commonproto.Header{
		Fields: map[string][]byte{"tracing": []byte("sample data")},
	}

	// Start a workflow
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

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent, startedEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      &commonproto.TaskList{Name: tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
					return nil, []*commonproto.Decision{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			signalEvent = nil
			startedEvent = nil
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enums.EventTypeWorkflowExecutionSignaled {
					signalEvent = event
				}
				if event.GetEventType() == enums.EventTypeWorkflowExecutionStarted {
					startedEvent = event
				}
			}
			if signalEvent != nil && startedEvent != nil {
				return nil, []*commonproto.Decision{}, nil
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

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

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

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	// Send a signal
	signalName := "my signal"
	signalInput := []byte("my signal input")
	wfIDReusePolicy := enums.WorkflowIdReusePolicyAllowDuplicate
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		Header:                              header,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		SignalName:                          signalName,
		SignalInput:                         signalInput,
		Identity:                            identity,
		WorkflowIdReusePolicy:               wfIDReusePolicy,
	}
	resp, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = []byte("signal to terminate input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id

	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.True(startedEvent != nil)
	s.Equal(header, startedEvent.GetWorkflowExecutionStartedEventAttributes().Header)

	// Send signal to not existed workflow
	id = "integration-signal-with-start-workflow-test-non-exist"
	signalName = "signal to non exist"
	signalInput = []byte("signal to non exist input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Assert visibility is correct
	listOpenRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Domain:          s.domainName,
		MaximumPageSize: 100,
		StartTimeFilter: &commonproto.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	}
	listResp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), listOpenRequest)
	s.NoError(err)
	s.Equal(1, len(listResp.Executions))

	// Terminate workflow execution and assert visibility is correct
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "kill workflow",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	for i := 0; i < 10; i++ { // retry
		listResp, err = s.engine.ListOpenWorkflowExecutions(NewContext(), listOpenRequest)
		s.NoError(err)
		if len(listResp.Executions) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.Equal(0, len(listResp.Executions))

	listClosedRequest := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Domain:          s.domainName,
		MaximumPageSize: 100,
		StartTimeFilter: &commonproto.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	}
	listClosedResp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), listClosedRequest)
	s.NoError(err)
	s.Equal(1, len(listClosedResp.Executions))
}

func (s *integrationSuite) TestSignalWithStartWorkflow_IDReusePolicy() {
	id := "integration-signal-with-start-workflow-id-reuse-test"
	wt := "integration-signal-with-start-workflow-id-reuse-test-type"
	tl := "integration-signal-with-start-workflow-id-reuse-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	// Start a workflow
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

	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {
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

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// test policy WorkflowIdReusePolicyRejectDuplicate
	signalName := "my signal"
	signalInput := []byte("my signal input")
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		SignalName:                          signalName,
		SignalInput:                         signalInput,
		Identity:                            identity,
		WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicyRejectDuplicate,
	}
	ctx, _ := rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err := s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "reject duplicate workflow ID"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enums.WorkflowIdReusePolicyAllowDuplicateFailedOnly
	ctx, _ = rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "allow duplicate workflow ID if last run failed"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test policy WorkflowIdReusePolicyAllowDuplicate
	sRequest.WorkflowIdReusePolicy = enums.WorkflowIdReusePolicyAllowDuplicate
	ctx, _ = rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test WorkflowIdReusePolicyAllowDuplicateFailedOnly",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly success start
	sRequest.WorkflowIdReusePolicy = enums.WorkflowIdReusePolicyAllowDuplicateFailedOnly
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
}
