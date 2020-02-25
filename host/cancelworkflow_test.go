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
	"time"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/log/tag"
)

func (s *integrationSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "integration-request-cancel-workflow-test"
	wt := "integration-request-cancel-workflow-test-type"
	tl := "integration-request-cancel-workflow-test-tasklist"
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
			DecisionType: enums.DecisionTypeCancelWorkflowExecution,
			Attributes: &commonproto.Decision_CancelWorkflowExecutionDecisionAttributes{CancelWorkflowExecutionDecisionAttributes: &commonproto.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
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

	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	_, err = s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	_, err = s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Domain: s.domainName,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.CancellationAlreadyRequested{}, err)

	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 3; i++ {
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
		if lastEvent.EventType != enums.EventTypeWorkflowExecutionCanceled {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
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
	s.Logger.Info("StartWorkflowExecution on foreign domain", tag.WorkflowDomainName(s.foreignDomainName), tag.WorkflowRunID(we2.RunId))

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
			DecisionType: enums.DecisionTypeRequestCancelExternalWorkflowExecution,
			Attributes: &commonproto.Decision_RequestCancelExternalWorkflowExecutionDecisionAttributes{RequestCancelExternalWorkflowExecutionDecisionAttributes: &commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     s.foreignDomainName,
				WorkflowId: id,
				RunId:      we2.RunId,
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

	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
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
		}

		return []byte(strconv.Itoa(int(foreignActivityCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCancelWorkflowExecution,
			Attributes: &commonproto.Decision_CancelWorkflowExecutionDecisionAttributes{CancelWorkflowExecutionDecisionAttributes: &commonproto.CancelWorkflowExecutionDecisionAttributes{
				Details: []byte("Cancelled"),
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

	// Cancel the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	cancellationSent := false
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

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != enums.EventTypeExternalWorkflowExecutionCancelRequested {
			s.Logger.Info("Cancellation still not sent")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		externalWorkflowExecutionCancelRequestedEvent := lastEvent.GetExternalWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), externalWorkflowExecutionCancelRequestedEvent.InitiatedEventId)
		s.Equal(id, externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.WorkflowId)
		s.Equal(we2.RunId, externalWorkflowExecutionCancelRequestedEvent.WorkflowExecution.RunId)

		cancellationSent = true
		break
	}

	s.True(cancellationSent)

	// Accept cancellation.
	_, err = foreignPoller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("foreign PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	executionCancelled := false
GetHistoryLoop:
	for i := 1; i < 10; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: s.foreignDomainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
				RunId:      we2.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != enums.EventTypeWorkflowExecutionCanceled {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		s.Equal("Cancelled", string(cancelledEventAttributes.Details))
		executionCancelled = true

		// Find cancel requested event and verify it.
		var cancelRequestEvent *commonproto.HistoryEvent
		for _, x := range history.Events {
			if x.EventType == enums.EventTypeWorkflowExecutionCancelRequested {
				cancelRequestEvent = x
			}
		}

		s.NotNil(cancelRequestEvent)
		cancelRequestEventAttributes := cancelRequestEvent.GetWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(intiatedEventID), cancelRequestEventAttributes.ExternalInitiatedEventId)
		s.Equal(id, cancelRequestEventAttributes.ExternalWorkflowExecution.WorkflowId)
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
			DecisionType: enums.DecisionTypeRequestCancelExternalWorkflowExecution,
			Attributes: &commonproto.Decision_RequestCancelExternalWorkflowExecutionDecisionAttributes{RequestCancelExternalWorkflowExecutionDecisionAttributes: &commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Domain:     s.foreignDomainName,
				WorkflowId: "workflow_not_exist",
				RunId:      we.RunId,
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

	// Cancel the foreign workflow with this decision request.
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	cancellationSentFailed := false
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

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != enums.EventTypeRequestCancelExternalWorkflowExecutionFailed {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue CheckHistoryLoopForCancelSent
		}

		requestCancelExternalWorkflowExecutionFailedEvetn := lastEvent.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()
		s.Equal(int64(intiatedEventID), requestCancelExternalWorkflowExecutionFailedEvetn.InitiatedEventId)
		s.Equal("workflow_not_exist", requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.WorkflowId)
		s.Equal(we.RunId, requestCancelExternalWorkflowExecutionFailedEvetn.WorkflowExecution.RunId)

		cancellationSentFailed = true
		break
	}

	s.True(cancellationSentFailed)
}
