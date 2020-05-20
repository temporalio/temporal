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
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/payloads"
)

func (s *integrationSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "integration-request-cancel-workflow-test"
	wt := "integration-request-cancel-workflow-test-type"
	tl := "integration-request-cancel-workflow-test-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ScheduleActivityTask,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskList:                      &tasklistpb.TaskList{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_CancelWorkflowExecution,
			Attributes: &decisionpb.Decision_CancelWorkflowExecutionDecisionAttributes{CancelWorkflowExecutionDecisionAttributes: &decisionpb.CancelWorkflowExecutionDecisionAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
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
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	_, err = s.engine.RequestCancelWorkflowExecution(NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != eventpb.EventType_WorkflowExecutionCanceled {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		var details string
		err = payloads.Decode(cancelledEventAttributes.GetDetails(), &details)
		s.NoError(err)
		s.Equal("Cancelled", details)
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

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	foreignRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.foreignNamespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ScheduleActivityTask,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskList:                      &tasklistpb.TaskList{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_RequestCancelExternalWorkflowExecution,
			Attributes: &decisionpb.Decision_RequestCancelExternalWorkflowExecutionDecisionAttributes{RequestCancelExternalWorkflowExecutionDecisionAttributes: &decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Namespace:  s.foreignNamespace,
				WorkflowId: id,
				RunId:      we2.RunId,
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	foreignDtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]*decisionpb.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ScheduleActivityTask,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskList:                      &tasklistpb.TaskList{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_CancelWorkflowExecution,
			Attributes: &decisionpb.Decision_CancelWorkflowExecutionDecisionAttributes{CancelWorkflowExecutionDecisionAttributes: &decisionpb.CancelWorkflowExecutionDecisionAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.foreignNamespace,
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
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != eventpb.EventType_ExternalWorkflowExecutionCancelRequested {
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
			Namespace: s.foreignNamespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we2.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != eventpb.EventType_WorkflowExecutionCanceled {
			s.Logger.Warn("Execution not cancelled yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		cancelledEventAttributes := lastEvent.GetWorkflowExecutionCanceledEventAttributes()
		var details string
		err = payloads.Decode(cancelledEventAttributes.GetDetails(), &details)
		s.NoError(err)
		s.Equal("Cancelled", details)
		executionCancelled = true

		// Find cancel requested event and verify it.
		var cancelRequestEvent *eventpb.HistoryEvent
		for _, x := range history.Events {
			if x.EventType == eventpb.EventType_WorkflowExecutionCancelRequested {
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

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ScheduleActivityTask,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskList:                      &tasklistpb.TaskList{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_RequestCancelExternalWorkflowExecution,
			Attributes: &decisionpb.Decision_RequestCancelExternalWorkflowExecutionDecisionAttributes{RequestCancelExternalWorkflowExecutionDecisionAttributes: &decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes{
				Namespace:  s.foreignNamespace,
				WorkflowId: "workflow_not_exist",
				RunId:      we.RunId,
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
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
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-2]
		if lastEvent.EventType != eventpb.EventType_RequestCancelExternalWorkflowExecutionFailed {
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
