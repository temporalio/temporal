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
	"strings"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	decisionpb "go.temporal.io/api/decision/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
)

func (s *integrationSuite) TestSignalWorkflow() {
	id := "integration-signal-workflow-test"
	wt := "integration-signal-workflow-test-type"
	tl := "integration-signal-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Send a signal to non-exist workflow
	_, err0 := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*decisionpb.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in decider
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = payloads.EncodeString("another signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in decider
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
	tl := "integration-signal-workflow-test-duplicate-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	numOfSignaledEvent := 0
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			numOfSignaledEvent = 0
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
			return []*decisionpb.Decision{}, nil
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	requestID := uuid.New()
	signalReqest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
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
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(0, numOfSignaledEvent)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision() {
	id := "integration-signal-external-workflow-test"
	wt := "integration-signal-external-workflow-test-type"
	tl := "integration-signal-external-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
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
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &decisionpb.SignalExternalWorkflowExecutionDecisionAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we2.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	foreignDtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*decisionpb.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.foreignNamespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: foreignDtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
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
		//common.PrettyPrintHistory(history, s.Logger)

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
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
	_, err = foreignPoller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *integrationSuite) TestSignalWorkflow_Cron_NoWorkflowTaskCreated() {
	id := "integration-signal-workflow-test-cron"
	wt := "integration-signal-workflow-test-cron-type"
	tl := "integration-signal-workflow-test-cron-taskqueue"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
		CronSchedule:               cronSpec,
	}
	now := time.Now()

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err := s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// decider logic
	var workflowTaskDelay time.Duration
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		workflowTaskDelay = time.Now().Sub(now)

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowTaskDelay > time.Second*2)
}

func (s *integrationSuite) TestSignalExternalWorkflowDecision_WithoutRunID() {
	id := "integration-signal-external-workflow-test-without-run-id"
	wt := "integration-signal-external-workflow-test-without-run-id-type"
	tl := "integration-signal-external-workflow-test-without-run-id-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
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
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we2, err0 := s.engine.StartWorkflowExecution(NewContext(), foreignRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on foreign Namespace", tag.WorkflowNamespace(s.foreignNamespace), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &decisionpb.SignalExternalWorkflowExecutionDecisionAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					// No RunID in decision
				},
				SignalName: signalName,
				Input:      signalInput,
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
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	workflowComplete := false
	foreignActivityCount := int32(1)
	foreignActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	foreignDtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if foreignActivityCounter < foreignActivityCount {
			foreignActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, foreignActivityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(foreignActivityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*decisionpb.Decision{}, nil
				}
			}
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	foreignPoller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.foreignNamespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: foreignDtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start both current and foreign workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = foreignPoller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("foreign PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = foreignPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("foreign PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	signalSent := false
	intiatedEventID := 10
CheckHistoryLoopForSignalSent:
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

		signalRequestedEvent := history.Events[len(history.Events)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
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
	_, err = foreignPoller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
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
	tl := "integration-signal-unknown-workflow-decision-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
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
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &decisionpb.SignalExternalWorkflowExecutionDecisionAttributes{
				Namespace: s.foreignNamespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow_not_exist",
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	signalSentFailed := false
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

		signalFailedEvent := history.Events[len(history.Events)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
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
	tl := "integration-signal-self-workflow-decision-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
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
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &decisionpb.SignalExternalWorkflowExecutionDecisionAttributes{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
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
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the foreign workflow with this decision request.
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	signalSentFailed := false
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

		signalFailedEvent := history.Events[len(history.Events)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
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
	tl := "integration-signal-with-start-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	// Start a workflow
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent, startedEvent *historypb.HistoryEvent
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 2,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*decisionpb.Decision{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			signalEvent = nil
			startedEvent = nil
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
				}
			}
			if signalEvent != nil && startedEvent != nil {
				return []*decisionpb.Decision{}, nil
			}
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send a signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wfIDReusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		Header:                     header,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		SignalName:                 signalName,
		SignalInput:                signalInput,
		Identity:                   identity,
		WorkflowIdReusePolicy:      wfIDReusePolicy,
	}
	resp, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in decider
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = payloads.EncodeString("signal to terminate input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id

	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
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
	signalInput = payloads.EncodeString("signal to non exist input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in decider
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Assert visibility is correct
	listOpenRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	}
	listResp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), listOpenRequest)
	s.NoError(err)
	s.Equal(1, len(listResp.Executions))

	// Terminate workflow execution and assert visibility is correct
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
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
	tl := "integration-signal-with-start-workflow-id-reuse-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start a workflow
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
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
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// test policy WorkflowIdReusePolicyRejectDuplicate
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		SignalName:                 signalName,
		SignalInput:                signalInput,
		Identity:                   identity,
		WorkflowIdReusePolicy:      enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
	ctx, _ := rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err := s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "reject duplicate workflow Id"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	ctx, _ = rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "allow duplicate workflow Id if last run failed"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test policy WorkflowIdReusePolicyAllowDuplicate
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	ctx, _ = rpc.NewContextWithTimeoutAndHeaders(5 * time.Second)
	resp, err = s.engine.SignalWithStartWorkflowExecution(ctx, sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())

	// Terminate workflow execution
	_, err = s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test WorkflowIdReusePolicyAllowDuplicateFailedOnly",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// test policy WorkflowIdReusePolicyAllowDuplicateFailedOnly success start
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	resp, err = s.engine.SignalWithStartWorkflowExecution(NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
}
