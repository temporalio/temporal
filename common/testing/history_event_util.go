// Copyright (c) 2019 Uber Technologies, Inc.
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

package testing

import (
	"time"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
)

const (
	timeout              = int32(10000)
	signal               = "NDC signal"
	checksum             = "NDC checksum"
	childWorkflowPrefix  = "child-"
	reason               = "NDC reason"
	workflowType         = "test-workflow-type"
	taskList             = "taskList"
	identity             = "identity"
	decisionTaskAttempts = 0
	childWorkflowID      = "child-workflowID"
	externalWorkflowID   = "external-workflowID"
)

var (
	globalTaskID int64 = 1
)

// InitializeHistoryEventGenerator initializes the history event generator
func InitializeHistoryEventGenerator(
	domain string,
	defaultVersion int64,
) Generator {

	generator := NewEventGenerator(time.Now().UnixNano())
	generator.SetVersion(defaultVersion)
	// Functions
	notPendingDecisionTask := func(input ...interface{}) bool {
		count := 0
		history := input[0].([]Vertex)
		for _, e := range history {
			switch e.GetName() {
			case enums.EventTypeDecisionTaskScheduled.String():
				count++
			case enums.EventTypeDecisionTaskCompleted.String(),
				enums.EventTypeDecisionTaskFailed.String(),
				enums.EventTypeDecisionTaskTimedOut.String():
				count--
			}
		}
		return count <= 0
	}
	containActivityComplete := func(input ...interface{}) bool {
		history := input[0].([]Vertex)
		for _, e := range history {
			if e.GetName() == enums.EventTypeActivityTaskCompleted.String() {
				return true
			}
		}
		return false
	}
	hasPendingActivity := func(input ...interface{}) bool {
		count := 0
		history := input[0].([]Vertex)
		for _, e := range history {
			switch e.GetName() {
			case enums.EventTypeActivityTaskScheduled.String():
				count++
			case enums.EventTypeActivityTaskCanceled.String(),
				enums.EventTypeActivityTaskFailed.String(),
				enums.EventTypeActivityTaskTimedOut.String(),
				enums.EventTypeActivityTaskCompleted.String():
				count--
			}
		}
		return count > 0
	}
	canDoBatch := func(currentBatch []Vertex, history []Vertex) bool {
		if len(currentBatch) == 0 {
			return true
		}

		hasPendingDecisionTask := false
		for _, event := range history {
			switch event.GetName() {
			case enums.EventTypeDecisionTaskScheduled.String():
				hasPendingDecisionTask = true
			case enums.EventTypeDecisionTaskCompleted.String(),
				enums.EventTypeDecisionTaskFailed.String(),
				enums.EventTypeDecisionTaskTimedOut.String():
				hasPendingDecisionTask = false
			}
		}
		if hasPendingDecisionTask {
			return false
		}
		if currentBatch[len(currentBatch)-1].GetName() == enums.EventTypeDecisionTaskScheduled.String() {
			return false
		}
		if currentBatch[0].GetName() == enums.EventTypeDecisionTaskCompleted.String() {
			return len(currentBatch) == 1
		}
		return true
	}

	// Setup decision task model
	decisionModel := NewHistoryEventModel()
	decisionSchedule := NewHistoryEventVertex(enums.EventTypeDecisionTaskScheduled.String())
	decisionSchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeDecisionTaskScheduled
		historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
			TaskList: &commonproto.TaskList{
				Name: taskList,
				Kind: enums.TaskListKindNormal,
			},
			StartToCloseTimeoutSeconds: timeout,
			Attempt:                    decisionTaskAttempts,
		}}
		return historyEvent
	})
	decisionStart := NewHistoryEventVertex(enums.EventTypeDecisionTaskStarted.String())
	decisionStart.SetIsStrictOnNextVertex(true)
	decisionStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeDecisionTaskStarted
		historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
		}}
		return historyEvent
	})
	decisionFail := NewHistoryEventVertex(enums.EventTypeDecisionTaskFailed.String())
	decisionFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeDecisionTaskFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &commonproto.DecisionTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Cause:            enums.DecisionTaskFailedCauseUnhandledDecision,
			Identity:         identity,
			ForkEventVersion: version,
		}}
		return historyEvent
	})
	decisionTimedOut := NewHistoryEventVertex(enums.EventTypeDecisionTaskTimedOut.String())
	decisionTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeDecisionTaskTimedOut
		historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: &commonproto.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      enums.TimeoutTypeScheduleToStart,
		}}
		return historyEvent
	})
	decisionComplete := NewHistoryEventVertex(enums.EventTypeDecisionTaskCompleted.String())
	decisionComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeDecisionTaskCompleted
		historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			BinaryChecksum:   checksum,
		}}
		return historyEvent
	})
	decisionComplete.SetIsStrictOnNextVertex(true)
	decisionComplete.SetMaxNextVertex(2)
	decisionScheduleToStart := NewHistoryEventEdge(decisionSchedule, decisionStart)
	decisionStartToComplete := NewHistoryEventEdge(decisionStart, decisionComplete)
	decisionStartToFail := NewHistoryEventEdge(decisionStart, decisionFail)
	decisionStartToTimedOut := NewHistoryEventEdge(decisionStart, decisionTimedOut)
	decisionFailToSchedule := NewHistoryEventEdge(decisionFail, decisionSchedule)
	decisionFailToSchedule.SetCondition(notPendingDecisionTask)
	decisionTimedOutToSchedule := NewHistoryEventEdge(decisionTimedOut, decisionSchedule)
	decisionTimedOutToSchedule.SetCondition(notPendingDecisionTask)
	decisionModel.AddEdge(decisionScheduleToStart, decisionStartToComplete, decisionStartToFail, decisionStartToTimedOut,
		decisionFailToSchedule, decisionTimedOutToSchedule)

	// Setup workflow model
	workflowModel := NewHistoryEventModel()

	workflowStart := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionStarted.String())
	workflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		historyEvent := getDefaultHistoryEvent(1, defaultVersion)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionStarted
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonproto.WorkflowType{
				Name: workflowType,
			},
			TaskList: &commonproto.TaskList{
				Name: taskList,
				Kind: enums.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			Identity:                            identity,
			FirstExecutionRunId:                 uuid.New(),
		}}
		return historyEvent
	})
	workflowSignal := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionSignaled.String())
	workflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionSignaled
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			SignalName: signal,
			Identity:   identity,
		}}
		return historyEvent
	})
	workflowComplete := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionCompleted.String())
	workflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionCompleted
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &commonproto.WorkflowExecutionCompletedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	continueAsNew := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionContinuedAsNew.String())
	continueAsNew.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionContinuedAsNew
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &commonproto.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: uuid.New(),
			WorkflowType: &commonproto.WorkflowType{
				Name: workflowType,
			},
			TaskList: &commonproto.TaskList{
				Name: taskList,
				Kind: enums.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			DecisionTaskCompletedEventId:        eventID - 1,
			Initiator:                           enums.ContinueAsNewInitiatorDecider,
		}}
		return historyEvent
	})
	workflowFail := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionFailed.String())
	workflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &commonproto.WorkflowExecutionFailedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancel := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionCanceled.String())
	workflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionCanceled
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &commonproto.WorkflowExecutionCanceledEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancelRequest := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionCancelRequested.String())
	workflowCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionCancelRequested
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &commonproto.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                    "",
			ExternalInitiatedEventId: 1,
			ExternalWorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			Identity: identity,
		}}
		return historyEvent
	})
	workflowTerminate := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionTerminated.String())
	workflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionTerminated
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &commonproto.WorkflowExecutionTerminatedEventAttributes{
			Identity: identity,
			Reason:   reason,
		}}
		return historyEvent
	})
	workflowTimedOut := NewHistoryEventVertex(enums.EventTypeWorkflowExecutionTimedOut.String())
	workflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeWorkflowExecutionTimedOut
		historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &commonproto.WorkflowExecutionTimedOutEventAttributes{
			TimeoutType: enums.TimeoutTypeStartToClose,
		}}
		return historyEvent
	})
	workflowStartToSignal := NewHistoryEventEdge(workflowStart, workflowSignal)
	workflowStartToDecisionSchedule := NewHistoryEventEdge(workflowStart, decisionSchedule)
	workflowStartToDecisionSchedule.SetCondition(notPendingDecisionTask)
	workflowSignalToDecisionSchedule := NewHistoryEventEdge(workflowSignal, decisionSchedule)
	workflowSignalToDecisionSchedule.SetCondition(notPendingDecisionTask)
	decisionCompleteToWorkflowComplete := NewHistoryEventEdge(decisionComplete, workflowComplete)
	decisionCompleteToWorkflowComplete.SetCondition(containActivityComplete)
	decisionCompleteToWorkflowFailed := NewHistoryEventEdge(decisionComplete, workflowFail)
	decisionCompleteToWorkflowFailed.SetCondition(containActivityComplete)
	decisionCompleteToCAN := NewHistoryEventEdge(decisionComplete, continueAsNew)
	decisionCompleteToCAN.SetCondition(containActivityComplete)
	workflowCancelRequestToCancel := NewHistoryEventEdge(workflowCancelRequest, workflowCancel)
	workflowModel.AddEdge(workflowStartToSignal, workflowStartToDecisionSchedule, workflowSignalToDecisionSchedule,
		decisionCompleteToCAN, decisionCompleteToWorkflowComplete, decisionCompleteToWorkflowFailed, workflowCancelRequestToCancel)

	// Setup activity model
	activityModel := NewHistoryEventModel()
	activitySchedule := NewHistoryEventVertex(enums.EventTypeActivityTaskScheduled.String())
	activitySchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskScheduled
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
			ActivityId:   uuid.New(),
			ActivityType: &commonproto.ActivityType{Name: "activity"},
			Domain:       domain,
			TaskList: &commonproto.TaskList{
				Name: taskList,
				Kind: enums.TaskListKindNormal,
			},
			ScheduleToCloseTimeoutSeconds: timeout,
			ScheduleToStartTimeoutSeconds: timeout,
			StartToCloseTimeoutSeconds:    timeout,
			DecisionTaskCompletedEventId:  lastEvent.EventId,
		}}
		return historyEvent
	})
	activityStart := NewHistoryEventVertex(enums.EventTypeActivityTaskStarted.String())
	activityStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskStarted
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
			Attempt:          0,
		}}
		return historyEvent
	})
	activityComplete := NewHistoryEventVertex(enums.EventTypeActivityTaskCompleted.String())
	activityComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskCompleted
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
		}}
		return historyEvent
	})
	activityFail := NewHistoryEventVertex(enums.EventTypeActivityTaskFailed.String())
	activityFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &commonproto.ActivityTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			Reason:           reason,
		}}
		return historyEvent
	})
	activityTimedOut := NewHistoryEventVertex(enums.EventTypeActivityTaskTimedOut.String())
	activityTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskTimedOut
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &commonproto.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      enums.TimeoutTypeScheduleToClose,
		}}
		return historyEvent
	})
	activityCancelRequest := NewHistoryEventVertex(enums.EventTypeActivityTaskCancelRequested.String())
	activityCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskCancelRequested
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &commonproto.ActivityTaskCancelRequestedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskScheduledEventAttributes().DecisionTaskCompletedEventId,
			ActivityId:                   lastEvent.GetActivityTaskScheduledEventAttributes().ActivityId,
		}}
		return historyEvent
	})
	activityCancel := NewHistoryEventVertex(enums.EventTypeActivityTaskCanceled.String())
	activityCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeActivityTaskCanceled
		historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &commonproto.ActivityTaskCanceledEventAttributes{
			LatestCancelRequestedEventId: lastEvent.EventId,
			ScheduledEventId:             lastEvent.EventId,
			StartedEventId:               lastEvent.EventId,
			Identity:                     identity,
		}}
		return historyEvent
	})
	activityCancelRequestFail := NewHistoryEventVertex(enums.EventTypeRequestCancelActivityTaskFailed.String())
	activityCancelRequestFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		versionBump := input[2].(int64)
		subVersion := input[3].(int64)
		version := lastGeneratedEvent.GetVersion() + versionBump + subVersion
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeRequestCancelActivityTaskFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_RequestCancelActivityTaskFailedEventAttributes{RequestCancelActivityTaskFailedEventAttributes: &commonproto.RequestCancelActivityTaskFailedEventAttributes{
			ActivityId:                   uuid.New(),
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskCancelRequestedEventAttributes().DecisionTaskCompletedEventId,
		}}
		return historyEvent
	})
	decisionCompleteToATSchedule := NewHistoryEventEdge(decisionComplete, activitySchedule)

	activityScheduleToStart := NewHistoryEventEdge(activitySchedule, activityStart)
	activityScheduleToStart.SetCondition(hasPendingActivity)

	activityStartToComplete := NewHistoryEventEdge(activityStart, activityComplete)
	activityStartToComplete.SetCondition(hasPendingActivity)

	activityStartToFail := NewHistoryEventEdge(activityStart, activityFail)
	activityStartToFail.SetCondition(hasPendingActivity)

	activityStartToTimedOut := NewHistoryEventEdge(activityStart, activityTimedOut)
	activityStartToTimedOut.SetCondition(hasPendingActivity)

	activityCompleteToDecisionSchedule := NewHistoryEventEdge(activityComplete, decisionSchedule)
	activityCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityFailToDecisionSchedule := NewHistoryEventEdge(activityFail, decisionSchedule)
	activityFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityTimedOutToDecisionSchedule := NewHistoryEventEdge(activityTimedOut, decisionSchedule)
	activityTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityCancelToDecisionSchedule := NewHistoryEventEdge(activityCancel, decisionSchedule)
	activityCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)

	// TODO: bypass activity cancel request event. Support this event later.
	//activityScheduleToActivityCancelRequest := NewHistoryEventEdge(activitySchedule, activityCancelRequest)
	//activityScheduleToActivityCancelRequest.SetCondition(hasPendingActivity)
	activityCancelReqToCancel := NewHistoryEventEdge(activityCancelRequest, activityCancel)
	activityCancelReqToCancel.SetCondition(hasPendingActivity)

	activityCancelReqToCancelFail := NewHistoryEventEdge(activityCancelRequest, activityCancelRequestFail)
	activityCancelRequestFailToDecisionSchedule := NewHistoryEventEdge(activityCancelRequestFail, decisionSchedule)
	activityCancelRequestFailToDecisionSchedule.SetCondition(notPendingDecisionTask)

	activityModel.AddEdge(decisionCompleteToATSchedule, activityScheduleToStart, activityStartToComplete,
		activityStartToFail, activityStartToTimedOut, decisionCompleteToATSchedule, activityCompleteToDecisionSchedule,
		activityFailToDecisionSchedule, activityTimedOutToDecisionSchedule, activityCancelReqToCancel,
		activityCancelReqToCancelFail, activityCancelToDecisionSchedule, activityCancelRequestFailToDecisionSchedule)

	// Setup timer model
	timerModel := NewHistoryEventModel()
	timerStart := NewHistoryEventVertex(enums.EventTypeTimerStarted.String())
	timerStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeTimerStarted
		historyEvent.Attributes = &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
			TimerId:                      uuid.New(),
			StartToFireTimeoutSeconds:    10,
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerFired := NewHistoryEventVertex(enums.EventTypeTimerFired.String())
	timerFired.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeTimerFired
		historyEvent.Attributes = &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
			TimerId:        lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerCancel := NewHistoryEventVertex(enums.EventTypeTimerCanceled.String())
	timerCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeTimerCanceled
		historyEvent.Attributes = &commonproto.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &commonproto.TimerCanceledEventAttributes{
			TimerId:                      lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId:               lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetTimerStartedEventAttributes().DecisionTaskCompletedEventId,
			Identity:                     identity,
		}}
		return historyEvent
	})
	timerStartToFire := NewHistoryEventEdge(timerStart, timerFired)
	timerStartToCancel := NewHistoryEventEdge(timerStart, timerCancel)

	decisionCompleteToTimerStart := NewHistoryEventEdge(decisionComplete, timerStart)
	timerFiredToDecisionSchedule := NewHistoryEventEdge(timerFired, decisionSchedule)
	timerFiredToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerCancelToDecisionSchedule := NewHistoryEventEdge(timerCancel, decisionSchedule)
	timerCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerModel.AddEdge(timerStartToFire, timerStartToCancel, decisionCompleteToTimerStart, timerFiredToDecisionSchedule, timerCancelToDecisionSchedule)

	// Setup child workflow model
	childWorkflowModel := NewHistoryEventModel()
	childWorkflowInitial := NewHistoryEventVertex(enums.EventTypeStartChildWorkflowExecutionInitiated.String())
	childWorkflowInitial.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeStartChildWorkflowExecutionInitiated
		historyEvent.Attributes = &commonproto.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &commonproto.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:       domain,
			WorkflowId:   childWorkflowID,
			WorkflowType: &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			TaskList: &commonproto.TaskList{
				Name: taskList,
				Kind: enums.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			DecisionTaskCompletedEventId:        lastEvent.EventId,
			WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicyRejectDuplicate,
		}}
		return historyEvent
	})
	childWorkflowInitialFail := NewHistoryEventVertex(enums.EventTypeStartChildWorkflowExecutionFailed.String())
	childWorkflowInitialFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeStartChildWorkflowExecutionFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &commonproto.StartChildWorkflowExecutionFailedEventAttributes{
			Domain:                       domain,
			WorkflowId:                   childWorkflowID,
			WorkflowType:                 &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			Cause:                        enums.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning,
			InitiatedEventId:             lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
		}}
		return historyEvent
	})
	childWorkflowStart := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionStarted.String())
	childWorkflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionStarted
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &commonproto.ChildWorkflowExecutionStartedEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.EventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      uuid.New(),
			},
		}}
		return historyEvent
	})
	childWorkflowCancel := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionCanceled.String())
	childWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionCanceled
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &commonproto.ChildWorkflowExecutionCanceledEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowComplete := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionCompleted.String())
	childWorkflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionCompleted
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &commonproto.ChildWorkflowExecutionCompletedEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowFail := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionFailed.String())
	childWorkflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &commonproto.ChildWorkflowExecutionFailedEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTerminate := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionTerminated.String())
	childWorkflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionTerminated
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &commonproto.ChildWorkflowExecutionTerminatedEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTimedOut := NewHistoryEventVertex(enums.EventTypeChildWorkflowExecutionTimedOut.String())
	childWorkflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeChildWorkflowExecutionTimedOut
		historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &commonproto.ChildWorkflowExecutionTimedOutEventAttributes{
			Domain:           domain,
			WorkflowType:     &commonproto.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
			TimeoutType:    enums.TimeoutTypeScheduleToClose,
		}}
		return historyEvent
	})
	decisionCompleteToChildWorkflowInitial := NewHistoryEventEdge(decisionComplete, childWorkflowInitial)
	childWorkflowInitialToFail := NewHistoryEventEdge(childWorkflowInitial, childWorkflowInitialFail)
	childWorkflowInitialToStart := NewHistoryEventEdge(childWorkflowInitial, childWorkflowStart)
	childWorkflowStartToCancel := NewHistoryEventEdge(childWorkflowStart, childWorkflowCancel)
	childWorkflowStartToFail := NewHistoryEventEdge(childWorkflowStart, childWorkflowFail)
	childWorkflowStartToComplete := NewHistoryEventEdge(childWorkflowStart, childWorkflowComplete)
	childWorkflowStartToTerminate := NewHistoryEventEdge(childWorkflowStart, childWorkflowTerminate)
	childWorkflowStartToTimedOut := NewHistoryEventEdge(childWorkflowStart, childWorkflowTimedOut)
	childWorkflowCancelToDecisionSchedule := NewHistoryEventEdge(childWorkflowCancel, decisionSchedule)
	childWorkflowCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowFail, decisionSchedule)
	childWorkflowFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowCompleteToDecisionSchedule := NewHistoryEventEdge(childWorkflowComplete, decisionSchedule)
	childWorkflowCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTerminateToDecisionSchedule := NewHistoryEventEdge(childWorkflowTerminate, decisionSchedule)
	childWorkflowTerminateToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTimedOutToDecisionSchedule := NewHistoryEventEdge(childWorkflowTimedOut, decisionSchedule)
	childWorkflowTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowInitialFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowInitialFail, decisionSchedule)
	childWorkflowInitialFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowModel.AddEdge(decisionCompleteToChildWorkflowInitial, childWorkflowInitialToFail, childWorkflowInitialToStart,
		childWorkflowStartToCancel, childWorkflowStartToFail, childWorkflowStartToComplete, childWorkflowStartToTerminate,
		childWorkflowStartToTimedOut, childWorkflowCancelToDecisionSchedule, childWorkflowFailToDecisionSchedule,
		childWorkflowCompleteToDecisionSchedule, childWorkflowTerminateToDecisionSchedule, childWorkflowTimedOutToDecisionSchedule,
		childWorkflowInitialFailToDecisionSchedule)

	// Setup external workflow model
	externalWorkflowModel := NewHistoryEventModel()
	externalWorkflowSignal := NewHistoryEventVertex(enums.EventTypeSignalExternalWorkflowExecutionInitiated.String())
	externalWorkflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeSignalExternalWorkflowExecutionInitiated
		historyEvent.Attributes = &commonproto.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &commonproto.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
			Domain:                       domain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			SignalName:        "signal",
			ChildWorkflowOnly: false,
		}}
		return historyEvent
	})
	externalWorkflowSignalFailed := NewHistoryEventVertex(enums.EventTypeSignalExternalWorkflowExecutionFailed.String())
	externalWorkflowSignalFailed.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeSignalExternalWorkflowExecutionFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &commonproto.SignalExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        enums.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
			DecisionTaskCompletedEventId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Domain:                       domain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowSignaled := NewHistoryEventVertex(enums.EventTypeExternalWorkflowExecutionSignaled.String())
	externalWorkflowSignaled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeExternalWorkflowExecutionSignaled
		historyEvent.Attributes = &commonproto.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &commonproto.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Domain:           domain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
		return historyEvent
	})
	externalWorkflowCancel := NewHistoryEventVertex(enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String())
	externalWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated
		historyEvent.Attributes = &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &commonproto.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				DecisionTaskCompletedEventId: lastEvent.EventId,
				Domain:                       domain,
				WorkflowExecution: &commonproto.WorkflowExecution{
					WorkflowId: externalWorkflowID,
					RunId:      uuid.New(),
				},
				ChildWorkflowOnly: false,
			}}
		return historyEvent
	})
	externalWorkflowCancelFail := NewHistoryEventVertex(enums.EventTypeRequestCancelExternalWorkflowExecutionFailed.String())
	externalWorkflowCancelFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeRequestCancelExternalWorkflowExecutionFailed
		historyEvent.Attributes = &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &commonproto.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        enums.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
			DecisionTaskCompletedEventId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Domain:                       domain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowCanceled := NewHistoryEventVertex(enums.EventTypeExternalWorkflowExecutionCancelRequested.String())
	externalWorkflowCanceled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*commonproto.HistoryEvent)
		lastGeneratedEvent := input[1].(*commonproto.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enums.EventTypeExternalWorkflowExecutionCancelRequested
		historyEvent.Attributes = &commonproto.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &commonproto.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Domain:           domain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
		return historyEvent
	})
	decisionCompleteToExternalWorkflowSignal := NewHistoryEventEdge(decisionComplete, externalWorkflowSignal)
	decisionCompleteToExternalWorkflowCancel := NewHistoryEventEdge(decisionComplete, externalWorkflowCancel)
	externalWorkflowSignalToFail := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignalFailed)
	externalWorkflowSignalToSignaled := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignaled)
	externalWorkflowCancelToFail := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCancelFail)
	externalWorkflowCancelToCanceled := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCanceled)
	externalWorkflowSignaledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignaled, decisionSchedule)
	externalWorkflowSignaledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowSignalFailedToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignalFailed, decisionSchedule)
	externalWorkflowSignalFailedToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCanceledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCanceled, decisionSchedule)
	externalWorkflowCanceledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCancelFailToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCancelFail, decisionSchedule)
	externalWorkflowCancelFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowModel.AddEdge(decisionCompleteToExternalWorkflowSignal, decisionCompleteToExternalWorkflowCancel,
		externalWorkflowSignalToFail, externalWorkflowSignalToSignaled, externalWorkflowCancelToFail, externalWorkflowCancelToCanceled,
		externalWorkflowSignaledToDecisionSchedule, externalWorkflowSignalFailedToDecisionSchedule,
		externalWorkflowCanceledToDecisionSchedule, externalWorkflowCancelFailToDecisionSchedule)

	// Config event generator
	generator.SetBatchGenerationRule(canDoBatch)
	generator.AddInitialEntryVertex(workflowStart)
	generator.AddExitVertex(workflowComplete, workflowFail, workflowTerminate, workflowTimedOut, continueAsNew)
	// generator.AddRandomEntryVertex(workflowSignal, workflowTerminate, workflowTimedOut)
	generator.AddModel(decisionModel)
	generator.AddModel(workflowModel)
	generator.AddModel(activityModel)
	generator.AddModel(timerModel)
	generator.AddModel(childWorkflowModel)
	generator.AddModel(externalWorkflowModel)
	return generator
}

func getDefaultHistoryEvent(
	eventID int64,
	version int64,
) *commonproto.HistoryEvent {

	globalTaskID++
	return &commonproto.HistoryEvent{
		EventId:   eventID,
		Timestamp: time.Now().UnixNano(),
		TaskId:    globalTaskID,
		Version:   version,
	}
}

func copyConnections(
	originalMap map[string][]Edge,
) map[string][]Edge {

	newMap := make(map[string][]Edge)
	for key, value := range originalMap {
		newMap[key] = copyEdges(value)
	}
	return newMap
}

func copyExitVertices(
	originalMap map[string]bool,
) map[string]bool {

	newMap := make(map[string]bool)
	for key, value := range originalMap {
		newMap[key] = value
	}
	return newMap
}

func copyVertex(vertex []Vertex) []Vertex {
	newVertex := make([]Vertex, len(vertex))
	for idx, v := range vertex {
		newVertex[idx] = v.DeepCopy()
	}
	return newVertex
}

func copyEdges(edges []Edge) []Edge {
	newEdges := make([]Edge, len(edges))
	for idx, e := range edges {
		newEdges[idx] = e.DeepCopy()
	}
	return newEdges
}
